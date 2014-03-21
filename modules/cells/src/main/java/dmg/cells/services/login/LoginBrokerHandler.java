package dmg.cells.services.login;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;

import static java.util.concurrent.TimeUnit.*;

/**
 * Utility class to periodically register a door in a login broker.
 */
public class LoginBrokerHandler
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(LoginBrokerHandler.class);

    enum UpdateMode
    {
        EAGER, NORMAL
    }

    private static final long EAGER_UPDATE_TIME = SECONDS.toMillis(1);

    private String[] _loginBrokers;
    private String _protocolFamily;
    private String _protocolVersion;
    private String _protocolEngine;
    private long   _brokerUpdateTime = MINUTES.toMillis(5);
    private TimeUnit _brokerUpdateTimeUnit = MILLISECONDS;
    private double _brokerUpdateThreshold = 0.1;
    private UpdateMode _currentUpdateMode = UpdateMode.NORMAL;
    private LoadProvider _load = new FixedLoad(0.0);
    private String[] _hosts;
    private int _port;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _task;
    private String _root;

    public LoginBrokerHandler()
    {
        try {
            setAddresses(NetworkUtils.getLocalAddressesV4());
        } catch (SocketException e) {
            _log.error("Failed to obtain the IP addresses of this host: " +
                       e.getMessage());
        }
    }

    public static final String hh_lb_set_update = "<updateTime/sec>";
    public synchronized String ac_lb_set_update_$_1(Args args)
    {
        long time = Long.parseLong(args.argv(0));
        if (time < 2) {
            throw new IllegalArgumentException("Update time out of range");
        }

        _brokerUpdateTime = time;
        _brokerUpdateTimeUnit = TimeUnit.SECONDS;
        rescheduleTask();

        return "";
    }

    public static final String hh_lb_set_threshold = "<threshold>";
    public synchronized String ac_lb_set_threshold_$_1(Args args)
    {
        setUpdateThreshold(Double.parseDouble(args.argv(0)));
        return "";
    }

    private synchronized void sendUpdate()
    {
        if (_loginBrokers == null || _hosts == null) {
            return;
        }

        LoginBrokerInfo info =
            new LoginBrokerInfo(getCellName(),
                                getCellDomainName(),
                                _protocolFamily,
                                _protocolVersion,
                                _protocolEngine,
                                _root);
        info.setUpdateTime(_brokerUpdateTimeUnit.toMillis(_brokerUpdateTime));
        info.setHosts(_hosts);
        info.setPort(_port);
        info.setLoad(_load.getLoad());

        UpdateMode newUpdateMode = UpdateMode.NORMAL;
        for (String loginBroker: _loginBrokers) {
            try {
                sendMessage(new CellMessage(new CellPath(loginBroker), info));
            } catch (NoRouteToCellException e) {
                _log.warn("Failed to send update to {}", loginBroker);
                newUpdateMode = UpdateMode.EAGER;
            }
        }

        if (_currentUpdateMode != newUpdateMode) {
            _currentUpdateMode = newUpdateMode;
            rescheduleTask();
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        if (_loginBrokers == null || _task == null) {
            pw.println("    Login Broker : DISABLED");
            return;
        }
        pw.println("    LoginBroker      : " + Arrays.toString(_loginBrokers));
        pw.println("    Protocol Family  : " + _protocolFamily);
        pw.println("    Protocol Version : " + _protocolVersion);
        pw.println("    Update Time      : " + _brokerUpdateTime + " " + _brokerUpdateTimeUnit);
        pw.println("    Update Threshold : " +
                   ((int)(_brokerUpdateThreshold * 100.0)) + " %");

    }

    public synchronized void setAddresses(List<InetAddress> addresses)
    {
        _hosts = new String[addresses.size()];

        /**
         *  Add addresses ensuring preferred ordering: external
         *  addresses are before any internal interface addresses.
         */
        int nextExternalIfIndex = 0;
        int nextInternalIfIndex = addresses.size() - 1;

        for (InetAddress addr: addresses) {
            String host = addr.getCanonicalHostName();
            if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress() &&
                !addr.isSiteLocalAddress() && !addr.isMulticastAddress()) {
                _hosts[nextExternalIfIndex++] = host;
            } else {
                _hosts[nextInternalIfIndex--] = host;
            }
        }

        rescheduleTask();
    }

    public synchronized void setAddress(String host)
        throws SocketException, UnknownHostException
    {
        if (host == null) {
            // FIXME: this should include IPv6 addresses
            setAddresses(NetworkUtils.getLocalAddressesV4());
        } else {
            InetAddress address = InetAddress.getByName(host);

            if (address.isAnyLocalAddress()) {
                // FIXME: this should check if reporting IPv6 addresses is
                //        appropriate
                setAddresses(NetworkUtils.getLocalAddressesV4());
            } else {
                setAddresses(Collections.singletonList(address));
            }
        }
    }

    public synchronized void setPort(int port)
    {
        _port = port;
        rescheduleTask();
    }

    public synchronized void setLoad(int children, int maxChildren)
    {
        double load =
            (maxChildren > 0) ? (double)children / (double)maxChildren : 0.0;
        setLoad(new FixedLoad(load));
    }

    public synchronized void setLoad(LoadProvider load)
    {
        double diff = Math.abs(_load.getLoad() - load.getLoad());
        if (diff > _brokerUpdateThreshold) {
            rescheduleTask();
        }
        _load = load;
    }

    public synchronized void setLoginBrokers(String[] loginBrokers)
    {
        _loginBrokers = loginBrokers;
        rescheduleTask();
    }

    public synchronized String[] getLoginBrokers()
    {
        return Arrays.copyOf(_loginBrokers, _loginBrokers.length);
    }

    public synchronized void setProtocolFamily(String protocolFamily)
    {
        _protocolFamily = protocolFamily;
        rescheduleTask();
    }

    public synchronized String getProtocolFamily()
    {
        return _protocolFamily;
    }

    public synchronized void setProtocolVersion(String protocolVersion)
    {
        _protocolVersion = protocolVersion;
        rescheduleTask();
    }

    public synchronized String getProtocolVersion()
    {
        return _protocolVersion;
    }

    public synchronized void setProtocolEngine(String protocolEngine)
    {
        _protocolEngine = protocolEngine;
        rescheduleTask();
    }

    public synchronized String getProtocolEngine()
    {
        return _protocolEngine;
    }

    public synchronized void setUpdateThreshold(double threshold)
    {
        _brokerUpdateThreshold = threshold;
    }

    public synchronized double getUpdateThreshold()
    {
        return _brokerUpdateThreshold;
    }

    public synchronized void setUpdateTime(long time)
    {
        _brokerUpdateTime = time;
    }

    public synchronized long getUpdateTime()
    {
        return _brokerUpdateTime;
    }

    public synchronized void setUpdateTimeUnit(TimeUnit unit)
    {
        _brokerUpdateTimeUnit = unit;
        rescheduleTask();
    }

    public synchronized TimeUnit getUpdateTimeUnit()
    {
        return _brokerUpdateTimeUnit;
    }

    /**
     * Root directory of door.
     *
     * If null, then a per-user root directory is assumed.
     */
    public synchronized void setRoot(String root)
    {
        _root = root;
    }

    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
        rescheduleTask();
    }

    public synchronized void start()
    {
        scheduleTask();
    }

    public synchronized void stop()
    {
        if (_task != null) {
            _task.cancel(false);
            _task = null;
        }
    }

    private void rescheduleTask()
    {
        if (_task != null) {
            _task.cancel(false);
            scheduleTask();
        }
    }

    private void scheduleTask()
    {
        Runnable command = new Runnable() {
                @Override
                public void run()
                {
                    sendUpdate();
                }
            };
        switch (_currentUpdateMode) {
        case EAGER:
            _task = _executor.scheduleWithFixedDelay(command, EAGER_UPDATE_TIME, EAGER_UPDATE_TIME, MILLISECONDS);
            break;

        case NORMAL:
            _task = _executor.scheduleWithFixedDelay(command, 0, _brokerUpdateTime, _brokerUpdateTimeUnit);
            break;
        }
    }

    /**
     * Callback interface to query the current load.
     */
    public interface LoadProvider
    {
        double getLoad();
    }

    private static class FixedLoad implements LoadProvider
    {
        private double _load;

        public FixedLoad(double load)
        {
            _load = load;
        }

        @Override
        public double getLoad()
        {
            return _load;
        }
    }
}
