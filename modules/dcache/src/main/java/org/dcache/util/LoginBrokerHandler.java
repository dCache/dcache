package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.login.LoginBrokerInfo;
import dmg.util.Args;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.cells.CellCommandListener;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Utility class to periodically register a door in a login broker.
 */
public class LoginBrokerHandler
    extends AbstractCellComponent
    implements CellCommandListener
{
    private final static Logger _log =
        LoggerFactory.getLogger(LoginBrokerHandler.class);

    private static final long EAGER_UPDATE_TIME = SECONDS.toMillis(1);

    private CellPath _loginBroker;
    private String _protocolFamily;
    private String _protocolVersion;
    private String _protocolEngine;
    private long   _brokerUpdateTime = MINUTES.toMillis(5);
    private TimeUnit _brokerUpdateTimeUnit = MILLISECONDS;
    private long _currentBrokerUpdateTime = EAGER_UPDATE_TIME;
    private double _brokerUpdateThreshold = 0.1;
    private LoadProvider _load = new FixedLoad(0.0);
    private String[] _hosts;
    private int _port;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _task;

    public LoginBrokerHandler()
    {
        try {
            setAddresses(NetworkUtils.getLocalAddressesV4());
        } catch (SocketException e) {
            _log.error("Failed to obtain the IP addresses of this host: " +
                       e.getMessage());
        }
    }

    public final static String hh_lb_set_update = "<updateTime/sec>";
    public synchronized String ac_lb_set_update_$_1(Args args)
    {
        setUpdateTime(Long.parseLong(args.argv(0)));
        return "";
    }

    public final static String hh_lb_set_threshold = "<threshold>";
    public synchronized String ac_lb_set_threshold_$_1(Args args)
    {
        setUpdateThreshold(Double.parseDouble(args.argv(0)));
        return "";
    }

    private synchronized void sendUpdate()
    {
        if (_loginBroker == null || _hosts == null) {
            return;
        }

        LoginBrokerInfo info =
            new LoginBrokerInfo(getCellName(),
                                getCellDomainName(),
                                _protocolFamily,
                                _protocolVersion,
                                _protocolEngine);
        info.setUpdateTime(_brokerUpdateTimeUnit.toMillis(_brokerUpdateTime));
        info.setHosts(_hosts);
        info.setPort(_port);
        info.setLoad(_load.getLoad());

        try {
            sendMessage(new CellMessage(_loginBroker, info));
            normalUpdates();
        } catch (NoRouteToCellException e) {
            _log.error("Failed to send update to " + _loginBroker);
            eagerUpdates();
        }
    }

    private void eagerUpdates()
    {
        if (_currentBrokerUpdateTime != EAGER_UPDATE_TIME) {
            _currentBrokerUpdateTime = EAGER_UPDATE_TIME;
            rescheduleTask();
        }
    }

    private void normalUpdates()
    {
        long millis = _brokerUpdateTimeUnit.toMillis(_brokerUpdateTime);
        if (_currentBrokerUpdateTime != millis) {
            _currentBrokerUpdateTime = millis;
            rescheduleTask();
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        if (_loginBroker == null) {
            pw.println("    Login Broker : DISABLED");
            return;
        }
        pw.println("    LoginBroker      : " + _loginBroker);
        pw.println("    Protocol Family  : " + _protocolFamily);
        pw.println("    Protocol Version : " + _protocolVersion);
        pw.println("    Update Time      : " + _brokerUpdateTime +
                   " seconds");
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
        InetAddress address = InetAddress.getByName(host);
        if (address.isAnyLocalAddress()) {
            setAddresses(NetworkUtils.getLocalAddressesV4());
        } else {
            setAddresses(Collections.singletonList(address));
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

    public synchronized void setLoginBroker(CellPath loginBroker)
    {
        _loginBroker = loginBroker;
        rescheduleTask();
    }

    public synchronized CellPath getLoginBroker()
    {
        return (CellPath) _loginBroker.clone();
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
        if (time < 2) {
            throw new IllegalArgumentException("Update time out of range");
        }
        _brokerUpdateTime = time;
        rescheduleTask();
    }

    public synchronized long getUpdateTime()
    {
        return _brokerUpdateTime;
    }

    public synchronized void setUpdateTimeUnit(TimeUnit unit)
    {
        _brokerUpdateTimeUnit = unit;
    }

    public synchronized TimeUnit getUpdateTimeUnit()
    {
        return _brokerUpdateTimeUnit;
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
        _task = _executor.scheduleWithFixedDelay(command, 0, _currentBrokerUpdateTime,
                                                 SECONDS);
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
