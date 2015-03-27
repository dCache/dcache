package dmg.cells.services.login;

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.util.Args;
import org.dcache.util.NetworkUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.concurrent.TimeUnit.*;

/**
 * Utility class to periodically register a door in a login broker.
 */
public class LoginBrokerHandler
    extends AbstractCellComponent
    implements CellCommandListener, CellMessageReceiver
{
    private static final Logger _log =
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
    private long _brokerUpdateTime = MINUTES.toMillis(5);
    private TimeUnit _brokerUpdateTimeUnit = MILLISECONDS;
    private double _brokerUpdateThreshold = 0.1;
    private UpdateMode _currentUpdateMode = UpdateMode.NORMAL;
    private LoadProvider _load = () -> 0.0;
    private Supplier<List<InetAddress>> _addresses = createAnyAddressSupplier();
    private int _port;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _task;
    private String _root;

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

    protected LoginBrokerInfo newInfo(String cell, String domain,
                                      String protocolFamily, String protocolVersion,
                                      String protocolEngine, String root, List<InetAddress> addresses, int port,
                                      double load, long updateTime)
    {
        return new LoginBrokerInfo(cell, domain, protocolFamily, protocolVersion,
                                   protocolEngine, root, addresses, port, load, updateTime);
    }

    private synchronized void sendUpdate()
    {
        if (_loginBrokers == null) {
            return;
        }

        List<InetAddress> addresses = _addresses.get();
        if (addresses.isEmpty()) {
            return;
        }

        LoginBrokerInfo info =
                newInfo(getCellName(), getCellDomainName(),
                        _protocolFamily, _protocolVersion, _protocolEngine, _root, addresses, _port, _load.getLoad(),
                        _brokerUpdateTimeUnit.toMillis(_brokerUpdateTime));

        for (String loginBroker : _loginBrokers) {
            sendMessage(new CellMessage(new CellPath(loginBroker), info));
        }

        if (_currentUpdateMode != UpdateMode.NORMAL) {
            _currentUpdateMode = UpdateMode.NORMAL;
            rescheduleTask();
        }
    }

    public synchronized void messageArrived(NoRouteToCellException e)
    {
        if (_currentUpdateMode != UpdateMode.EAGER) {
            CellAddressCore destinationAddress = e.getDestinationPath().getDestinationAddress();
            for (String loginBroker : _loginBrokers) {
                if (destinationAddress.equals(new CellAddressCore(loginBroker))) {
                    _currentUpdateMode = UpdateMode.EAGER;
                    rescheduleTask();
                    break;
                }
            }
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
        pw.println("    Port             : " + _port);
        pw.println("    Hosts            : " + _addresses.get());
        pw.println("    Update Time      : " + _brokerUpdateTime + " " + _brokerUpdateTimeUnit);
        pw.println("    Update Threshold : " +
                   ((int) (_brokerUpdateThreshold * 100.0)) + " %");
    }

    /**
     * Sets the address of the door being published. If null or a wildcard address is provided,
     * all interfaces of the door are published. If an address is provided, the canonical
     * host is resolved and published with the address. If a name is provided, the name
     * is resolved to an address and published together with the name.
     */
    public void setAddress(@Nullable String host) throws UnknownHostException
    {
        if (host == null) {
            setAddressSupplier(createAnyAddressSupplier());
        } else if (NetworkUtils.isInetAddress(host)) {
            InetAddress address = InetAddresses.forString(host);
            checkArgument(!address.isMulticastAddress());
            if (address.isAnyLocalAddress()) {
                setAddressSupplier(createAnyAddressSupplier());
            } else {
                setAddressSupplier(createSingleAddressSupplier(address));
            }
        } else {
            setAddressSupplier(createSingleAddressSupplier(InetAddress.getByName(host)));
        }
    }

    /**
     * Sets both the port and address from the given socket address.
     */
    public synchronized void setSocketAddress(InetSocketAddress socketAddress)
    {
        InetAddress address = socketAddress.getAddress();
        checkArgument(!address.isMulticastAddress());
        _port = socketAddress.getPort();
        if (address.isAnyLocalAddress()) {
            setAddressSupplier(createAnyAddressSupplier());
        } else if (NetworkUtils.isInetAddress(socketAddress.getHostString())) {
            InetAddress canonicalAddress = NetworkUtils.withCanonicalAddress(address);
            setAddressSupplier(() -> Collections.singletonList(canonicalAddress));
        } else {
            setAddressSupplier(() -> Collections.singletonList(address));
        }
    }

    public synchronized void setAddressSupplier(Supplier<List<InetAddress>> addresses)
    {
        _addresses = addresses;
        rescheduleTask();
    }

    public synchronized void setPort(int port)
    {
        _port = port;
        rescheduleTask();
    }

    public synchronized void setLoad(int children, int maxChildren)
    {
        double load =
                (maxChildren > 0) ? (double) children / (double) maxChildren : 0.0;
        setLoadProvider(() -> load);
    }

    public synchronized void setLoadProvider(LoadProvider load)
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
        Runnable command = new Runnable()
        {
            @Override
            public void run()
            {
                try {
                    sendUpdate();
                } catch (Throwable e) {
                    Thread thisThread = Thread.currentThread();
                    UncaughtExceptionHandler ueh = thisThread.getUncaughtExceptionHandler();
                    ueh.uncaughtException(thisThread, e);
                    Throwables.propagateIfPossible(e);
                }
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

    public static Supplier<List<InetAddress>> createSingleAddressSupplier(InetAddress address)
    {
        return () -> Collections.singletonList(NetworkUtils.withCanonicalAddress(address));
    }

    public static Supplier<List<InetAddress>> createAnyAddressSupplier()
    {
        String localHostAddresses = System.getProperty(NetworkUtils.LOCAL_HOST_ADDRESS_PROPERTY);
        if (!isNullOrEmpty(localHostAddresses)) {
            List<InetAddress> address = new ArrayList<>();
            for (String s : Splitter.on(',').omitEmptyStrings().trimResults().split(localHostAddresses)) {
                address.add(NetworkUtils.withCanonicalAddress(InetAddresses.forString(s)));
            }
            return () -> address;
        }

        return new AnyAddressSupplier();
    }

    /**
     * Callback interface to query the current load.
     */
    public interface LoadProvider
    {
        double getLoad();
    }

    public static class AnyAddressSupplier implements Supplier<List<InetAddress>>
    {
        @Override
        public List<InetAddress> get()
        {
            ArrayList<InetAddress> addresses = new ArrayList<>();
            try {
                Enumeration<NetworkInterface> interfaces =
                        NetworkInterface.getNetworkInterfaces();
                while (interfaces.hasMoreElements()) {
                    NetworkInterface i = interfaces.nextElement();
                    try {
                        if (i.isUp() && !i.isLoopback()) {
                            Enumeration<InetAddress> e = i.getInetAddresses();
                            while (e.hasMoreElements()) {
                                addresses.add(NetworkUtils.withCanonicalAddress(e.nextElement()));
                            }
                        }
                    } catch (SocketException e) {
                        _log.warn("Not publishing NIC {}: {}", i.getName(), e.getMessage());
                    }
                }
            } catch (SocketException e) {
                _log.warn("Not publishing NICs: {}", e.getMessage());
            }
            return addresses;
        }
    }
}
