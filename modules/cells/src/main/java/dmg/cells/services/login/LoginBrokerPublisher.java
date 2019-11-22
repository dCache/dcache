package dmg.cells.services.login;

import com.google.common.base.Splitter;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.net.InetAddresses;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.DoubleSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellRoute;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;

import org.dcache.util.FireAndForgetTask;
import org.dcache.util.NDC;
import org.dcache.util.NetworkUtils;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * Utility class to periodically publish login broker information.
 */
public class LoginBrokerPublisher
    extends AbstractCellComponent
    implements CellCommandListener, CellMessageReceiver, CellEventListener, CellLifeCycleAware, CellInfoProvider
{
    private static final Logger _log =
            LoggerFactory.getLogger(LoginBrokerPublisher.class);

    private enum LastEvent
    {
        NONE, UPDATE_SUBMITTED, UPDATE_SENT, ROUTE_ADDED, NOROUTE
    }

    private CellAddressCore _topic;
    private String _protocolFamily;
    private String _protocolVersion;
    private String _protocolEngine;
    private long _brokerUpdateTime = MINUTES.toMillis(5);
    private TimeUnit _brokerUpdateTimeUnit = MILLISECONDS;
    private double _brokerUpdateThreshold = 0.1;
    private LastEvent _lastEvent = LastEvent.NONE;
    private DoubleSupplier _load = () -> 0.0;
    private Supplier<List<InetAddress>> _addresses = createAnyAddressSupplier();
    private int _port;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _task;
    private String _root = "/";
    private List<String> _readPaths = Collections.emptyList();
    private List<String> _writePaths = Collections.emptyList();
    private boolean _readEnabled = true;
    private boolean _writeEnabled = true;
    private List<InetAddress> _lastAddresses = Collections.emptyList();

    /**
     * Tags to advertise. For thread safety, the list must not be modified.
     * Instead it has to be copied and the field must be updated.
     */
    private List<String> _tags = Collections.emptyList();

    @Command(name = "lb set update", hint = "set login broker update frequency",
            description = "Defines how often information about this doors should be published.")
    class SetUpdateCommand implements Callable<String>
    {
        @Argument(metaVar = "seconds")
        int time;

        @Override
        public String call() throws IllegalArgumentException
        {
            checkArgument(time >= 2, "Update time out of range.");
            setBrokerUpdateTime(_brokerUpdateTime, _brokerUpdateTimeUnit);
            return "";
        }
    }

    @Command(name = "lb set threshold", hint = "set threshold load for OOB updates",
            description = "Sets the relative threshold for sending out-of-band updates. If the " +
                          "load of this for changes by a factor more than this threshold, an " +
                          "immediate update is published.")
    class SetThresholdCommand implements Callable<String>
    {
        @Argument
        double load;

        @Override
        public String call()
        {
            setUpdateThreshold(load);
            return "";
        }
    }

    @Command(name = "lb set tags", hint = "set published tags",
            description = "Doors may be tagged and subscribers of door information may filter " +
                          "by these tags.")
    class SetTagCommand implements Callable<String>
    {
        @Argument(required = false)
        String[] tags = {};

        @Override
        public String call()
        {
            setTags(asList(tags));
            return "";
        }
    }

    @Command(name = "lb disable", hint = "suspend publishing capabilities",
            description = "Allows to temporarily suppress publishing of read and write capabilities. " +
                          "It will appear as it the door does not authorize access to any read and/or " +
                          "write paths. Without additional options, both read and write capabilities " +
                          "will be suspended.\n\n" +
                          "Note that this does not actually disable the door. Only the advertized " +
                          "capabilities are changed.")
    class DisableCommand implements Callable<String>
    {
        @Option(name = "read")
        boolean read;

        @Option(name = "write")
        boolean write;

        @Override
        public String call()
        {
            if (read || !write) {
                setReadEnabled(false);
            }
            if (write || !read) {
                setWriteEnabled(false);
            }
            return "";
        }
    }

    @Command(name = "lb enable", hint = "resume publishing capabilities",
            description = "Allows to continue publishing read and/or write capabilities. Without " +
                          "additional options, both read and write capabilities will be published " +
                          "in correspondence with the door's configuration.")
    class EnableCommand implements Callable<String>
    {
        @Option(name = "read")
        boolean read;

        @Option(name = "write")
        boolean write;

        @Override
        public String call()
        {
            if (read || !write) {
                setReadEnabled(true);
            }
            if (write || !read) {
                setWriteEnabled(true);
            }
            return "";
        }
    }

    private synchronized Optional<LoginBrokerInfo> createLoginBrokerInfo(List<InetAddress> addresses)
    {
        _lastAddresses = addresses;
        if (_task != null && !addresses.isEmpty()) {
            Collection<String> readPaths = _readEnabled ? _readPaths : Collections.emptyList();
            Collection<String> writePaths = _writeEnabled ? _writePaths : Collections.emptyList();
            return Optional.of(
                    new LoginBrokerInfo(getCellName(), getCellDomainName(), _protocolFamily, _protocolVersion,
                                        _protocolEngine, _root, readPaths, writePaths, _tags, addresses, _port,
                                        _load.getAsDouble(), _brokerUpdateTimeUnit.toMillis(_brokerUpdateTime)));
        }
        return Optional.empty();
    }

    private synchronized void sendUpdate(Optional<LoginBrokerInfo> info)
    {
        if (_topic != null) {
            _lastEvent = LastEvent.UPDATE_SENT;
            if (info.isPresent()) {
                sendMessage(new CellMessage(_topic, info.get()));
            }
        }
    }

    protected void submitUpdate()
    {
        _lastEvent = LastEvent.UPDATE_SUBMITTED;
        _executor.execute(this::sendUpdate);
    }

    private void sendUpdate()
    {
        sendUpdate(createLoginBrokerInfo(getAddressSupplier().get()));
    }

    public synchronized void messageArrived(NoRouteToCellException e)
    {
        CellAddressCore destinationAddress = e.getDestinationPath().getDestinationAddress();
        if (_topic != null && destinationAddress.equals(_topic)) {
            switch (_lastEvent) {
            case UPDATE_SENT:
                _lastEvent = LastEvent.NOROUTE;
                break;
            case ROUTE_ADDED:
                submitUpdate();
                break;
            default:
                break;
            }
        }
    }

    public LoginBrokerInfo messageArrived(LoginBrokerInfoRequest msg)
    {
        return createLoginBrokerInfo(getAddressSupplier().get()).orElse(null);
    }

    @Override
    public synchronized void routeAdded(CellEvent ce)
    {
        CellRoute route = (CellRoute) ce.getSource();
        if (route.getRouteType() == CellRoute.TOPIC || route.getRouteType() == CellRoute.DOMAIN) {
            switch (_lastEvent) {
            case UPDATE_SENT:
                _lastEvent = LastEvent.ROUTE_ADDED;
                break;
            case NOROUTE:
                submitUpdate();
                break;
            default:
                break;
            }
        }
    }

    @Override
    public synchronized void getInfo(PrintWriter pw)
    {
        if (_topic == null || _task == null) {
            pw.println("    Login Broker : DISABLED");
            return;
        }
        pw.println("    LoginBroker      : " + _topic);
        pw.println("    Protocol Family  : " + _protocolFamily);
        pw.println("    Protocol Version : " + _protocolVersion);
        pw.println("    Port             : " + _port);
        pw.println("    Addresses        : " + _lastAddresses);
        pw.println("    Tags             : " + _tags);
        pw.println("    Root             : " + Strings.nullToEmpty(_root));
        pw.println("    Read paths       : " + _readPaths + (_readEnabled ? "" : " (disabled)"));
        pw.println("    Write paths      : " + _writePaths  + (_writeEnabled ? "" : " (disabled)"));
        pw.println("    Update Time      : " + _brokerUpdateTime + ' ' + _brokerUpdateTimeUnit);
        pw.println("    Update Threshold : " + ((int) (_brokerUpdateThreshold * 100.0)) + " %");
        pw.println("    Last event       : " + _lastEvent);
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
        } else {
            setAddressSupplier(NetworkUtils.hostListAddressSupplier(host));
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

    public synchronized Supplier<List<InetAddress>> getAddressSupplier()
    {
        return _addresses;
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

    public synchronized void setLoadProvider(DoubleSupplier load)
    {
        double diff = Math.abs(_load.getAsDouble() - load.getAsDouble());
        if (diff > _brokerUpdateThreshold) {
            rescheduleTask();
        }
        _load = load;
    }

    public synchronized void setTopic(String topic)
    {
        _topic = new CellAddressCore(topic);
        rescheduleTask();
    }

    public synchronized String getTopic()
    {
        return Objects.toString(_topic, null);
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
     * <p>
     * If null, then a per-user root directory is assumed.
     */
    public synchronized void setRoot(String root)
    {
        _root = root;
    }

    public synchronized void setReadPaths(List<String> paths)
    {
        checkArgument(!paths.stream().anyMatch(String::isEmpty));
        _readPaths = paths;
        rescheduleTask();
    }

    public synchronized void setWritePaths(List<String> paths)
    {
        checkArgument(!paths.stream().anyMatch(String::isEmpty));
        _writePaths = paths;
        rescheduleTask();
    }

    public synchronized void setTags(List<String> tags)
    {
        _tags = tags;
        rescheduleTask();
    }

    public synchronized void setWriteEnabled(boolean enabled)
    {
        _writeEnabled = enabled;
        rescheduleTask();
    }

    public synchronized void setReadEnabled(boolean enabled)
    {
        _readEnabled = enabled;
        rescheduleTask();
    }

    public synchronized void setBrokerUpdateTime(long time, TimeUnit unit)
    {
        _brokerUpdateTime = time;
        _brokerUpdateTimeUnit = unit;
        rescheduleTask();
    }

    public synchronized void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
        rescheduleTask();
    }

    @Override
    public synchronized void afterStart()
    {
        scheduleTask();
    }

    @Override
    public synchronized void beforeStop()
    {
        if (_task != null) {
            _task.cancel(true);
            _task = null;
        }
        _addresses = Collections::emptyList;
        _writeEnabled = false;
        _readEnabled = false;
        _load = () -> 1.0;
        sendUpdate();
    }

    @GuardedBy("this")
    private void rescheduleTask()
    {
        if (_task != null) {
            _task.cancel(false);
            scheduleTask();
        }
    }

    private void scheduleTask()
    {
        _task = _executor.scheduleWithFixedDelay(
                new FireAndForgetTask(this::sendUpdate), 0, _brokerUpdateTime, _brokerUpdateTimeUnit);
    }

    public static Supplier<List<InetAddress>> createSingleAddressSupplier(InetAddress address)
    {
        return () -> Collections.singletonList(address);
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

        return NetworkUtils.anyAddressSupplier();
    }
}
