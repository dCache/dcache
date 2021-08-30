package diskCacheV111.poolManager;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.io.PrintWriter;
import java.lang.Thread.UncaughtExceptionHandler;
import java.net.InetSocketAddress;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.CheckStagePermission;
import diskCacheV111.util.DestinationCostException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.SourceCostException;
import diskCacheV111.vehicles.DCapProtocolInfo;
import diskCacheV111.vehicles.IpProtocolInfo;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.Pool2PoolTransferMsg;
import diskCacheV111.vehicles.PoolFetchFileMessage;
import diskCacheV111.vehicles.PoolHitInfoMessage;
import diskCacheV111.vehicles.PoolMgrReplicateFileMsg;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import diskCacheV111.vehicles.ProtocolInfo;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.WarningPnfsFileInfoMessage;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellSetupProvider;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.CostException;
import org.dcache.poolmanager.Partition;
import org.dcache.poolmanager.PartitionManager;
import org.dcache.poolmanager.PoolInfo;
import org.dcache.poolmanager.PoolManagerGetRestoreHandlerInfo;
import org.dcache.poolmanager.PoolSelector;
import org.dcache.poolmanager.SelectedPool;
import org.dcache.util.Args;
import org.dcache.util.FireAndForgetTask;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static diskCacheV111.poolManager.RequestContainerV5.StateType.TRANSITORY;
import static diskCacheV111.poolManager.RequestContainerV5.StateType.WAITING;
import static dmg.util.CommandException.checkCommand;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.dcache.util.MathUtils.addWithInfinity;

public class RequestContainerV5
        extends AbstractCellComponent
        implements Runnable, CellCommandListener, CellMessageReceiver, CellSetupProvider, CellInfoProvider
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RequestContainerV5.class);

    protected enum StateType {
        /**
         * A TRANSITORY state is one in which a call to stateEngine method is
         * guaranteed to change the state.  Such states require no (external)
         * stimulus.
         */
        TRANSITORY,

        /**
         * A WAITING state is one in which the request is waiting asynchronously
         * for some external event.  Such states require some (external)
         * stimulus to trigger a change in state.
         */
        WAITING
    }

    /**
     * The various states that a request can adopt as it is being processed.
     */
    public enum RequestState {
        /** The initial request state. */
        ST_INIT(TRANSITORY),

        /**
         * It has been determined that the request cannot be satisfied as-is and
         * a pool-to-pool copy is required.  The pool-to-pool copy is to be
         * initiated.
         */
        ST_POOL_2_POOL(TRANSITORY),

        /**
         * It has been determined that the request cannot be satisfied as-is and
         * the file should be staged back from tape.  The staging request is to
         * be initiated.
         */
        ST_STAGE(TRANSITORY),

        /**
         * A pool has been requested to stage a file.  Waiting for that stage
         * request to complete.
         */
        ST_WAITING_FOR_STAGING(WAITING),

        /**
         * A pool has been requested to create a replica: a pool-to-pool copy.
         * Waiting for that pool-to-pool request to complete.
         */
        ST_WAITING_FOR_POOL_2_POOL(WAITING),

        /**
         * No further processing is currently possible.  External changes (such
         * as pools becoming available or manual intervention) may trigger a
         * change in state.
         */
        ST_SUSPENDED(WAITING),

        /** All processing is now completed.  Nothing further to do. */
        ST_OUT(TRANSITORY);

        private final StateType type;

        public boolean is(StateType type)
        {
            return this.type == type;
        }

        RequestState(StateType type)
        {
            this.type = type;
        }
    }

    private static final String POOL_UNKNOWN_STRING  = "<unknown>";

    private static final String STRING_NEVER = "never";
    private static final String STRING_BESTEFFORT = "besteffort";
    private static final String STRING_NOTCHECKED = "notchecked";

    /** value in milliseconds */
    private static final int DEFAULT_TICKER_INTERVAL = 60000;

    private static final DateTimeFormatter DATE_TIME_FORMAT = DateTimeFormatter.ofPattern("MM.dd HH:mm:ss");

    private final Map<UOID, PoolRequestHandler> _messageHash = new HashMap<>();
    private final Map<String, PoolRequestHandler> _handlerHash = new HashMap<>();

    private CellStub _billing;
    private CellStub _poolStub;
    private long _retryTimer = 15 * 60 * 1000;

    private static final int MAX_REQUEST_CLUMPING = 20;

    private String _onError = "suspend";
    private int _maxRetries = 3;
    private int _maxRestore = -1;

    private CheckStagePermission _stagePolicyDecisionPoint;
    private boolean _allowAnonymousStaging;

    private boolean _sendHitInfo;

    private int _restoreExceeded;
    private boolean _suspendIncoming;
    private boolean _suspendStaging;

    private PoolSelectionUnit _selectionUnit;
    private PoolMonitorV5 _poolMonitor;
    private PnfsHandler _pnfsHandler;

    private Executor _executor;
    private final Map<PnfsId, CacheException> _selections = new ConcurrentHashMap<>();
    private PartitionManager _partitionManager;
    private volatile long _checkFilePingTimer = 10 * 60 * 1000;
    /** value in milliseconds */
    private final long _ticketInterval;

    private Thread _tickerThread;

    private PoolPingThread _poolPingThread;

    /**
     * Tape Protection.
     * allStates defines that all states are allowed.
     * allStatesExceptStage defines that all states except STAGE are allowed.
     */
    public static final EnumSet<RequestState> allStates =
            EnumSet.allOf(RequestState.class);

    public static final EnumSet<RequestState> allStatesExceptStage =
            EnumSet.complementOf(EnumSet.of(RequestState.ST_STAGE));

    /**
     * RC state machine states sufficient to access online files.
     */
    public static final EnumSet<RequestState> ONLINE_FILES_ONLY
            = EnumSet.of(RequestState.ST_INIT);

    public RequestContainerV5(long tickerInterval) {
        _ticketInterval = tickerInterval;
    }

    public RequestContainerV5()
    {
        this(DEFAULT_TICKER_INTERVAL);
    }

    @Override
    public CellSetupProvider mock()
    {
        RequestContainerV5 mock = new RequestContainerV5();
        mock.setPartitionManager(new PartitionManager());
        return mock;
    }


    public void start()
    {
        _tickerThread = new Thread(this, "Container-ticker");
        _tickerThread.start();
        _poolPingThread = new PoolPingThread();
        _poolPingThread.start();
    }

    public void shutdown()
    {
        if (_tickerThread != null) {
            _tickerThread.interrupt();
        }
        if (_poolPingThread != null) {
            _poolPingThread.interrupt();
        }
    }

    @VisibleForTesting
    void pingAllPools() throws InterruptedException
    {
        if (_poolPingThread != null) {
            _poolPingThread.checkNow();
        }
    }

    @Required
    public void setPoolSelectionUnit(PoolSelectionUnit selectionUnit)
    {
        _selectionUnit = selectionUnit;
    }

    @Required
    public void setPoolMonitor(PoolMonitorV5 poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    @Required
    public void setPnfsHandler(PnfsHandler pnfsHandler)
    {
        _pnfsHandler = pnfsHandler;
    }

    @Required
    public void setPartitionManager(PartitionManager partitionManager)
    {
        _partitionManager = partitionManager;
    }

    @Required
    public void setExecutor(Executor executor)
    {
        _executor = executor;
    }

    public void setHitInfoMessages(boolean sendHitInfo)
    {
        _sendHitInfo = sendHitInfo;
    }

    @Required
    public void setBilling(CellStub billing)
    {
        _billing = billing;
    }

    @Required
    public void setPoolStub(CellStub poolStub)
    {
        _poolStub = poolStub;
    }

    public void messageArrived(CellMessage envelope, Object message)
    {
        UOID uoid = envelope.getLastUOID();
        PoolRequestHandler handler;

        synchronized (_messageHash) {
            handler = _messageHash.remove(uoid);
            if (handler == null) {
                return;
            }
        }

        handler.mailForYou(message);
    }

    @Override
    public void run()
    {
        while (!Thread.interrupted()) {
            try {
                Thread.sleep(_ticketInterval);

                List<PoolRequestHandler> list;
                synchronized (_handlerHash) {
                    list = new ArrayList<>(_handlerHash.values());
                }
                list.forEach(PoolRequestHandler::checkExpiredRequests);
            } catch (InterruptedException e) {
                break;
            } catch (Throwable t) {
                Thread thisThread = Thread.currentThread();
                UncaughtExceptionHandler ueh =
                        thisThread.getUncaughtExceptionHandler();
                ueh.uncaughtException(thisThread, t);
            }
        }
        LOGGER.debug("Container-ticker done");
    }

    public void poolStatusChanged(String poolName, int poolStatus) {
        LOGGER.info("Restore Manager : got 'poolRestarted' for {}", poolName);
        try {
            List<PoolRequestHandler> list;
            synchronized (_handlerHash) {
                list = new ArrayList<>(_handlerHash.values());
            }

            for (PoolRequestHandler rph : list) {
                switch (poolStatus) {
                case PoolStatusChangedMessage.UP:
                    /*
                     * if pool is up, re-try all request scheduled to this pool
                     * and all requests, which do not have any pool candidates
                     *
                     * in this construction we will fall down to next case
                     */
                    if (rph.getPoolCandidate().equals(POOL_UNKNOWN_STRING)) {
                        LOGGER.info("Restore Manager : retrying : {}", rph);
                        rph.retry();
                    }
                case PoolStatusChangedMessage.DOWN:
                    /*
                     * if pool is down, re-try all request scheduled to this
                     * pool
                     */
                    if (rph.getPoolCandidate().equals(poolName)) {
                        LOGGER.info("Restore Manager : retrying : {}", rph);
                        rph.retry();
                    }
                }
            }
        } catch (RuntimeException e) {
            LOGGER.error("Problem retrying pool " + poolName, e);
        }
    }

    @Override
    public void getInfo(PrintWriter pw)
    {
        Partition def = _partitionManager.getDefaultPartition();

        pw.println("      Retry Timeout : " + (_retryTimer/1000) + " seconds");
        pw.println("  Thread Controller : " + _executor);
        pw.println("    Maximum Retries : " + _maxRetries);
        pw.println("    Pool Ping Timer : " + (_checkFilePingTimer/1000) + " seconds");
        pw.println("           On Error : " + _onError);
        pw.println("          Allow p2p : " + (def._p2pAllowed ? "on" : "off") +
                " oncost=" + (def._p2pOnCost ? "on" : "off") +
                " fortransfer=" + (def._p2pForTransfer ? "on" : "off"));
        pw.println("      Allow staging : " + (def._hasHsmBackend ? "on":"off"));
        pw.println("Allow stage on cost : " + (def._stageOnCost ? "on":"off"));
        pw.println("      Restore Limit : " + (_maxRestore<0?"unlimited":(String.valueOf(_maxRestore))));
        pw.println("   Restore Exceeded : " + _restoreExceeded);
        if (_suspendIncoming) {
            pw.println("   Suspend Incoming : on (not persistent)");
        }
        if (_suspendStaging) {
            pw.println("   Suspend Staging  : on (not persistent)");
        }
    }

    @Override
    public void printSetup(PrintWriter pw)
    {
        pw.append("rc onerror ").println(_onError);
        pw.append("rc set max retries ").println(_maxRetries);
        pw.append("rc set retry ").println(_retryTimer/1000);
        pw.append("rc set poolpingtimer ").println(_checkFilePingTimer/1000);
        pw.append("rc set max restore ")
                .println(_maxRestore < 0 ? "unlimited" : (String.valueOf(_maxRestore)));
    }

    public static final String hh_rc_set_sameHostCopy =
            STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
    @AffectsSetup
    public String ac_rc_set_sameHostCopy_$_1(Args args)
    {
        _partitionManager.setProperties("default", ImmutableMap.of("sameHostCopy", args.argv(0)));
        return "";
    }

    public static final String hh_rc_set_sameHostRetry =
            STRING_NEVER+"|"+STRING_BESTEFFORT+"|"+STRING_NOTCHECKED;
    @AffectsSetup
    public String ac_rc_set_sameHostRetry_$_1(Args args)
    {
        _partitionManager.setProperties("default", ImmutableMap.of("sameHostRetry", args.argv(0)));
        return "";
    }

    public static final String fh_rc_set_max_restore = "Limit total number of concurrent restores.  If the total number of\n" +
            "restores reaches this limit then any additional restores will fail;\n" +
            "when the total number of restores drops below limit then additional\n" +
            "restores will be accepted.  Setting the limit to \"0\" will result in\n" +
            "all restores failing; setting the limit to \"unlimited\" will remove\n" +
            "the limit.";
    public static final String hh_rc_set_max_restore = "<maxNumberOfRestores>";
    @AffectsSetup
    public String ac_rc_set_max_restore_$_1(Args args) {
        if (args.argv(0).equals("unlimited")) {
            _maxRestore = -1;
            return "";
        }
        int n = Integer.parseInt(args.argv(0));
        if (n < 0) {
            throw new
                    IllegalArgumentException("must be >=0");
        }
        _maxRestore = n;
        return "";
    }
    public static final String hh_rc_select = "[<pnfsId> [<errorNumber> [<errorMessage>]] [-remove]]";
    public String ac_rc_select_$_0_3(Args args) {

        if (args.argc() == 0) {
            StringBuilder sb = new StringBuilder();
            _selections.forEach((k,v) -> sb.append(k).append("  ").append(v).append('\n'));
            return sb.toString();
        }

        PnfsId pnfsId = new PnfsId(args.argv(0));

        if (args.hasOption("remove")) {
            _selections.remove(pnfsId);
            return "";
        }

        int errorNumber = args.argc() > 1 ? Integer.parseInt(args.argv(1)) : 1;
        String errorMessage = args.argc() > 2 ? args.argv(2) : ("Failed-"+errorNumber);

        _selections.put(pnfsId, new CacheException(errorNumber, errorMessage));
        return "";
    }
    public static final String hh_rc_set_warning_path = " # obsolete";
    public String ac_rc_set_warning_path_$_0_1(Args args) {
        return "";
    }
    public static final String fh_rc_set_poolpingtimer =
            " rc set poolpingtimer <timer/seconds> "+
            ""+
            "    If set to a nonzero value, the restore handler will frequently"+
            "    check the pool whether the request is still pending, failed"+
            "    or has been successful" +
            "";
    public static final String hh_rc_set_poolpingtimer = "<checkPoolFileTimer/seconds>";
    @AffectsSetup
    public String ac_rc_set_poolpingtimer_$_1(Args args) {
        _checkFilePingTimer = 1000L * Long.parseLong(args.argv(0));

        PoolPingThread poolPingThread = _poolPingThread;
        if (poolPingThread != null) {
            synchronized (poolPingThread) {
                poolPingThread.notify();
            }
        }
        return "";
    }
    public static final String hh_rc_set_retry = "<retryTimer/seconds>";
    @AffectsSetup
    public String ac_rc_set_retry_$_1(Args args) {
        _retryTimer = 1000L * Long.parseLong(args.argv(0));
        return "";
    }
    public static final String hh_rc_set_max_retries = "<maxNumberOfRetries>";
    @AffectsSetup
    public String ac_rc_set_max_retries_$_1(Args args) {
        _maxRetries = Integer.parseInt(args.argv(0));
        return "";
    }
    public static final String hh_rc_suspend = "[on|off] -all";
    public String ac_rc_suspend_$_0_1(Args args) {
        boolean all = args.hasOption("all");
        if (args.argc() == 0) {
            if (all) {
                _suspendIncoming = true;
            }
            _suspendStaging = true;
        } else {
            String mode = args.argv(0);
            switch (mode) {
            case "on":
                if (all) {
                    _suspendIncoming = true;
                }
                _suspendStaging = true;
                break;
            case "off":
                if (all) {
                    _suspendIncoming = false;
                }
                _suspendStaging = false;
                break;
            default:
                throw new
                        IllegalArgumentException("Usage : rc suspend [on|off]");
            }

        }
        return "";
    }
    public static final String hh_rc_onerror = "suspend|fail";
    @AffectsSetup
    public String ac_rc_onerror_$_1(Args args) {
        String onerror = args.argv(0);
        if ((!onerror.equals("suspend")) &&
                (!onerror.equals("fail"))) {
            throw new
                    IllegalArgumentException("Usage : rc onerror fail|suspend");
        }

        _onError = onerror;
        return "onerror " + _onError;
    }
    public static final String fh_rc_retry =
            "NAME\n" +
            "           rc retry\n\n" +
            "SYNOPSIS\n" +
            "           I)  rc retry <pnfsId> [OPTIONS]\n" +
            "           II) rc retry * -force-all [OPTIONS]\n\n" +
            "DESCRIPTION\n" +
            "           Forces a 'restore request' to be retried.\n" +
            "           While  using syntax I, a single request  is retried,\n" +
            "           syntax II retries all requests which reported an error.\n" +
            "           If the '-force-all' options is given, all requests are\n" +
            "           retried, regardless of their current status.\n";
    public static final String hh_rc_retry = "<pnfsId>|* -force-all";
    public String ac_rc_retry_$_1(Args args)
    {
        boolean forceAll = args.hasOption("force-all");
        if (args.argv(0).equals("*")) {
            List<PoolRequestHandler> all;
            //
            // Remember : we are not allowed to call 'retry' as long
            // as we  are holding the _handlerHash lock.
            //
            synchronized (_handlerHash) {
                all = new ArrayList<>(_handlerHash.values());
            }
            all.stream()
                    .filter(h -> forceAll || h._currentRc != 0)
                    .forEach(PoolRequestHandler::retry);
        } else {
            PoolRequestHandler rph;
            synchronized (_handlerHash) {
                rph = _handlerHash.get(args.argv(0));
                if (rph == null) {
                    throw new
                            IllegalArgumentException("Not found : " + args
                                    .argv(0));
                }
            }
            rph.retry();
        }
        return "";
    }

    @Command(name = "rc failed", hint = "abort a file read request",
            description = "Aborts all requests from clients to read a"
                    + " particular file.  This is typically done when"
                    + " the request is suspended and it is impossible for"
                    + " dCache to provide that file (for example, the disk"
                    + " server with that file has been decomissioned, the tape"
                    + " containing that file's data is broken).  Although this"
                    + " command is useful for recovering from these situations,"
                    + " its usage often points to misconfiguration elsewhere in"
                    + " dCache.")
    public class FailedCommand implements Callable<String>
    {
        @Argument(usage="The request ID, as shown in the first column of the 'rc ls' command.")
        String id;

        @Argument(index=1, required=false, usage="The error number to return to the door.")
        Integer errorNumber = 1;

        @Argument(index=2, required=false, usage="The error message explaining why the transfer failed.")
        String errorString = "Operator Intervention";

        @Override
        public String call() throws CommandException
        {
            checkCommand(errorNumber >= 0, "Error number must be >= 0");

            PoolRequestHandler rph;
            synchronized (_handlerHash) {
                rph = _handlerHash.get(id);
            }

            checkCommand(rph != null, "Not found : %s", id);

            rph.fail(errorNumber, errorString);
            return "";
        }
    }

    public static final String hh_rc_ls = " [<regularExpression>] [-w] [-l] # lists pending requests";
    public String ac_rc_ls_$_0_1(Args args) {
        StringBuilder sb  = new StringBuilder();

        Pattern pattern = args.argc() > 0 ? Pattern.compile(args.argv(0)) : null;
        boolean isLongListing = args.hasOption("l");

        if (!args.hasOption("w")) {
            List<PoolRequestHandler> allRequestHandlers;
            synchronized (_handlerHash) {
                allRequestHandlers = new ArrayList<>(_handlerHash.values());
            }

            for (PoolRequestHandler h : allRequestHandlers) {
                if (h == null) {
                    continue;
                }
                String line = h.toString();
                if ((pattern == null) || pattern.matcher(line).matches()) {
                    sb.append(line).append("\n");
                    if (isLongListing) {
                        for (CellMessage m: h.getMessages()) {
                            PoolMgrSelectReadPoolMsg request =
                                    (PoolMgrSelectReadPoolMsg) m.getMessageObject();
                            sb.append("    ").append(request.getProtocolInfo()).append('\n');
                        }
                    }
                }
            }
        } else {
            Map<UOID, PoolRequestHandler>  allPendingRequestHandlers   = new HashMap<>();
            synchronized (_messageHash) {
                allPendingRequestHandlers.putAll(_messageHash);
            }

            for (Map.Entry<UOID, PoolRequestHandler> requestHandler : allPendingRequestHandlers.entrySet()) {
                UOID uoid = requestHandler.getKey();
                PoolRequestHandler h = requestHandler.getValue();

                if (h == null) {
                    continue;
                }
                String line = uoid.toString() + " " + h.toString();
                if ((pattern == null) || pattern.matcher(line).matches()) {
                    sb.append(line).append("\n");
                }

            }
        }
        return sb.toString();
    }

    public PoolManagerGetRestoreHandlerInfo messageArrived(PoolManagerGetRestoreHandlerInfo msg) {
        msg.setResult(getRestoreHandlerInfo());
        return msg;
    }

    public List<RestoreHandlerInfo> getRestoreHandlerInfo() {
        List<RestoreHandlerInfo> requests;
        synchronized (_handlerHash) {
            requests = _handlerHash.values().stream()
                    .map(PoolRequestHandler::getRestoreHandlerInfo)
                    .collect(toList());
        }
        return requests;
    }

    public static final String hh_xrc_ls = " # lists pending requests (binary)";
    public Object ac_xrc_ls(Args args) {

        List<PoolRequestHandler> all;
        synchronized (_handlerHash) {
            all = new ArrayList<>(_handlerHash.values());
        }

        return all.stream()
                .map(PoolRequestHandler::getRestoreHandlerInfo)
                .toArray(RestoreHandlerInfo[]::new);
    }

    public void messageArrived(CellMessage envelope,
                               PoolMgrSelectReadPoolMsg request)
            throws PatternSyntaxException, IOException
    {
        boolean enforceP2P = false;

        PnfsId pnfsId = request.getPnfsId();
        String poolGroup = request.getPoolGroup();
        ProtocolInfo protocolInfo = request.getProtocolInfo();
        EnumSet<RequestState> allowedStates = request.getAllowedStates();

        String hostName;
        if (protocolInfo instanceof IpProtocolInfo) {
            InetSocketAddress target = ((IpProtocolInfo)protocolInfo).getSocketAddress();
            hostName = target.isUnresolved() ? target.getHostString() : target.getAddress().getHostAddress();
        } else {
            hostName = "NoSuchHost";
        }

        String netName = _selectionUnit.getNetIdentifier(hostName);
        String protocolNameFromInfo = protocolInfo.getProtocol() + "/" + protocolInfo.getMajorVersion();

        String protocolName = _selectionUnit.getProtocolUnit(protocolNameFromInfo);
        if (protocolName == null) {
            throw new
                    IllegalArgumentException("Protocol not found : "+protocolNameFromInfo);
        }

        if (request instanceof PoolMgrReplicateFileMsg) {
            if (request.isReply()) {
                LOGGER.warn("Unexpected PoolMgrReplicateFileMsg arrived (is a reply)");
                return;
            } else {
                enforceP2P = true;
            }
        }

        String canonicalName = pnfsId + "@" + netName + "-" + protocolName + (enforceP2P ? "-p2p" : "")
                        + (poolGroup == null ? "" : ("-pg-" + poolGroup));

        LOGGER.info("Adding request for : {}", canonicalName);
        synchronized (_handlerHash) {
            _handlerHash.compute(canonicalName, (k,v) -> {
                        if (v == null) {
                            PoolRequestHandler h = new PoolRequestHandler(pnfsId, poolGroup,
                                    canonicalName, allowedStates, envelope);
                            h.start();
                            return h;
                        } else {
                            v.addRequest(envelope);
                            return v;
                        }});
        }
    }

    // replicate a file
    public static final String hh_replicate = " <pnfsid> <client IP>";
    public String ac_replicate_$_2(Args args) {

        String commandReply = "Replication initiated...";

        try {
            FileAttributes fileAttributes =
                    _pnfsHandler.getFileAttributes(new PnfsId(args.argv(0)),
                            PoolMgrReplicateFileMsg.getRequiredAttributes());

            // TODO: call p2p direct
            // send message to yourself
            PoolMgrReplicateFileMsg req =
                    new PoolMgrReplicateFileMsg(fileAttributes,
                            new DCapProtocolInfo("DCap", 3, 0,
                                    new InetSocketAddress(args.argv(1),
                                            2222)));

            sendMessage(new CellMessage(new CellAddressCore("PoolManager"), req));

        } catch (CacheException e) {
            commandReply = "P2P failed : " + e.getMessage();
        }

        return commandReply;
    }


    ///////////////////////////////////////////////////////////////
    //
    // the read io request handler
    //
    private class PoolRequestHandler {

        private final PnfsId _pnfsId;
        private final String _poolGroup;
        private final List<CellMessage> _messages = new ArrayList<>();
        private int _retryCounter;
        private final CDC _cdc = new CDC();

        /**
         * A list of objects that are notified whenever a PoolRequestHandler
         * object changes state.  The notification happens immediately after
         * the `_state` value is updated.  The supplied `RequestState` is
         * the previous (or "old") state.
         */
        private final List<Consumer<RequestState>> _observers = new CopyOnWriteArrayList<>();


        @GuardedBy("RequestContainerV5.this._messageHash")
        private UOID _waitingFor;

        private String _status = "Idle";
        private volatile RequestState _state = RequestState.ST_INIT;
        private final Collection<RequestState> _allowedStates;
        private boolean _stagingDenied;
        private int _currentRc;
        private String _currentRm = "";

        /**
         * A read-accessible pool with a replica of the data but is currently
         * considered overloaded.  If there are no read-accessible pools with a
         * replica of the file, or if there is (at least one) non-overloaded
         * pool with a replica of this file then the value is null.
         */
        @Nullable
        private SelectedPool _hotPoolWithFile;

        /**
         * The pool from which to read the file or the pool to which
         * to stage the file. Set by askIfAvailable() when it returns
         * RequestStatusCode.FOUND, by exercisePool2PoolReply() when it returns
         * RequestStatusCode.OK, and by askForStaging(). Also set in the
         * stateEngine() at various points.
         */
        private volatile SelectedPool _poolCandidate;

        /**
         * The pool used for staging.
         *
         * Serves a critical role when retrying staging to avoid that
         * the same pool is chosen twice in a row.
         */
        private Optional<SelectedPool> _stageCandidate = Optional.empty();

        /**
         * The destination of a pool to pool transfer. Set by
         * askForPoolToPool() when it returns RequestStatusCode.FOUND.
         */
        private volatile SelectedPool _p2pDestinationPool;

        /**
         * The source of a pool to pool transfer. Set by
         * askForPoolToPool() when it return RequestStatusCode.FOUND.
         */
        private SelectedPool _p2pSourcePool;

        private final long _started = System.currentTimeMillis();
        private final String _name;
        private final FileAttributes _fileAttributes;
        private final StorageInfo _storageInfo;
        private final ProtocolInfo _protocolInfo;
        private final String _linkGroup;
        private final String _billingPath;
        private final String _transferPath;
        private final PoolSelector _poolSelector;
        private final boolean _failOnExcluded;
        private final boolean _enforceP2P;
        private final int _destinationFileStatus;

        private Partition _parameter = _partitionManager.getDefaultPartition();

        /**
         * Indicates the next time a TTL of a request message will be
         * exceeded.
         */
        private long _nextTtlTimeout;

        public PoolRequestHandler(PnfsId pnfsId, String poolGroup,
                String canonicalName, Collection<RequestState> allowedStates,
                CellMessage message)
        {
            _pnfsId  = pnfsId;
            _poolGroup = poolGroup;
            _name    = canonicalName;
            _allowedStates = allowedStates;

            PoolMgrSelectReadPoolMsg request =
                    (PoolMgrSelectReadPoolMsg)message.getMessageObject();

            _messages.add(message);
            _nextTtlTimeout = addWithInfinity(System.currentTimeMillis(), message.getTtl());

            _linkGroup = request.getLinkGroup();
            _protocolInfo = request.getProtocolInfo();
            _fileAttributes = request.getFileAttributes();
            _storageInfo = _fileAttributes.getStorageInfo();
            _billingPath = request.getBillingPath();
            _transferPath = request.getTransferPath();

            _retryCounter = request.getContext().getRetryCounter();
            _stageCandidate = Optional.ofNullable(request.getContext().getPreviousStagePool());

            if (request instanceof PoolMgrReplicateFileMsg) {
                _enforceP2P = true;
                _destinationFileStatus = ((PoolMgrReplicateFileMsg)request).getDestinationFileStatus();
            } else {
                _enforceP2P = false;
                _destinationFileStatus = Pool2PoolTransferMsg.UNDETERMINED;
            }

            Set<String> excluded = request.getExcludedHosts();
            _failOnExcluded = excluded != null && !excluded.isEmpty();

            _poolSelector =
                    _poolMonitor.getPoolSelector(_fileAttributes,
                            _protocolInfo,
                            _linkGroup,
                            excluded);
            if (_sendHitInfo) {
                _observers.add(oldState -> {
                    if (_state == RequestState.ST_OUT && _currentRc == 0) {
                        switch (oldState) {
                        case ST_INIT:
                            sendHitMsg(_poolCandidate.info(), true);
                            break;

                        case ST_WAITING_FOR_POOL_2_POOL:
                            sendHitMsg(_p2pSourcePool.info(), true);
                            break;

                        case ST_WAITING_FOR_STAGING:
                            sendHitMsg(null, false);
                            break;
                        }
                    }
                });
            }
	}


        /**
         * Called once, after PoolRequestHandler constructed, to start
         * processing the first request.
         */
        public void start()
        {
            startStateEngine();
        }

        //...........................................................
        //
        // the following methods can be called from outside
        // at any time.
        //...........................................................
        //
        // add request is assumed to be synchronized by a higher level.
        //
        public void addRequest(CellMessage message) {

            PoolMgrSelectReadPoolMsg request =
                    (PoolMgrSelectReadPoolMsg)message.getMessageObject();

            // fail-fast if state is not allowed
            if (!request.getAllowedStates().contains(_state)) {
                request.setFailed(CacheException.PERMISSION_DENIED, "Pool manager state not allowed");
                message.revertDirection();
                sendMessage(message);
                return;
            }

            _messages.add(message);
            _stagingDenied = false;

            _nextTtlTimeout = Math.min(_nextTtlTimeout,
                    addWithInfinity(System.currentTimeMillis(), message.getTtl()));
        }

        public List<CellMessage> getMessages() {
            synchronized (_handlerHash) {
                return new ArrayList<>(_messages);
            }
        }

        public String getPoolCandidate()
        {
            if (_poolCandidate != null) {
                return _poolCandidate.name();
            } else if (_stageCandidate.isPresent()) {
                return _stageCandidate.get().name();
            } else if (_p2pDestinationPool != null) {
                return _p2pDestinationPool.name();
            } else {
                return POOL_UNKNOWN_STRING;
            }
        }

        private String getPoolCandidateState()
        {
            if (_stageCandidate.isPresent()) {
                return _stageCandidate.get().name();
            } else if (_p2pDestinationPool != null) {
                return (_p2pSourcePool == null ? POOL_UNKNOWN_STRING : _p2pSourcePool.name())
                    + "->" + _p2pDestinationPool.name();
            } else {
                return POOL_UNKNOWN_STRING;
            }
        }

	public RestoreHandlerInfo getRestoreHandlerInfo() {
            return new RestoreHandlerInfo(
                    _name,
                    _messages.size(),
                    _retryCounter,
                    _started,
                    getPoolCandidateState(),
                    _status,
                    _currentRc,
                    _currentRm);
	}

        @Override
        public String toString() {
            return _name + " m=" + _messages.size() + " r=" +
                    _retryCounter + " [" + getPoolCandidateState() + "] [" + _status + "] " +
                    "{" + _currentRc + "," + _currentRm + "}";
        }

        private void mailForYou(Object message) {
            //
            // !!!!!!!!! remove this
            //
            //if (message instanceof PoolFetchFileMessage) {
            //    _log.info("mailForYou !!!!! reply ignored ");
            //    return;
            //}
            add(message);
        }

        private void checkExpiredRequests()
        {
            add((Runnable)this::expireRequests);
        }

        private void retry()
        {
            add((Runnable)this::retryRequest);
        }

        private void fail(int errorCode, String errorMessage)
        {
            checkArgument(errorCode > 0, "Error number must be > 0");

            String message = errorMessage == null ? ("Error-" + _currentRc)
                    : errorMessage;

            add((Runnable)() -> failRequest(errorCode, message));
        }

        //...................................................................
        //
        // from now on, methods can only be called from within
        // the state mechanism. (which is thread save because
        // we only allow to run a single thread at a time.
        //
        private void clearSteering() {
            synchronized (_messageHash) {
                if (_waitingFor != null) {
                    _messageHash.remove(_waitingFor);
                    _waitingFor = null;
                }
            }
        }

        private void success(SelectedPool pool)
        {
            _poolCandidate = requireNonNull(pool);
            _currentRc = 0;
            _currentRm = "";
            answerRequests();
            nextStep(RequestState.ST_OUT);
        }

        private void sendFetchRequest(SelectedPool pool)
                throws MissingResourceCacheException
        {
            // TODO: Include assumption in request
            CellMessage cellMessage = new CellMessage(
                        new CellPath(pool.address()),
                        new PoolFetchFileMessage(pool.name(), _fileAttributes)
                    );
            synchronized (_messageHash) {
                if (_maxRestore >= 0 && _messageHash.size() >= _maxRestore) {
                    throw new MissingResourceCacheException("Stage attempts exceed limit "
                            + _maxRestore);
                }
                if (_waitingFor != null) {
                    _messageHash.remove(_waitingFor);
                }
                _waitingFor = cellMessage.getUOID();
                _messageHash.put(_waitingFor, this);
                sendMessage(cellMessage);
            }
        }

        private void sendPool2PoolRequest(SelectedPool sourcePool, SelectedPool destPool)
        {
            // TOOD: Include assumptions in request
            Pool2PoolTransferMsg pool2pool =
                    new Pool2PoolTransferMsg(sourcePool.name(), destPool.name(), _fileAttributes);
            pool2pool.setDestinationFileStatus(_destinationFileStatus);
            LOGGER.info("[p2p] Sending transfer request: {}", pool2pool);
            CellMessage cellMessage =
                    new CellMessage(new CellPath(destPool.address()), pool2pool);

            synchronized (_messageHash) {
                if (_waitingFor != null) {
                    _messageHash.remove(_waitingFor);
                }
                _waitingFor = cellMessage.getUOID();
                _messageHash.put(_waitingFor, this);
                sendMessage(cellMessage);
            }
        }

        private void retryRequest()
        {
            _retryCounter = -1;
            failRequest(CacheException.OUT_OF_DATE, "Operator asked for retry");
        }

        private void failRequest(int code, String message)
        {
            checkArgument(code != 0, "failRequest called with zero error");

           _currentRc = code;
           _currentRm = message;
            updateStatus("Failed: " + message);

            answerRequests();
            nextStep(RequestState.ST_OUT);
        }

        /**
         * Removes request messages who's time to live has been
         * exceeded. Messages are dropped; no reply is sent to the
         * requestor, as we assume it is no longer waiting for the
         * reply.
         */
        private void expireRequests()
        {
            /* Access to _messages is controlled by a lock on
             * _handlerHash.
             */
            synchronized (_handlerHash) {
                long now = System.currentTimeMillis();

                if (now < _nextTtlTimeout) {
                    return;
                }

                _nextTtlTimeout = Long.MAX_VALUE;

                Iterator<CellMessage> i = _messages.iterator();
                while (i.hasNext()) {
                    CellMessage message = i.next();
                    long ttl = message.getTtl();
                    if (message.getLocalAge() >= ttl) {
                        LOGGER.info("Discarding request from {} because its time to live has been exceeded.", message.getSourcePath().getCellName());
                        i.remove();
                        _nextTtlTimeout = Math.min(_nextTtlTimeout,
                                addWithInfinity(now, ttl));
                    }
                }
            }
        }

        private boolean answerRequests(int limit)
        {
            Iterator<CellMessage> messages = _messages.iterator();
            for (int i = 0; i < limit && messages.hasNext(); i++) {
                CellMessage message =  messages.next();
                answerRequest(message);
                messages.remove();
            }
            return messages.hasNext();
        }

        private void answerRequest(CellMessage message)
        {
            PoolMgrSelectReadPoolMsg rpm =
                    (PoolMgrSelectReadPoolMsg) message.getMessageObject();
            rpm.setContext(_retryCounter + 1, _stageCandidate.orElse(null));
            if (_currentRc == 0) {
                rpm.setPool(new diskCacheV111.vehicles.Pool(_poolCandidate.name(), _poolCandidate.info().getAddress(), _poolCandidate.assumption()));
                rpm.setSucceeded();
            } else {
                rpm.setFailed(_currentRc, _currentRm);
            }
            message.revertDirection();
            sendMessage(message);
        }

        //
        // and the heart ...
        //
        private final Deque<Object> _fifo = new LinkedList<>();
        private boolean _stateEngineActive;
        private boolean _overwriteCost;

        public class RunEngine implements Runnable {
            @Override
            public void run() {
                try (CDC ignored = _cdc.restore()) {
                    stateLoop();
                } finally {
                    synchronized (_fifo) {
                        _stateEngineActive = false;
                    }
                }
            }

            @Override
            public String toString() {
                return PoolRequestHandler.this.toString();
            }
        }

        private void add(Object obj) {
            synchronized (_fifo) {
                LOGGER.info("Adding Object : {}", obj);
                _fifo.addFirst(obj);

                if (!_stateEngineActive) {
                    startStateEngine();
                }
            }
        }

        private void startStateEngine()
        {
            synchronized (_fifo) {
                LOGGER.info("Starting Engine");
                _stateEngineActive = true;
                try {
                    _executor.execute(new FireAndForgetTask(new RunEngine()));
                } catch (RuntimeException e) {
                    _stateEngineActive = false;
                    throw e;
                }
            }
        }

        private void stateLoop() {
            LOGGER.info("ACTIVATING STATE ENGINE {} {}", _pnfsId, (System.currentTimeMillis()-_started));

            while (!Thread.interrupted() && _state != RequestState.ST_OUT) {

                Object inputObject;

                if (_state.is(TRANSITORY)) {
                    inputObject = null;
                } else {
                    synchronized (_fifo) {
                        inputObject = _fifo.pollLast();
                    }

                    if (inputObject == null) {
                        return;
                    }
                }

                try {
                    if (inputObject instanceof Runnable) {
                        ((Runnable)inputObject).run();
                    } else {
                        LOGGER.info("StageEngine called in mode {}", _state);

                        RequestState formerState = _state;

                        stateEngine(inputObject);

                        if (_state.is(TRANSITORY) && _state == formerState) {
                            throw new RuntimeException("Loop detected in state " + _state);
                        }

                        LOGGER.info("StageEngine left with: {}", _state);
                    }
                } catch (RuntimeException e) {
                    LOGGER.error("Bug in state loop for {}", _pnfsId, e);
                    failRequest(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                            "Bug detected: " + e);
                }
            }
        }

        private boolean isFileStageable()
        {
            return _parameter._hasHsmBackend && _storageInfo.isStored();
        }

        private boolean isStagingAllowed()
        {
            /* If the result is cached or the door disabled staging,
             * then we don't check the permissions.
             */
            if (_stagingDenied || !_allowedStates.contains(RequestState.ST_STAGE)) {
                return false;
            }

            /* Staging is allowed if just one of the requests has
             * permission to stage.
             */
            for (CellMessage envelope: _messages) {
                try {
                    PoolMgrSelectReadPoolMsg msg =
                        (PoolMgrSelectReadPoolMsg) envelope.getMessageObject();
                    if (_stagePolicyDecisionPoint.canPerformStaging(msg.getSubject(),
                                                                    msg.getFileAttributes(),
                                                                    msg.getProtocolInfo())) {
                        return true;
                    }
                } catch (IOException | PatternSyntaxException e) {
                    LOGGER.error("Failed to verify stage permissions: {}", e.getMessage());
                }
            }

            /* None of the requests had the necessary credentials to
             * stage. This result is cached.
             */
            _stagingDenied = true;
            return false;
        }

        private void nextStep(RequestState state) {
            RequestState oldState = _state;
            if (state == RequestState.ST_OUT) {
                // end state
                _state = RequestState.ST_OUT;
                _observers.forEach(e -> e.accept(oldState));
                return;
            }

            if (state == RequestState.ST_STAGE && !isStagingAllowed()) {
                _state = RequestState.ST_OUT;
                updateStatus("Failed: stage not allowed");
                LOGGER.debug("Subject is not authorized to stage");
                _currentRc = CacheException.PERMISSION_DENIED;
                _currentRm = "File not online. Staging not allowed.";
                sendInfoMessage(
                        _currentRc , "Permission denied." + _currentRm);
                answerRequests();
            } else if (!_allowedStates.contains(state)) {
                _state = RequestState.ST_OUT;
                updateStatus("Failed: transition to " + state + " not allowed");
                LOGGER.debug("No permission to perform {}", state);
                _currentRc = CacheException.PERMISSION_DENIED;
                _currentRm = "Permission denied.";
                sendInfoMessage(_currentRc,
                        "Permission denied for " + state);
                answerRequests();
            } else {
                _state = state;
                _currentRc = 0;
                _currentRm = "";
            }
            _observers.forEach(e -> e.accept(oldState));
        }

        //
        //  askIfAvailable :
        //
        //      default : (bestPool=set,overwriteCost=false) otherwise mentioned
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : file is on pool which is allowed and has reasonable cost.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_FOUND :
        //
        //         Because : file is not in cache at all
        //
        //         (bestPool=0)
        //
        //         -> _hasHsmBackend : STAGE
        //              else         : Suspended (CacheException.POOL_UNAVAILABLE, pool unavailable)
        //
        //      RequestStatusCode.NOT_PERMITTED :
        //
        //         Because : file not in an permitted pool but somewhere else
        //
        //         (bestPool=0,overwriteCost=true)
        //
        //         -> _p2pAllowed ||
        //            ! _hasHsmBackend  : P2P
        //            else              : STAGE
        //
        //      RequestStatusCode.COST_EXCEEDED :
        //
        //         Because : file is in permitted pools but cost is too high.
        //
        //         -> _p2pOnCost          : P2P
        //            _hasHsmBackend &&
        //            _stageOnCost        : STAGE
        //            else                : 127 , "Cost exceeded (st,p2p not allowed)"
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - No entry in configuration Permission Matrix
        //                   - Code Exception
        //
        //         (bestPool=0)
        //
        //         -> STAGE
        //
        //
        //
        //  askForPoolToPool(overwriteCost) :
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : source and destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_PERMITTED :
        //
        //         Because : - already too many copies (_maxPnfsFileCopies)
        //                   - file already everywhere (no destination found)
        //                   - SAME_HOST_NEVER : but no valid combination found
        //
        //         -> DONE 'using bestPool'
        //
        //      RequestStatusCode.S_COST_EXCEEDED (only if ! overwriteCost) :
        //
        //         Because : best source pool exceeds 'alert' cost.
        //
        //         -> _hasHsmBackend &&
        //            _stageOnCost    : STAGE
        //            bestPool == 0   : 194,"File not present in any reasonable pool"
        //            else            : DONE 'using bestPool'
        //
        //      RequestStatusCode.COST_EXCEEDED (only if ! overwriteCost)  :
        //
        //         Because : file is in permitted pools but cost of
        //                   best destination pool exceeds cost of best
        //                   source pool (resp. slope * source).
        //
        //         -> _bestPool == 0 : 192,"File not present in any reasonable pool"
        //            else           : DONE 'using bestPool'
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - no source pool (code problem)
        //                   - Code Exception
        //
        //         -> 132,"PANIC : Tried to do p2p, but source was empty"
        //                or exception text.
        //
        //  askForStaging :
        //
        //      RequestStatusCode.FOUND :
        //
        //         Because : destination pool found and cost ok.
        //
        //         -> DONE
        //
        //      RequestStatusCode.NOT_FOUND :
        //
        //         -> 149 , "No pool candidates available or configured for 'staging'"
        //         -> 150 , "No cheap candidates available for 'staging'"
        //
        //      RequestStatusCode.ERROR :
        //
        //         Because : - Code Exception
        //
        private void stateEngine(Object inputObject) {
            LOGGER.debug("stateEngine: for case {}", _state);

            switch (_state) {
            case ST_INIT:
                CacheException ce = _selections.get(_pnfsId);
                if (ce != null) {
                    failRequest(ce.getRc(), ce.getMessage());
                    return;
                }

                if (_suspendIncoming) {
                    suspend(1005, "Incoming request");
                    return;
                }

                if (_enforceP2P) {
                    nextStep(RequestState.ST_POOL_2_POOL);
                    return;
                }

                try {
                    try {
                        SelectedPool pool = selectReadPool();
                        success(pool);
                    } catch (CostException e) {
                        if (!e.shouldTryAlternatives()) {
                            throw e;
                        }

                        _parameter = _poolSelector.getCurrentPartition();
                        _hotPoolWithFile = e.getPool();
                        LOGGER.info("[read] {} ({})", e.getMessage(), _hotPoolWithFile);

                        if (_parameter._p2pOnCost) {
                            nextStep(RequestState.ST_POOL_2_POOL);
                        } else if (isFileStageable() &&  _parameter._stageOnCost) {
                            nextStep(RequestState.ST_STAGE);
                        } else {
                            failRequest(127, "Cost exceeded (st,p2p not allowed)");
                        }
                    }
                } catch (FileNotInCacheException e) {
                    LOGGER.info("[read] {}", e.getMessage());
                    if (isFileStageable()) {
                        LOGGER.debug(" stateEngine: parameter has HSM backend and the file is stored on tape ");
                        nextStep(RequestState.ST_STAGE);
                    } else {
                        LOGGER.debug(" stateEngine: case 1: parameter has NO HSM backend or case 2: the HSM backend exists but the file isn't stored on it.");
                        suspendIfEnabled(CacheException.POOL_UNAVAILABLE, "Pool unavailable");
                        // FIXME move this notification into the state change observer
                        if (_sendHitInfo) {
                            sendHitMsg(null, false);
                        }
                    }
                } catch (PermissionDeniedCacheException e) {
                    LOGGER.info("[read] {}", e.getMessage());
                    //
                    //  if we can't read the file because 'read is prohibited'
                    //  we at least must give dCache the chance to copy it
                    //  to another pool (not regarding the cost).
                    //
                    _overwriteCost = true;
                    //
                    //  if we don't have an hsm we overwrite the p2pAllowed
                    //
                    nextStep(_parameter._p2pAllowed || !isFileStageable()
                            ? RequestState.ST_POOL_2_POOL : RequestState.ST_STAGE);

                } catch (CacheException | IllegalArgumentException e) {
                    LOGGER.warn("Read pool selection failed: {}", e.getMessage());
                    nextStep(RequestState.ST_STAGE);
                }
                break;

            case ST_POOL_2_POOL:
                try {
                    askForPoolToPool(_overwriteCost);
                    nextStep(RequestState.ST_WAITING_FOR_POOL_2_POOL);
                    updateStatus("Waiting for pool-to-pool transfer: "
                            + _p2pSourcePool + " to " + _p2pDestinationPool);
                } catch (PermissionDeniedCacheException e) {
                    LOGGER.info("[p2p] {}", e.toString());
                    if (_hotPoolWithFile == null) {
                        if (_enforceP2P) {
                            failRequest(e.getRc(), e.getMessage());
                        } else if (isFileStageable()) {
                            LOGGER.info("ST_POOL_2_POOL : Pool to pool not permitted, trying to stage the file");
                            nextStep(RequestState.ST_STAGE);
                        } else {
                            suspendIfEnabled(265, "Pool to pool not permitted");
                        }
                    } else {
                        success(_hotPoolWithFile);
                        LOGGER.info("ST_POOL_2_POOL : Choosing high cost pool {}", _poolCandidate.info());
                    }
                } catch (SourceCostException e) {
                    LOGGER.info("[p2p] {}", e.getMessage());
                    if (isFileStageable() && _parameter._stageOnCost) {
                        if (_enforceP2P) {
                            failRequest(e.getRc(), e.getMessage());
                        } else {
                            LOGGER.info("ST_POOL_2_POOL : staging");
                            nextStep(RequestState.ST_STAGE);
                        }
                    } else {
                        if (_hotPoolWithFile != null) {
                            success(_hotPoolWithFile);
                            LOGGER.info("ST_POOL_2_POOL : Choosing high cost pool {}", _poolCandidate.info());
                        } else {
                            //
                            // this can't possibly happen
                            //
                            failRequest(194,"PANIC : File not present in any reasonable pool");
                        }
                    }
                } catch (DestinationCostException e) {
                    LOGGER.info("[p2p] {}", e.getMessage());
                    if (_hotPoolWithFile == null) {
                        //
                        // this can't possibly happen
                        //
                        if (!_enforceP2P) {
                            failRequest(192, "PANIC : File not present in any reasonable pool");
                        } else {
                            failRequest(e.getRc(), e.getMessage());
                        }
                    } else {
                        success(_hotPoolWithFile);
                        LOGGER.info(" found high cost object");
                    }
                } catch (CacheException | IllegalArgumentException e) {
                    int rc = e instanceof CacheException ?
                            ((CacheException) e).getRc() : 128;
                    LOGGER.warn("[p2p] {}", e.getMessage());
                    if (_enforceP2P) {
                        failRequest(rc, e.getMessage());
                    } else if (isFileStageable()) {
                        nextStep(RequestState.ST_STAGE);
                    } else {
                        // FIXME refactor askForPoolToPool to avoid
                        // side-effects to avoid this.
                        suspendIfEnabled(rc, e.getMessage());
                    }
                }
                break;

            case ST_STAGE:
                if (_suspendStaging) {
                    suspend(1005, "Would trigger stage");
                    return;
                }

                try {
                    SelectedPool pool = askForStaging();
                    LOGGER.info("[staging] selected pool {}", pool.info());
                    nextStep(RequestState.ST_WAITING_FOR_STAGING);
                    updateStatus("Waiting for stage: " + pool);
                } catch (CostException e) {
                    errorHandler(125, e.getMessage());
                } catch (MissingResourceCacheException e) {
                    _restoreExceeded++;
                    failRequest(5, "Failed to stage file: " + e.getMessage());
                    sendInfoMessage(5, "Failed to stage file: " + e.getMessage());
                } catch (CacheException | IllegalArgumentException e) {
                    LOGGER.warn("[stage] failed to stage file: {}", e.getMessage());
                    int rc = e instanceof CostException ? 125
                            : (e instanceof IllegalArgumentException ? 128
                            : ((CacheException)e).getRc());
                    errorHandler(rc, e.getMessage());
                }
                break;

            case ST_WAITING_FOR_POOL_2_POOL:
                if (inputObject instanceof Pool2PoolTransferMsg) {
                    Pool2PoolTransferMsg message = (Pool2PoolTransferMsg)inputObject;

                    if (message.getReturnCode() == 0) {
                        if (_parameter._p2pForTransfer && ! _enforceP2P) {
                            failRequest(CacheException.OUT_OF_DATE,
                                    "Pool locations changed due to p2p transfer");
                        } else {
                            success(_p2pDestinationPool);
                        }
                    } else {
                        LOGGER.info("ST_POOL_2_POOL : Pool to pool reported a problem");
                        if (isFileStageable()) {
                            LOGGER.info("ST_POOL_2_POOL : trying to stage the file");
                            nextStep(RequestState.ST_STAGE);
                        } else {
                            errorHandler(message);
                        }
                    }
                } else if (inputObject instanceof PingFailure) {
                    PingFailure message = (PingFailure)inputObject;
                    if (_p2pDestinationPool.address().equals(message.getPool())) {
                        LOGGER.info("Ping reported that request died.");
                        clearSteering();
                        errorHandler(CacheException.TIMEOUT, "Replication timed out");
                    }
                } else if (inputObject != null) {
                    LOGGER.error("Unexpected message type: {}. Possibly a bug.", inputObject.getClass());
                    clearSteering();
                    errorHandler(102, "Unexpected message type " + inputObject.getClass());
                }
                break;

            case ST_WAITING_FOR_STAGING:
                if (inputObject instanceof PoolFetchFileMessage) {
                    PoolFetchFileMessage message = (PoolFetchFileMessage)inputObject;

                    switch (message.getReturnCode()) {
                    case 0:
                        if (_parameter._p2pForTransfer) {
                            failRequest(CacheException.OUT_OF_DATE,
                                    "Pool locations changed due to stage");
                        } else {
                            success(_stageCandidate.orElseThrow(() -> new RuntimeException("Stage successful without candidate pool")));
                        }
                        break;

                    case CacheException.HSM_DELAY_ERROR:
                        suspend(message);
                        break;

                    default:
                        errorHandler(message);
                        break;
                    }
                } else if (inputObject instanceof PingFailure) {
                    PingFailure message = (PingFailure)inputObject;
                    CellAddressCore stagePoolAddress = _stageCandidate
                            .map(SelectedPool::address).orElse(null);
                    if (Objects.equals(stagePoolAddress, message.getPool())) {
                        LOGGER.info("Ping reported that request died.");
                        clearSteering();
                        errorHandler(CacheException.TIMEOUT, "Staging timed out");
                    }
                } else if (inputObject != null) {
                    LOGGER.error("Unexpected message type: {}. Possibly a bug.", inputObject.getClass());
                    clearSteering();
                    errorHandler(102, "Unexpected message type " + inputObject.getClass());
                }
                break;

            case ST_SUSPENDED:
                // Nothing to do.
                break;
            }
        }

        private void answerRequests()
        {
                //
                // it is essential that we are not within any other
                // lock when trying to get the handlerHash lock.
                //
                synchronized (_handlerHash) {
                    _handlerHash.remove(_name);
                }

                int limit = _currentRc == 0 ? MAX_REQUEST_CLUMPING
                        : Integer.MAX_VALUE;

                if (answerRequests(limit)) {
                    _currentRc = CacheException.OUT_OF_DATE;
                    _currentRm = "Request clumping limit reached";
                    answerRequests(Integer.MAX_VALUE);
                }
        }

        private void updateStatus(String newStatus)
        {
            _status = newStatus + " " + LocalDateTime.now().format(DATE_TIME_FORMAT);
        }

        private void suspend(Message reply)
        {
            String message = reply.getErrorObject() == null
                    ? "No info" : reply.getErrorObject().toString();
            suspend(reply.getReturnCode(), message);
        }

        private void suspend(int code, String reason)
        {
            String message = "Suspended: " + reason;
            _currentRc = code;
            _currentRm = message;

            LOGGER.debug(" stateEngine: {}", message);
            updateStatus(message);
            nextStep(RequestState.ST_SUSPENDED);
            sendInfoMessage(0, message);
        }

        private void suspendIfEnabled(int code, String message)
        {
            if (_onError.equals("suspend") && !_failOnExcluded) {
                suspend(code, message);
            } else {
                failRequest(code, message);
            }
        }

        private void errorHandler(Message reply)
        {
            String message = reply.getErrorObject() == null
                        ? ("Error=" + _currentRc) : reply.getErrorObject().toString();
            errorHandler(reply.getReturnCode(), message);
        }

        private void errorHandler(int code, String message)
        {
            if (_retryCounter >= _maxRetries) {
                suspendIfEnabled(code, message);
            } else {
                failRequest(code, message);
            }
        }

        private SelectedPool selectReadPool() throws CacheException
        {
            try {
                SelectedPool pool = _poolSelector.selectReadPool();
                _parameter = _poolSelector.getCurrentPartition();
                return pool;
            } finally {
                LOGGER.info("[read] Took  {} ms", (System.currentTimeMillis() - _started));
            }
        }

        private void askForPoolToPool(boolean overwriteCost) throws CacheException
        {
            try {
                Partition.P2pPair pools = _poolSelector.selectPool2Pool(_poolGroup,
                        overwriteCost);

                _p2pSourcePool = pools.source;
                _p2pDestinationPool = pools.destination;
                LOGGER.info("[p2p] source={};dest={}",
                        _p2pSourcePool, _p2pDestinationPool);
                sendPool2PoolRequest(_p2pSourcePool, _p2pDestinationPool);
            } finally {
                LOGGER.info("[p2p] Selection took {} ms", (System.currentTimeMillis() - _started));
            }
        }

        @Nonnull
        private SelectedPool askForStaging() throws CacheException
        {
            try {
                SelectedPool pool =  _poolSelector.selectStagePool(_stageCandidate.map(SelectedPool::info));
                _stageCandidate = Optional.of(pool);
                sendFetchRequest(pool);
                return pool;
            } finally {
                LOGGER.info("[stage] Selection took {} ms", (System.currentTimeMillis() - _started));
            }
        }

        private void sendInfoMessage(int rc, String infoMessage)
        {
            WarningPnfsFileInfoMessage info = new WarningPnfsFileInfoMessage("PoolManager",
                    getCellAddress(), _pnfsId, rc, infoMessage);
            info.setStorageInfo(_fileAttributes.getStorageInfo());
            info.setFileSize(_fileAttributes.getSize());
            info.setBillingPath(_billingPath);
            info.setTransferPath(_transferPath);
            _billing.notify(info);
        }

        private void sendHitMsg(PoolInfo pool, boolean cached)
        {
            PoolHitInfoMessage msg = new PoolHitInfoMessage(pool == null ? null : pool.getAddress(),
                    _pnfsId);
            msg.setBillingPath(_billingPath);
            msg.setTransferPath(_transferPath);
            msg.setFileCached(cached);
            msg.setStorageInfo(_fileAttributes.getStorageInfo());
            msg.setFileSize(_fileAttributes.getSize());
            msg.setProtocolInfo(_protocolInfo);
            _billing.notify(msg);
        }
    }

    public void setStageConfigurationFile(String path)
    {
        _stagePolicyDecisionPoint = new CheckStagePermission(path);
        _stagePolicyDecisionPoint.setAllowAnonymousStaging(_allowAnonymousStaging);
    }

    public void setAllowAnonymousStaging(boolean isAllowed)
    {
        _allowAnonymousStaging = isAllowed;
        if (_stagePolicyDecisionPoint != null) {
            _stagePolicyDecisionPoint.setAllowAnonymousStaging(isAllowed);
        }
    }

    private class PoolPingThread extends Thread
    {
        private volatile boolean oneShot;

        private PoolPingThread()
        {
            super("Container-ping");
        }

        public synchronized void checkNow() throws InterruptedException
        {
            oneShot = true;
            notify();
            do {
                wait();
            } while (oneShot);
        }

        public void run()
        {
            try {
                while (!Thread.interrupted()) {
                    try {
                        synchronized (this) {
                            if (!oneShot) {
                                wait(_checkFilePingTimer);
                            }
                        }

                        long now = System.currentTimeMillis();

                        // Determine which pools to query
                        List<PoolRequestHandler> list;
                        synchronized (_handlerHash) {
                            list = new ArrayList<>(_handlerHash.values());
                        }
                        Multimap<CellAddressCore, PoolRequestHandler> p2pRequests = ArrayListMultimap.create();
                        Multimap<CellAddressCore, PoolRequestHandler> stageRequests = ArrayListMultimap.create();
                        for (PoolRequestHandler handler : list) {
                            if (oneShot || handler._started < now - _checkFilePingTimer) {
                                SelectedPool pool;
                                switch (handler._state) {
                                case ST_WAITING_FOR_POOL_2_POOL:
                                    pool = handler._p2pDestinationPool;
                                    if (pool != null) {
                                        p2pRequests.put(pool.address(), handler);
                                    }
                                    break;

                                case ST_WAITING_FOR_STAGING:
                                    handler._stageCandidate.ifPresent(p ->
                                            stageRequests.put(p.address(), handler));
                                    break;
                                }
                            }
                        }

                        // Send query to all pools
                        Map<CellAddressCore, ListenableFuture<String>> futures = new HashMap<>();
                        for (CellAddressCore pool : p2pRequests.keySet()) {
                            futures.put(pool, _poolStub.send(new CellPath(pool), "pp ls", String.class));
                        }
                        for (CellAddressCore pool : stageRequests.keySet()) {
                            futures.put(pool, _poolStub.send(new CellPath(pool), "rh ls", String.class));
                        }

                        // Collect replies
                        for (Map.Entry<CellAddressCore, ListenableFuture<String>> entry : futures.entrySet()) {
                            String reply;
                            try {
                                reply = CellStub.get(entry.getValue());
                            } catch (NoRouteToCellException | CacheException ignored) {
                                reply = "";
                            }

                            CellAddressCore address = entry.getKey();
                            for (PoolRequestHandler handler : p2pRequests.get(address)) {
                                if (!reply.contains(handler._pnfsId.toString())) {
                                    handler.add(new PingFailure(address));
                                }
                            }
                            for (PoolRequestHandler handler : stageRequests.get(address)) {
                                if (!reply.contains(handler._pnfsId.toString())) {
                                    handler.add(new PingFailure(address));
                                }
                            }
                        }
                    } catch (RuntimeException e) {
                        LOGGER.error("Pool ping failed", e);
                    }
                    synchronized (this) {
                        oneShot = false;
                        notify();
                    }

                }
            } catch (InterruptedException ignored) {
            }
        }
    }

    private static class PingFailure
    {
        private final CellAddressCore pool;

        private PingFailure(CellAddressCore pool)
        {
            this.pool = pool;
        }

        public CellAddressCore getPool()
        {
            return pool;
        }
    }
}
