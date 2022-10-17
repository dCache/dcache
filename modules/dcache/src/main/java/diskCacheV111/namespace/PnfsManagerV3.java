package diskCacheV111.namespace;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.dcache.acl.enums.AccessType.ACCESS_ALLOWED;
import static org.dcache.acl.enums.AccessType.ACCESS_DENIED;
import static org.dcache.acl.enums.AccessType.ACCESS_UNDEFINED;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.auth.attributes.Activity.DELETE;
import static org.dcache.auth.attributes.Activity.DOWNLOAD;
import static org.dcache.auth.attributes.Activity.LIST;
import static org.dcache.auth.attributes.Activity.MANAGE;
import static org.dcache.auth.attributes.Activity.READ_METADATA;
import static org.dcache.auth.attributes.Activity.UPDATE_METADATA;
import static org.dcache.auth.attributes.Activity.UPLOAD;
import static org.dcache.namespace.FileAttribute.ACCESS_TIME;
import static org.dcache.namespace.FileAttribute.CHANGE_TIME;
import static org.dcache.namespace.FileAttribute.CREATION_TIME;
import static org.dcache.namespace.FileAttribute.MODE;
import static org.dcache.namespace.FileAttribute.MODIFICATION_TIME;
import static org.dcache.namespace.FileAttribute.NLINK;
import static org.dcache.namespace.FileAttribute.OWNER;
import static org.dcache.namespace.FileAttribute.OWNER_GROUP;
import static org.dcache.namespace.FileAttribute.PNFSID;
import static org.dcache.namespace.FileAttribute.SIZE;
import static org.dcache.namespace.FileAttribute.TYPE;
import static org.dcache.namespace.FileAttribute.XATTR;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.DoorCancelledUploadNotificationMessage;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsListExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsReadExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsRemoveExtendedAttributesMessage;
import diskCacheV111.vehicles.PnfsRemoveLabelsMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.PnfsWriteExtendedAttributesMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;
import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.Option;
import java.io.File;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.security.auth.Subject;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.Restriction;
import org.dcache.cells.CellStub;
import org.dcache.chimera.UnixPermission;
import org.dcache.chimera.quota.JdbcQuota;
import org.dcache.chimera.quota.Quota;
import org.dcache.chimera.quota.QuotaHandler;
import org.dcache.commons.stats.RequestCounters;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.quota.data.QuotaInfo;
import org.dcache.quota.data.QuotaRequest;
import org.dcache.quota.data.QuotaType;
import org.dcache.util.Args;
import org.dcache.util.ByteUnit;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.ColumnWriter;
import org.dcache.util.ColumnWriter.TabulatedRow;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.TimeUtils;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsCreateSymLinkMessage;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.vehicles.PnfsRemoveChecksumMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.dcache.vehicles.quota.PnfsManagerGetQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerRemoveQuotaMessage;
import org.dcache.vehicles.quota.PnfsManagerSetQuotaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.Transactional;

public class PnfsManagerV3
      extends AbstractCellComponent
      implements CellCommandListener, CellMessageReceiver, CellInfoProvider, LeaderLatchListener {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(PnfsManagerV3.class);

    private static final int THRESHOLD_DISABLED = 0;

    private static final CellMessage SHUTDOWN_SENTINEL = new CellMessage();

    private static final String STORAGE_INFO_XATTR_PREFIX = "xattr.";

    private final Random _random = new Random(System.currentTimeMillis());

    private final RequestExecutionTimeGauges<Class<? extends PnfsMessage>> _gauges =
          new RequestExecutionTimeGauges<>("PnfsManagerV3");
    private final RequestCounters<Class<?>> _foldedCounters =
          new RequestCounters<>("PnfsManagerV3.Folded");

    /**
     * These messages are subject to being discarded if their time to live has been exceeded (or is
     * expected to be exceeded).
     */
    private final Class<?>[] DISCARD_EARLY = {
          PnfsGetCacheLocationsMessage.class,
          PnfsMapPathMessage.class,
          PnfsGetParentMessage.class,
          PnfsCreateEntryMessage.class,
          PnfsCreateUploadPath.class,
          PnfsGetFileAttributes.class,
          PnfsListDirectoryMessage.class
    };

    private int _threads;
    private int _directoryListLimit;
    private int _queueMaxSize;
    private int _listThreads;
    private long _logSlowThreshold;

    private ScheduledFuture<?> updateFsFuture;
    private ScheduledExecutorService scheduledExecutor;
    private TimeUnit updateFsStatIntervalUnit;
    private long updateFsStatInterval;

    private ScheduledFuture<?> updateGroupQuotaFuture;
    private ScheduledFuture<?> updateUserQuotaFuture;
    private TimeUnit updateQuotaIntervalUnit;
    private long updateQuotaInterval;
    private boolean quotaEnabled;

    private boolean useParentHashOnCreate;


    /**
     * Whether to use folding.
     */
    private boolean _canFold;

    /**
     * Queues for list operations. There is one queue per thread group.
     */
    private BlockingQueue<CellMessage> _listQueue;

    /**
     * Tasks queues used for messages that do not operate on cache locations.
     */
    private BlockingQueue<CellMessage>[] _fifos;

    /**
     * Executor for ProcessThread instances.
     */
    private final ExecutorService executor =
          Executors.newCachedThreadPool(
                new ThreadFactoryBuilder().setNameFormat("proc-%d").build());

    private CellPath _cacheModificationRelay;

    /**
     * These involve changes to access latency and retention policy.
     */
    private CellPath _attributesRelay;

    private PermissionHandler _permissionHandler;
    private NameSpaceProvider _nameSpaceProvider;

    /**
     * A value of difference in seconds which controls file's access time updates.
     */
    private long _atimeGap;

    private CellStub _stub;

    private List<String> _flushNotificationTargets;
    private List<String> _cancelUploadNotificationTargets = Collections.emptyList();

    private List<ProcessThread> _listProcessThreads = new ArrayList<>();

    private JdbcQuota quotaSystem;

    private void populateRequestMap() {
        _gauges.addGauge(PnfsAddCacheLocationMessage.class);
        _gauges.addGauge(PnfsClearCacheLocationMessage.class);
        _gauges.addGauge(PnfsGetCacheLocationsMessage.class);
        _gauges.addGauge(PnfsCreateEntryMessage.class);
        _gauges.addGauge(PnfsDeleteEntryMessage.class);
        _gauges.addGauge(PnfsMapPathMessage.class);
        _gauges.addGauge(PnfsRenameMessage.class);
        _gauges.addGauge(PnfsFlagMessage.class);
        _gauges.addGauge(PoolFileFlushedMessage.class);
        _gauges.addGauge(PnfsGetParentMessage.class);
        _gauges.addGauge(PnfsSetFileAttributes.class);
        _gauges.addGauge(PnfsGetFileAttributes.class);
        _gauges.addGauge(PnfsListDirectoryMessage.class);
        _gauges.addGauge(PnfsRemoveChecksumMessage.class);
        _gauges.addGauge(PnfsCreateSymLinkMessage.class);
        _gauges.addGauge(PnfsCreateUploadPath.class);
        _gauges.addGauge(PnfsCommitUpload.class);
        _gauges.addGauge(PnfsCancelUpload.class);
        _gauges.addGauge(PnfsListExtendedAttributesMessage.class);
        _gauges.addGauge(PnfsReadExtendedAttributesMessage.class);
        _gauges.addGauge(PnfsWriteExtendedAttributesMessage.class);
        _gauges.addGauge(PnfsRemoveExtendedAttributesMessage.class);
        _gauges.addGauge(PnfsRemoveLabelsMessage.class);
    }

    public PnfsManagerV3() {
        populateRequestMap();
    }

    @Required
    public void setUpdateFsStatInterval(long updateFsStatInterval) {
        this.updateFsStatInterval = updateFsStatInterval;
    }

    @Required
    public void setUpdateFsStatIntervalUnit(TimeUnit updateFsStatIntervalUnit) {
        this.updateFsStatIntervalUnit = updateFsStatIntervalUnit;
    }

    @Required
    public void setUpdateQuotaInterval(long updateQuotaInterval) {
        this.updateQuotaInterval = updateQuotaInterval;
    }

    @Required
    public void setUpdateQuotaIntervalUnit(TimeUnit updateQuotaIntervalUnit) {
        this.updateQuotaIntervalUnit = updateQuotaIntervalUnit;
    }

    @Required
    public void setQuotaEnabled(boolean quotaEnabled) {
        this.quotaEnabled = quotaEnabled;
    }

    public void setUseParentHashOnCreate(boolean useParentHashOnCreate) {
        this.useParentHashOnCreate = useParentHashOnCreate;
    }

    @Required
    public void setScheduledExecutor(ScheduledExecutorService executor) {
        scheduledExecutor = executor;
    }

    @Required
    public void setThreads(int threads) {
        _threads = threads;
    }

    @Required
    public void setListThreads(int threads) {
        _listThreads = threads;
    }

    @Required
    public void setCacheModificationRelay(String path) {
        _cacheModificationRelay =
              Strings.isNullOrEmpty(path) ? null : new CellPath(path);
        LOGGER.info("CacheModificationRelay = {}",
              (_cacheModificationRelay == null) ? "NONE" : _cacheModificationRelay.toString());
    }

    @Required
    public void setFileAttributesRelay(String path) {
        _attributesRelay =
              Strings.isNullOrEmpty(path) ? null : new CellPath(path);
        LOGGER.info("attributesRelay = {}",
              (_attributesRelay == null) ? "NONE" : _attributesRelay.toString());
    }

    @Required
    public void setLogSlowThreshold(long threshold) {
        _logSlowThreshold = threshold;
        LOGGER.info("logSlowThreshold {}",
              (_logSlowThreshold == THRESHOLD_DISABLED) ? "NONE"
                    : String.valueOf(_logSlowThreshold));
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler) {
        _permissionHandler = handler;
    }

    @Required
    public void setNameSpaceProvider(NameSpaceProvider provider) {
        _nameSpaceProvider = provider;
    }

    public NameSpaceProvider getNameSpaceProvider() {
        return _nameSpaceProvider;
    }

    @Required
    public void setQueueMaxSize(int maxSize) {
        _queueMaxSize = maxSize;
    }

    @Required
    public void setFolding(boolean folding) {
        _canFold = folding;
    }

    @Required
    public void setDirectoryListLimit(int limit) {
        _directoryListLimit = limit;
    }

    @Required
    public void setAtimeGap(long gap) {
        if (gap < 0) {
            _atimeGap = -1;
        } else {
            _atimeGap = TimeUnit.SECONDS.toMillis(gap);
        }
    }

    @Required
    public void setFlushNotificationTarget(String target) {
        _flushNotificationTargets = Splitter.on(",").omitEmptyStrings().splitToList(target);
    }

    @Required
    public void setCancelUploadNotificationTarget(String target) {
        _cancelUploadNotificationTargets = Splitter.on(',').omitEmptyStrings().splitToList(target);
    }

    @Required
    public void setQuotaSystem(JdbcQuota quota) {
        quotaSystem = quota;
    }

    @Required
    public QuotaHandler getQuotaSystem() {
        return quotaSystem;
    }

    public void init() {
        _stub = new CellStub(getCellEndpoint());

        _fifos = new BlockingQueue[_threads];
        LOGGER.info("Starting {} threads", _fifos.length);
        for (int i = 0; i < _fifos.length; i++) {
            if (_queueMaxSize > 0) {
                _fifos[i] = new LinkedBlockingQueue<>(_queueMaxSize);
            } else {
                _fifos[i] = new LinkedBlockingQueue<>();
            }
            executor.execute(new ProcessThread(_fifos[i]));
        }

        /* Start a seperate queue for list operations.  We use a shared queue,
         * as list operations are read only and thus there is no need
         * to serialize the operations.
         */
        _listQueue = new LinkedBlockingQueue<>();
        for (int j = 0; j < _listThreads; j++) {
            ProcessThread t = new ProcessThread(_listQueue);
            _listProcessThreads.add(t);
            executor.execute(t);
        }
    }

    public void shutdown() throws InterruptedException {
        drainQueues(_fifos);
        drainQueue(_listQueue);
        MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    private void drainQueues(BlockingQueue<CellMessage>[] queues) {
        Arrays.stream(queues).forEach(this::drainQueue);
    }

    private void drainQueue(BlockingQueue<CellMessage> queue) {
        String error = "Name space is shutting down.";
        ArrayList<CellMessage> drained = new ArrayList<>();
        queue.drainTo(drained);
        for (CellMessage envelope : drained) {
            Message msg = (Message) envelope.getMessageObject();
            if (msg.getReplyRequired()) {
                envelope.setMessageObject(new NoRouteToCellException(envelope, error));
                envelope.revertDirection();
                sendMessage(envelope);
            }
        }
        queue.offer(SHUTDOWN_SENTINEL);
    }

    @Override
    public void isLeader() {
        updateFsFuture = scheduledExecutor.
              scheduleWithFixedDelay(
                    new FireAndForgetTask(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                updateFsStat();
                            } catch (CacheException ignore) {
                                LOGGER.error("Failed to tun updateFsStat: {}", ignore.getMessage());
                            }
                        }
                    }),
                    100000,
                    updateFsStatIntervalUnit.toMillis(updateFsStatInterval),
                    TimeUnit.MILLISECONDS);

        if (quotaEnabled) {
            updateGroupQuotaFuture = scheduledExecutor.
                  scheduleWithFixedDelay(
                        new FireAndForgetTask(new Runnable() {
                            @Override
                            public void run() {
                                quotaSystem.updateGroupQuotas();
                            }
                        }),
                        600000,
                        updateQuotaIntervalUnit.toMillis(updateQuotaInterval),
                        TimeUnit.MILLISECONDS);

            updateUserQuotaFuture = scheduledExecutor.
                  scheduleWithFixedDelay(
                        new FireAndForgetTask(new Runnable() {
                            @Override
                            public void run() {
                                quotaSystem.updateUserQuotas();
                            }
                        }),
                        600000,
                        updateQuotaIntervalUnit.toMillis(updateQuotaInterval),
                        TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void notLeader() {
        updateFsFuture.cancel(true);
        if (quotaEnabled) {
            updateGroupQuotaFuture.cancel(true);
            updateUserQuotaFuture.cancel(true);
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.print("atime precision: ");
        if (_atimeGap < 0) {
            pw.println("Disabled");
        } else {
            pw.println(TimeUnit.MILLISECONDS.toSeconds(_atimeGap));
        }
        pw.println();
        pw.println("List queue: " + _listQueue.size());
        pw.println();
        pw.println("Threads (" + _fifos.length + ") Queue");
        for (int i = 0; i < _fifos.length; i++) {
            pw.println("    [" + i + "] " + _fifos[i].size());
        }
        pw.println();
        pw.println("Threads: "
              + Arrays.stream(_fifos).mapToInt(BlockingQueue::size).sum());
        pw.println();

        pw.println("Statistics:");
        pw.println(_gauges.toString());
        pw.println(_foldedCounters.toString());
    }

    @Command(name = "reset stats", hint="reset statistics",
            description = "Reset the counters and gauge statistics describing PnfsManager.  These"
                    + " statistics are shown as part of the 'info' command output.")
    public class ResetStatsCommand implements Callable<String> {

        @Option(name="target", usage="Which statistics to reset:\n"
                + "\n"
                + "\"calls\" is the cell message call gauges, labelled 'PnfsManagerV3'.\n"
                + "\n"
                + "\"folds\" is the message folding counts, labelled 'PnfsManagerV3.Folded'.\n"
                + "\n"
                + "\"all\" resets everything.\n"
                + "\n"
                + "If this option is not specified then \"all\" is assumed.",
                values={"calls", "folds", "all"})
        private String target;

        @Override
        public String call() throws CommandException {
            if (target == null) {
                target = "all";
            }
            switch (target) {
            case "all":
                _gauges.reset();
                _foldedCounters.reset();
                break;
            case "calls":
                _gauges.reset();
                break;
            case "folds":
                _foldedCounters.reset();
                break;
            default:
                throw new CommandException("Unknown target \"" + target + "\".");
            }
            return "";
        }
    }

    @Command(name = "pnfsidof",
          hint = "find the Pnfs-Id of a file",
          description = "Print the Pnfs-Id of a file given by its absolute path.")
    public class PnfsidofCommand implements Callable<String> {

        @Argument(usage = "The absolute path of the file.")
        String path;

        @Override
        public String call() throws CacheException {
            return _nameSpaceProvider.pathToPnfsid(ROOT, path, false).toString();
        }

    }

    public static final String hh_cacheinfoof = "<pnfsid>|<globalPath>";

    public String ac_cacheinfoof_$_1(Args args) {
        PnfsId pnfsId;
        StringBuilder sb = new StringBuilder();
        try {
            try {

                pnfsId = new PnfsId(args.argv(0));

            } catch (Exception ee) {
                pnfsId = _nameSpaceProvider.pathToPnfsid(ROOT, args.argv(0), true);
            }

            List<String> locations = _nameSpaceProvider.getCacheLocation(ROOT, pnfsId);

            for (String location : locations) {
                sb.append(" ").append(location);
            }

            sb.append("\n");
        } catch (Exception e) {
            sb.append("cacheinfoof failed: ").append(e.getMessage());
        }
        return sb.toString();
    }

    @Command(name = "pathfinder",
          hint = "find the path of a file",
          description = "Print the absolute path of the specified Pnfs-Id of a file.")
    public class PathfinderCommand implements Callable<String> {

        @Argument(usage = "The Pnfs-Id of the file.")
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException {
            return _nameSpaceProvider.pnfsidToPath(ROOT, pnfsId);
        }
    }

    @Command(name = "set meta",
          hint = "set the meta-data of a file",
          description = "Set the meta-data including: new owner, group, and permissions. " +
                "Returns 'OK' if the meta-data has been set successfully.")
    public class SetMetaCommand implements Callable<String> {

        @Argument(index = 0,
              valueSpec = "PNFSID|PATH",
              usage = "Pnfs-Id or absolute path of the file.")
        String pnfsidOrPath;

        @Argument(index = 1,
              usage = "The user id of the new owner of the file.")
        int uid;

        @Argument(index = 2,
              usage = "The new group id of the file.")
        int gid;

        @Argument(index = 3,
              usage = "The file access permissions mode." +
                    "Only in decimal mode for example: set meta <pnfsid> <uid> <gid> 644(rw-r--r--).")
        String perm;

        @Override
        public String call() throws CacheException {

            PnfsId pnfsId;
            if (PnfsId.isValid(pnfsidOrPath)) {
                pnfsId = new PnfsId(pnfsidOrPath);
            } else {
                pnfsId = _nameSpaceProvider.pathToPnfsid(ROOT, pnfsidOrPath, true);
            }

            FileAttributes attributes = FileAttributes.of()
                  .uid(uid)
                  .gid(gid)
                  .mode(Integer.parseInt(perm, 8))
                  .build();

            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                  EnumSet.noneOf(FileAttribute.class));

            return "Ok";
        }
    }

    @Command(name = "storageinfoof",
          hint = "Print the storage info of a file",
          description = "Display the storage information of a file including the following:\n" +
                "\t size:   \tSize of the file in bytes\n" +
                "\t new:    \tFalse if file already in the dCache\n" +
                "\t stored: \tTrue if file already stored in the HSM (Hierarchical Storage\n" +
                "\t         \tManager)\n" +
                "\t sClass: \tHSM depended. Used by the PoolManager for pool attraction\n" +
                "\t hsm:    \tStorage Manager name (enstore/osm)\n" +
                "\t         \tCan be overwritten by parent directory tag (hsmType)\n\n" +
                "\t accessLatency: \tfile's access latency (e.g. ONLINE/NEARLINE)\n" +
                "\t retentionPolicy: \tfile's retention policy (e.g. CUSTODIAL/REPLICA).\n\n" +
                "The next returned information is HSM specific. For OSM the following storage information"
                +
                " is displayed:\n" +
                "\t store: \tOSM store (e.g. zeus,h1, ...)\n" +
                "\t group: \tOSM Storage Group (e.g. h1raw99, ...)\n" +
                "\t bfid:  \tBitfile Id (GET only) (e.g. 000451243.2542452542.25424524).\n\n" +
                "For Enstore the following information is returned:\n" +
                "\t group: \tStorage Group (e.g. cdf,cms ...)\n" +
                "\t family: \tFile family (e.g. sgi2test,h6nxl8, ...)\n" +
                "\t bfid:   \tBitfile Id (GET only) (e.g. B0MS105746894100000)\n" +
                "\t volume: \tTape Volume (GET only) (e.g. IA6912)\n" +
                "\t location: \tLocation on tape (GET only)\n" +
                "\t           \t(e.g. : 0000_000000000_0000117).")

    public class StorageinfoofCommand implements Callable<String> {

        @Argument(valueSpec = "PNFSID|PATH",
              usage = "The Pnfs-Id or the absolute path of the file.")
        String pnfsidOrPath;

        @Option(name = "v",
              usage = "Get additional information about the file. If file path" +
                    " is specified, the return information contains: the path " +
                    "of the file, Pnfs-Id, path and the storage info of the file. " +
                    "If the Pnfs-Id is specified instead, the return info is just" +
                    " the Pnfs-Id and the storage info of the file.")
        boolean verbose;

        @Option(name = "n",
              usage = "Don't resolve links.")
        boolean noLinks;

        @Override
        public String call() throws CacheException {
            PnfsId pnfsId;
            StringBuilder sb = new StringBuilder();

            if (PnfsId.isValid(pnfsidOrPath)) {
                pnfsId = new PnfsId(pnfsidOrPath);
                if (verbose) {
                    sb.append("PnfsId : ").append(pnfsId).append("\n");
                }
            } else {
                pnfsId = _nameSpaceProvider.pathToPnfsid(ROOT, pnfsidOrPath, noLinks);
                if (verbose) {
                    sb.append("         Path : ").append(pnfsidOrPath)
                          .append("\n");
                    sb.append("       PnfsId : ").append(pnfsId).append("\n");
                    sb.append("    Meta Data : ");
                }

            }

            FileAttributes attributes =
                  _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                        EnumSet.of(FileAttribute.STORAGEINFO, FileAttribute.ACCESS_LATENCY,
                              FileAttribute.RETENTION_POLICY, FileAttribute.SIZE));

            StorageInfo info = StorageInfos.extractFrom(attributes);
            if (verbose) {
                sb.append(" Storage Info : ").append(info).append("\n");
            } else {
                sb.append(info).append("\n");
            }

            return sb.toString();
        }
    }

    @Command(name = "metadataof",
          hint = "get the meta-data of a file",
          description = "Print the metadata of a file. This metadata contains the " +
                "following information:\n" +
                "\t\tThe owner id to whom file belongs.\n" +
                "\t\tThe group id to which file belongs.\n" +
                "\t\tc : creation time.\n" +
                "\t\tm : last modification time.\n" +
                "\t\ta : access time.")
    public class MetadataofCommand implements Callable<String> {

        @Argument(valueSpec = "PNFSID|PATH",
              usage = "Pnfs-Id or absolute path of the file.")
        String pnfsidOrPath;

        @Option(name = "v", usage =
              "Get additional information about the file. If file path is specified, " +
                    "the return information contains: the path of the file, Pnfs-Id and the metadata of the file. "
                    +
                    "If the Pnfs-Id is specified instead, the return info is just the Pnfs-Id and the metadata "
                    +
                    "of the file.")
        boolean verbose;

        @Option(name = "n",
              usage = "Don't resolve links.")
        boolean noLinks;


        @Override
        public String call() throws CacheException {
            PnfsId pnfsId;
            StringBuilder sb = new StringBuilder();

            if (PnfsId.isValid(pnfsidOrPath)) {
                pnfsId = new PnfsId(pnfsidOrPath);
                if (verbose) {
                    sb.append("PnfsId : ").append(pnfsId).append("\n");
                }
            } else {
                pnfsId = _nameSpaceProvider.pathToPnfsid(ROOT, pnfsidOrPath, noLinks);
                if (verbose) {
                    sb.append("         Path : ").append(pnfsidOrPath)
                          .append("\n");
                    sb.append("       PnfsId : ").append(pnfsId).append("\n");
                    sb.append("    Meta Data : ");
                }

            }

            FileAttributes fileAttributes = _nameSpaceProvider
                  .getFileAttributes(ROOT, pnfsId, EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE,
                        CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME));

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            switch (fileAttributes.getFileType()) {
                case DIR:
                    sb.append("d");
                    break;
                case LINK:
                    sb.append("l");
                    break;
                case REGULAR:
                    sb.append("-");
                    break;
                default:
                    sb.append("x");
                    break;
            }
            sb.append(new UnixPermission(fileAttributes.getMode()).toString().substring(1));
            sb.append(";").append(fileAttributes.getOwner());
            sb.append(";").append(fileAttributes.getGroup());
            sb.append("[c=").append(formatter.format(fileAttributes.getCreationTime()));
            sb.append(";m=").append(formatter.format(fileAttributes.getModificationTime()));
            sb.append(";a=").append(formatter.format(fileAttributes.getAccessTime())).append("]");
            sb.append("\n");
            return sb.toString();
        }

    }

    @Command(name = "flags set", hint = "set flags",
          description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                "flags. This command allows one or more flags to be set on a file.")
    class FlagsSetCommand implements Callable<String> {

        @Argument(valueSpec = "PATH|PNFSID")
        PnfsIdOrPath file;

        @CommandLine(allowAnyOption = true, usage = "Flags to modify.")
        Args args;

        @Override
        public String call() throws CacheException {
            _nameSpaceProvider.setFileAttributes(ROOT, file.toPnfsId(_nameSpaceProvider),
                  FileAttributes.ofFlags(args.optionsAsMap()), EnumSet.noneOf(FileAttribute.class));
            return "";
        }
    }

    @Command(name = "flags remove", hint = "clear flags",
          description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                "flags. This command allows one or more flags to be cleared on a file.")
    class FlagsRemoveCommand implements Callable<String> {

        @Argument(valueSpec = "PATH|PNFSID")
        PnfsIdOrPath file;

        @CommandLine(allowAnyOption = true, valueSpec = "-KEY ...", usage = "Flags to clear.")
        Args args;

        @Override
        public String call() throws CacheException {
            PnfsId pnfsId = file.toPnfsId(_nameSpaceProvider);

            for (String flag : args.options().keySet()) {
                _nameSpaceProvider.removeFileAttribute(ROOT, pnfsId, flag);
            }
            return "";
        }
    }

    @Command(name = "flags ls", hint = "list flags",
          description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                "flags. This command lists the flags of a file.")
    class FlagsListCommand implements Callable<String> {

        @Argument(valueSpec = "PATH|PNFSID")
        PnfsIdOrPath file;

        @Override
        public String call() throws CacheException {
            FileAttributes attributes =
                  _nameSpaceProvider.getFileAttributes(ROOT, file.toPnfsId(_nameSpaceProvider),
                        EnumSet.of(FileAttribute.FLAGS));
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> e : attributes.getFlags().entrySet()) {
                sb.append("-").append(e.getKey()).append("=").append(e.getValue()).append("\n");
            }
            return sb.toString();
        }
    }

    public static final String fh_dumpthreadqueues = "   dumpthreadqueues [<threadId>]\n"
          + "        dumthreadqueus prints the context of\n"
          + "        thread[s] queue[s] into the error log file";

    public static final String hh_dumpthreadqueues = "[<threadId>]";

    public String ac_dumpthreadqueues_$_0_1(Args args) {
        if (args.argc() > 0) {
            int threadId = Integer.parseInt(args.argv(0));
            dumpThreadQueue(threadId);
            return "dumped";
        }
        for (int threadId = 0; threadId < _fifos.length; ++threadId) {
            dumpThreadQueue(threadId);
        }
        return "dumped";
    }

    @Command(name = "set file size",
          hint = "changes registered file size",
          description = "Updates the file's size in the namespace. This command has no effect on\n"
                +
                "the data stored on pools or on tape.")
    public class SetFileSizeCommand implements Callable<String> {

        @Argument(index = 0,
              usage = "The unique identifier of the file within dCache.")
        PnfsId pnfsId;

        @Argument(index = 1,
              usage = "If the value does not match the size of the stored data" +
                    "then the file may become unavailable. Use with caution!")
        String newsize;

        @Override
        public String call() throws CacheException {
            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId,
                  FileAttributes.ofSize(Long.parseLong(newsize)),
                  EnumSet.noneOf(FileAttribute.class));

            return "";
        }
    }

    @Command(name = "show group quota",
          hint = "Print group quota",
          description = "Display group quota")

    public class ShowGroupQuotaCommand implements Callable<String> {

        @Option(name = "gid",
              usage = "Get group quota info by GID")
        String gid;

        @Option(name = "h",
              usage = "Use unit suffixes Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and " +
                    "Petabyte in order to reduce the number of digits to three or less " +
                    "using base 10 for sizes.")
        boolean humanReadable;

        @Override
        public String call() throws CacheException {
            if (!quotaEnabled) {
                return "Quota is disabled.";
            }
            quotaSystem.refreshGroupQuotas();
            Optional<ByteUnit> displayUnit = humanReadable
                  ? Optional.empty()
                  : Optional.of(ByteUnit.BYTES);

            ColumnWriter table = new ColumnWriter()
                  .header("gid").right("gid").space()
                  .header("CUSTODIAL").bytes("custodial", displayUnit, ByteUnit.Type.DECIMAL)
                  .space()
                  .header("limit")
                  .bytes("custodial_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE").space()
                  .header("REPLICA").bytes("replica", displayUnit, ByteUnit.Type.DECIMAL).space()
                  .header("limit")
                  .bytes("replica_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE").space()
                  .header("OUTPUT").bytes("output", displayUnit, ByteUnit.Type.DECIMAL).space()
                  .header("limit").bytes("output_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE")
                  .space();
            Map<Integer, Quota> quotas = quotaSystem.getGroupQuotas();
            if (gid != null) {
                int igid = Integer.parseInt(gid);
                Quota quota = quotas.get(igid);
                if (quota == null) {
                    throw new CacheException("No group found");
                }
                fillRow(table, quota);
            } else {
                SortedSet<Integer> keys = new TreeSet<>(quotas.keySet());
                for (Integer key : keys) {
                    Quota quota = quotas.get(key);
                    fillRow(table, quota);
                }
            }
            return table.toString();
        }

        private void fillRow(ColumnWriter table, Quota quota) {
            table.row()
                  .value("gid", quota.getId())
                  .value("custodial", quota.getUsedCustodialSpace())
                  .value("custodial_limit", quota.getCustodialSpaceLimit())
                  .value("replica", quota.getUsedReplicaSpace())
                  .value("replica_limit", quota.getReplicaSpaceLimit())
                  .value("output", quota.getUsedOutputSpace())
                  .value("output_limit", quota.getOutputSpaceLimit());
        }

    }

    @Command(name = "show user quota",
          hint = "Print user quota",
          description = "Display user quota")

    public class ShowUserQuotaCommand implements Callable<String> {

        @Option(name = "uid",
              usage = "Get user quota info by UID")
        String uid;

        @Option(name = "h",
              usage = "Use unit suffixes Byte, Kilobyte, Megabyte, Gigabyte, Terabyte and " +
                    "Petabyte in order to reduce the number of digits to three or less " +
                    "using base 10 for sizes.")
        boolean humanReadable;

        @Override
        public String call() throws CacheException {
            if (!quotaEnabled) {
                return "Quota is disabled.";
            }
            quotaSystem.refreshUserQuotas();
            Optional<ByteUnit> displayUnit = humanReadable
                  ? Optional.empty()
                  : Optional.of(ByteUnit.BYTES);

            ColumnWriter table = new ColumnWriter()
                  .header("uid").right("uid").space()
                  .header("CUSTODIAL").bytes("custodial", displayUnit, ByteUnit.Type.DECIMAL)
                  .space()
                  .header("limit")
                  .bytes("custodial_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE").space()
                  .header("REPLICA").bytes("replica", displayUnit, ByteUnit.Type.DECIMAL).space()
                  .header("limit")
                  .bytes("replica_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE").space()
                  .header("OUTPUT").bytes("output", displayUnit, ByteUnit.Type.DECIMAL).space()
                  .header("limit").bytes("output_limit", displayUnit, ByteUnit.Type.DECIMAL, "NONE")
                  .space();
            Map<Integer, Quota> quotas = quotaSystem.getUserQuotas();
            if (uid != null) {
                int iuid = Integer.parseInt(uid);
                Quota quota = quotas.get(iuid);
                if (quota == null) {
                    throw new CacheException("No user found");
                }
                fillRow(table, quota);
            } else {
                SortedSet<Integer> keys = new TreeSet<>(quotas.keySet());
                for (Integer key : keys) {
                    Quota quota = quotas.get(key);
                    fillRow(table, quota);
                }
            }
            return table.toString();
        }

        private void fillRow(ColumnWriter table, Quota quota) {
            table.row()
                  .value("uid", quota.getId())
                  .value("custodial", quota.getUsedCustodialSpace())
                  .value("custodial_limit", quota.getCustodialSpaceLimit())
                  .value("replica", quota.getUsedReplicaSpace())
                  .value("replica_limit", quota.getReplicaSpaceLimit())
                  .value("output", quota.getUsedOutputSpace())
                  .value("output_limit", quota.getOutputSpaceLimit());
        }
    }

    @Command(name = "set user quota",
          hint = "Set user quota",
          description = "Set user quota")

    public class SetUserQuotaCommand implements Callable<String> {

        @Argument(index = 0,
              valueSpec = "UID",
              usage = "User uid.")
        int uid;

        @Option(name = "custodial",
              usage = "Specify custodial user quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes. \"null\" value means no limit.")
        String custodial;

        @Option(name = "replica",
              usage = "Specify replica user quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes.  \"null\" value means no limit.")
        String replica;

        @Option(name = "output",
              usage = "Specify output user quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes. \"null\" value means no limit.")
        String output;

        @Override
        public String call() throws CacheException {
            if (!quotaEnabled) {
                return "Quota is disabled.";
            }
            if (custodial == null &&
                  replica == null &&
                  output == null) {
                throw new CacheException("At least one option needs to be specified");
            }

            quotaSystem.refreshUserQuotas();
            Quota quota = quotaSystem.getUserQuotas().get(uid);

            if (quota == null) {
                quota = new Quota(uid, 0, null,
                      0, null, 0, null);
                quotaSystem.createUserQuota(quota);
            }
            Quota.furnishQuota(quota, custodial, output, replica);
            quotaSystem.setUserQuota(quota);
            return quota.toString();
        }
    }

    @Command(name = "set group quota",
          hint = "Set group quota",
          description = "Set group quota")

    public class SetGroupQuotaCommand implements Callable<String> {

        @Argument(index = 0,
              valueSpec = "GID",
              usage = "Group id.")
        int gid;

        @Option(name = "custodial",
              usage = "Specify custodial group quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes. \"null\" value means no limit.")
        String custodial;

        @Option(name = "replica",
              usage = "Specify replica group quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes.  \"null\" value means no limit.")
        String replica;

        @Option(name = "output",
              usage = "Specify output group quota in bytes, with optional byte unit suffix " +
                    "using either SI or IEEE 1541 prefixes. \"null\" value means no limit.")
        String output;

        @Override
        public String call() throws CacheException {
            if (!quotaEnabled) {
                return "Quota is disabled.";
            }
            if (custodial == null &&
                  replica == null &&
                  output == null) {
                throw new CacheException("At least one option needs to be specified");
            }

            quotaSystem.refreshGroupQuotas();
            Quota quota = quotaSystem.getGroupQuotas().get(gid);

            if (quota == null) {
                quota = new Quota(gid, 0, null,
                      0, null, 0, null);
                quotaSystem.createGroupQuota(quota);
            }
            Quota.furnishQuota(quota, custodial, output, replica);
            quotaSystem.setGroupQuota(quota);
            return quota.toString();
        }
    }

    @Command(name = "remove user quota",
          hint = "remove user quota",
          description = "delete user quota from quota system")
    public class RemoveUserQuotaCommand implements Callable<String> {

        @Argument(index = 0,
              valueSpec = "UID",
              usage = "User uid.")
        int uid;

        @Override
        public String call() throws CacheException {
            quotaSystem.deleteUserQuota(uid);
            return "Removed user quota for " + uid;
        }
    }

    @Command(name = "remove group quota",
          hint = "remove group quota",
          description = "delete group quota from quota system")
    public class RemoveGroupQuotaCommand implements Callable<String> {

        @Argument(index = 0,
              valueSpec = "GID",
              usage = "Group id.")
        int gid;

        @Override
        public String call() throws CacheException {
            quotaSystem.deleteGroupQuota(gid);
            return "Removed group quota for " + gid;
        }
    }

    public static final String hh_add_file_cache_location = "<pnfsid> <pool name>";

    public String ac_add_file_cache_location_$_2(Args args) throws Exception {

        PnfsId pnfsId = new PnfsId(args.argv(0));
        String cacheLocation = args.argv(1);

        /* At this point, the file is no longer new and should really
         * have level 2 set. Otherwise we would not be able to detect
         * when the file is deleted. Unfortunately, the only way to
         * ensure that level 2 exists (without changing the
         * interface), is to remove a non-existing attribute.
         *
         * If the cache location provider happens to be the same as
         * the name space provider, then is unnecessary, as setting
         * the cache location will set level 2.
         *
         * NOTE: Temporarily uncommented, as file leakage is not
         * fixed in the pool anyway.
         *
         if (_nameSpaceProvider != _cacheLocationProvider) {
         _nameSpaceProvider.removeFileAttribute(pnfsId,
         "_this_entry_doesn't_exist_");
         }
        */

        _nameSpaceProvider.addCacheLocation(ROOT, pnfsId, cacheLocation);

        return "";

    }

    public static final String hh_clear_file_cache_location = "<pnfsid> <pool name>";

    public String ac_clear_file_cache_location_$_2(Args args) throws Exception {

        PnfsId pnfsId = new PnfsId(args.argv(0));
        String cacheLocation = args.argv(1);

        _nameSpaceProvider.clearCacheLocation(ROOT, pnfsId, cacheLocation, false);

        return "";
    }

    @Command(name = "add file checksum",
          hint = "adds new checksum to the file",
          description = "Adds checksum value storage for the specific file and " +
                "checksum type. If the file already has a checksum corresponding " +
                "to the specified type it should be cleared, otherwise 'Checksum " +
                "mismatch' error is raised. Only one checksum with the " +
                "corresponding type could be added.")
    public class AddFileChecksumCommand implements Callable<String> {

        @Argument(index = 0,
              usage = "The unique identifier of the file within dCache system.")
        PnfsId pnfsId;

        @Argument(index = 1,
              usage = "The checksums type of the file. The following checksums " +
                    "are supported: adler32, md5 and md4.")
        ChecksumType type;

        @Argument(index = 2,
              usage = "The checksum value in hexadecimal.")
        String checksumValue;

        @Override
        public String call() throws CacheException {
            Checksum checksum = new Checksum(type, checksumValue);
            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId,
                  FileAttributes.ofChecksum(checksum), EnumSet.noneOf(FileAttribute.class));
            return "";
        }
    }

    @Command(name = "clear file checksum",
          hint = "clears existing checksum for the file",
          description = "Clears checksum value storage for the specific file and " +
                "checksum type.")
    public class ClearFileChecksumCommand implements Callable<String> {

        @Argument(index = 0,
              usage = "The unique identifier of the file within dCache system.")
        PnfsId pnfsId;

        @Argument(index = 1,
              usage = "The checksums type of the file. These following checksums " +
                    "are supported: adler32, md5 and md4.")
        ChecksumType type;

        @Override
        public String call() throws CacheException {
            _nameSpaceProvider.removeChecksum(ROOT, pnfsId, type);
            return "";
        }
    }

    @Command(name = "get file checksum",
          hint = "returns file checksum",
          description = "Display the checksum corresponding to the specified file and " +
                "the checksum type. Nothing is returned if there is no corresponding checksum.")
    public class GetFileChecksumCommand implements Callable<String> {

        @Argument(index = 0,
              usage = "The unique identifier of the file.")
        PnfsId pnfsId;

        @Argument(index = 1,
              usage = "The checksums type of the file. These following checksums " +
                    "are supported: adler32, md5 and md4.")
        String typeArg;

        @Override
        public String call() throws CacheException, NoSuchAlgorithmException {
            ChecksumType type = ChecksumType.getChecksumType(typeArg);
            return getChecksums(ROOT, pnfsId).stream()
                  .filter(c -> c.getType() == type)
                  .map(Checksum::toString)
                  .findAny()
                  .orElse("");
        }
    }

    @Command(name = "get file checksums",
          hint = "returns file checksums of all types stored in the PnfsManager",
          description =
                "Display the checksums of all the types stored in PnfsManager corresponding to " +
                      "the specified file. Nothing is returned if there is no corresponding checksum.")
    public class GetFileChecksumsCommand implements Callable<String> {

        @Argument(index = 0,
              usage = "PnfsId of the file.")
        PnfsId pnfsId;

        @Override
        public String call() throws CacheException, NoSuchAlgorithmException {
            Set<Checksum> checksums = getChecksums(ROOT, pnfsId);

            if (checksums.isEmpty()) {
                return "";
            } else {
                ColumnWriter writer = new ColumnWriter()
                      .header("TYPE").left("type").space()
                      .header("CHECKSUM").left("checksum");
                for (Checksum checksum : checksums) {
                    writer.row()
                          .value("type", checksum.getType())
                          .value("checksum", checksum.getValue());
                }
                return writer.toString();
            }
        }
    }

    @Command(name = "set log slow threshold",
          hint = "configure logging of slow namespace interactions",
          description = "Enable and configure how slow namespace "
                + "interactions are logged.  If an interaction takes "
                + "longer to complete than the configured threshold "
                + "duration then a warning is logged.")
    public class SetLogLowThresholdCommand implements Callable<String> {

        @Argument(usage = "The minimum duration of a namespace interaction, "
              + "in milliseconds, before a warning is logged.  The value "
              + "must be greater than zero.")
        int timeout;

        @Override
        public String call() throws CommandException {
            if (timeout <= 0) {
                throw new CommandException("Timeout must be greater than zero.");
            }

            _logSlowThreshold = timeout;

            return "";
        }
    }

    @Command(name = "get log slow threshold",
          hint = "the current threshold for reporting slow namespace interactions",
          description = "If this feature is currently disabled, returns "
                + "\"disabled\" otherwise returns the duration in "
                + "milliseconds.")
    public class GetLogSlowThresholdCommand implements Callable<String> {

        @Override
        public String call() {
            return _logSlowThreshold == THRESHOLD_DISABLED
                  ? "disabled"
                  : (_logSlowThreshold + " ms");
        }
    }

    @Command(name = "set log slow threshold disabled",
          hint = "disable reporting of slow namespace interactions",
          description = "Do not log a warning if namespace interactions "
                + "are slow.")
    public class SetLogSlowThresfoldDisabledCommand implements Callable<String> {

        @Override
        public String call() {
            _logSlowThreshold = THRESHOLD_DISABLED;
            return "";
        }

    }

    @Command(name = "show list activity", hint = "describe directory listing requests",
          description = "Show details of the directory list requests, both"
                + " those requests that are currently queued and and that"
                + " are currently being processed."
                + "\n\n"
                + "The output is organised into a table that is"
                + " separated into two sections: queued requests and"
                + " active requests.  If there are no requests in a"
                + " section then that section is omitted.  If there are no"
                + " requests at all then the table is replaced by a simple"
                + " statement confirming the lack of activity.\n"
                + "\n"
                + "The table has the following columns:\n"
                + "\n"
                + "  SOURCE    \tthe fully qualified cell name of the door\n"
                + "            \tmaking the request.\n"
                + "\n"
                + "  SESSION   \tthe ID of the client session within the\n"
                + "            \tdirectory listing was made.\n"
                + "\n"
                + "  USER      \tthe identity of the user if the session is\n"
                + "            \tauthenticated, or the IP address of the\n"
                + "            \tclient if the request was made anonymously.\n"
                + "\n"
                + "  ARRIVED   \tthe time when the request was received by\n"
                + "            \tPnfsManager, both as an absolute timestamp\n"
                + "            \tand relative to this command's invocation.\n"
                + "\n"
                + "  STARTED   \tthe time when PnfsManager started\n"
                + "            \tprocessing the request, both as an\n"
                + "            \tabsoluate timestamp and relative to this\n"
                + "            \tcommand's invocation.  If the request is\n"
                + "            \tqueued then this field is left blank.\n"
                + "\n"
                + "  FILTER    \tthe glob pattern used to select which\n"
                + "            \tcontents to return: the name of the file or\n"
                + "            \tdirectory must match the glob pattern.  All\n"
                + "            \tcontents are selected if the filter is\n"
                + "            \tempty.\n"
                + "\n"
                + "  RANGE     \tthe desired range of index values, with the\n"
                + "            \tfirst listed item having index 0.  Some \n"
                + "            \tRANGE values can result in a partial\n"
                + "            \tdirectory listing.  Note that, although the\n"
                + "            \tRANGE value may limit the number of items\n"
                + "            \tin the reply, the table shows the range in\n"
                + "            \tstandard maths notation, and not directly\n"
                + "            \tthe count limit.\n"
                + "\n"
                + "            \tTwo commonly seen RANGE values are the\n"
                + "            \tsemi-infinite range \"[0..+\u221e)\" and the\n"
                + "            \tinfinite range \"(-\u221e..+\u221e)\".  Both indicate\n"
                + "            \tthat a complete directory listing is\n"
                + "            \trequested.\n"
                + "\n"
                + "  ATTRIBUTES\tthe list of file attributes the door has\n"
                + "            \trequested about each listed file or\n"
                + "            \tdirectory.\n"
                + "\n"
                + "  PATH      \tthe path of the directory to be listed.\n"
                + "\n"
                + "There are several command-line options that control"
                + " whether various table columns are shown.  These options"
                + " are switched off by default to aid readability.")
    public class ShowListActivityCommand implements Callable<String> {

        @Option(name = "session", usage = "whether to the SESSION column.")
        private boolean includeSession;

        @Option(name = "attributes", usage = "whether to show the ATTRIBUTES"
              + " column.")
        private boolean includeAttributes;

        @Option(name = "times", usage = "whether to show the ARRIVED and STARTED"
              + " columns.")
        private boolean includeTimes;

        @Option(name = "selection", usage = "whether to show the FILTER and RANGE"
              + " columns.")
        private boolean includeSelection;

        @Override
        public String call() {
            ColumnWriter writer = buildColumnWriter();

            if (!_listQueue.isEmpty()) {
                writer.section("QUEUED REQUESTS");
                _listQueue.forEach(e -> addRow(writer.row(), e));
            }

            List<ActivityReport> activity = _listProcessThreads.stream()
                  .map(ProcessThread::getCurrentActivity)
                  .filter(Optional::isPresent)
                  .map(Optional::get)
                  .collect(Collectors.toList());

            if (!activity.isEmpty()) {
                writer.section("ACTIVE REQUESTS");
                activity.forEach(r -> addRow(writer.row(), r));
            }

            String report = writer.toString();
            return report.isEmpty() ? "Currently no list activity" : report;
        }

        private ColumnWriter buildColumnWriter() {
            ColumnWriter writer = new ColumnWriter()
                  .header("SOURCE").right("src-cell").fixed("@").left("src-domain").space();

            if (includeSession) {
                writer.header("SESSION").left("session").space();
            }

            writer.header("USER").left("user").space();

            if (includeTimes) {
                writer.header("ARRIVED").left("arrived").space()
                      .header("STARTED").left("started").space();
            }

            if (includeSelection) {
                writer.header("FILTER").left("filter").space()
                      .header("RANGE").left("range").space();
            }

            if (includeAttributes) {
                writer.header("ATTRIBUTES").left("attributes").space();
            }

            writer.header("PATH").left("path");

            return writer;
        }

        private void addRow(TabulatedRow row, CellMessage envelope) {
            row.value("session", envelope.getSession());

            CellAddressCore src = envelope.getDestinationPath().isFinalDestination()
                  // has revertDirection been called?
                  ? envelope.getSourceAddress() // not yet
                  : envelope.getDestinationPath().getDestinationAddress(); // yes
            row.value("src-cell", src.getCellName());
            row.value("src-domain", src.getCellDomainName());

            Instant arrived = Instant.now().minusMillis(envelope.getLocalAge());
            row.value("arrived", TimeUtils.relativeTimestamp(arrived));

            Object msgObject = envelope.getMessageObject();
            if (!(msgObject instanceof PnfsListDirectoryMessage)) {
                LOGGER.warn("Found non PnfsListDirectoryMessage {} on _listQueue", msgObject);
                return;
            }
            PnfsListDirectoryMessage message = (PnfsListDirectoryMessage) msgObject;
            row.value("user", Subjects.getDisplayName(message.getSubject()))
                  .value("filter", message.getPattern())
                  .value("range", message.getRange())
                  .value("attributes", message.getRequestedAttributes())
                  .value("path", message.getPnfsPath());
        }

        private void addRow(TabulatedRow row, ActivityReport report) {
            addRow(row, report.message);
            row.value("started", TimeUtils.relativeTimestamp(report.whenStarted));
        }
    }

    private void dumpThreadQueue(int queueId) {
        if (queueId < 0 || queueId >= _fifos.length) {
            throw new IllegalArgumentException(" illegal queue #" + queueId);
        }
        BlockingQueue<CellMessage> fifo = _fifos[queueId];
        Object[] fifoContent = fifo.toArray();

        LOGGER.warn("PnfsManager thread #{} queue dump ({}):", queueId, fifoContent.length);

        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < fifoContent.length; i++) {
            sb.append("fifo[").append(i).append("] : ");
            sb.append(fifoContent[i]).append('\n');
        }

        LOGGER.warn(sb.toString());
    }

    private Set<Checksum> getChecksums(Subject subject, PnfsId pnfsId)
          throws CacheException, NoSuchAlgorithmException {
        FileAttributes attributes =
              _nameSpaceProvider.getFileAttributes(subject, pnfsId,
                    EnumSet.of(FileAttribute.CHECKSUM));
        return attributes.getChecksumsIfPresent().orElse(Collections.emptySet());
    }

    private void updateFlag(PnfsFlagMessage pnfsMessage) {

        PnfsId pnfsId = pnfsMessage.getPnfsId();
        PnfsFlagMessage.FlagOperation operation = pnfsMessage.getOperation();
        String flagName = pnfsMessage.getFlagName();
        String value = pnfsMessage.getValue();
        Subject subject = pnfsMessage.getSubject();
        LOGGER.info("update flag " + operation + " flag=" + flagName + " value=" +
              value + " for " + pnfsId);

        try {
            // Note that dcap clients may bypass restrictions by not
            // specifying a path when interacting via mounted namespace.
            checkRestriction(pnfsMessage, UPDATE_METADATA);
            if (operation == PnfsFlagMessage.FlagOperation.GET) {
                pnfsMessage.setValue(updateFlag(subject, pnfsId, operation, flagName, value));
            } else {
                updateFlag(subject, pnfsId, operation, flagName, value);
            }

        } catch (FileNotFoundCacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Exception in updateFlag: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Exception in updateFlag", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private String updateFlag(Subject subject, PnfsId pnfsId,
          PnfsFlagMessage.FlagOperation operation,
          String flagName, String value)
          throws CacheException {
        FileAttributes attributes;
        switch (operation) {
            case SET:
                LOGGER.info("flags set " + pnfsId + " " + flagName + "=" + value);
                _nameSpaceProvider.setFileAttributes(subject, pnfsId,
                      FileAttributes.ofFlag(flagName, value), EnumSet.noneOf(FileAttribute.class));
                break;
            case SETNOOVERWRITE:
                LOGGER.info("flags set (dontoverwrite) " + pnfsId + " " + flagName + "=" + value);
                attributes = _nameSpaceProvider.getFileAttributes(subject, pnfsId,
                      EnumSet.of(FileAttribute.FLAGS));
                String current = attributes.getFlags().get(flagName);
                if ((current == null) || (!current.equals(value))) {
                    updateFlag(subject, pnfsId, PnfsFlagMessage.FlagOperation.SET,
                          flagName, value);
                }
                break;
            case GET:
                attributes = _nameSpaceProvider.getFileAttributes(subject, pnfsId,
                      EnumSet.of(FileAttribute.FLAGS));
                String v = attributes.getFlags().get(flagName);
                LOGGER.info("flags ls " + pnfsId + " " + flagName + " -> " + v);
                return v;
            case REMOVE:
                LOGGER.info("flags remove " + pnfsId + " " + flagName);
                _nameSpaceProvider.removeFileAttribute(subject, pnfsId, flagName);
                break;
        }
        return null;
    }

    public void addCacheLocation(PnfsAddCacheLocationMessage pnfsMessage) {
        LOGGER.info("addCacheLocation : {} for {}", pnfsMessage.getPoolName(),
              pnfsMessage.getPnfsId());
        try {
            /* At this point, the file is no longer new and should
             * really have level 2 set. Otherwise we would not be able
             * to detect when the file is deleted. Unfortunately, the
             * only way to ensure that level 2 exists (without
             * changing the interface), is to remove a non-existing
             * attribute.
             *
             * If the cache location provider happens to be the same
             * as the name space provider, then is unnecessary, as
             * setting the cache location will set level 2.
             *
             * NOTE: Temporarily uncommented, as file leakage is not
             * fixed in the pool anyway.
             *
             if (_nameSpaceProvider != _cacheLocationProvider) {
             _nameSpaceProvider.removeFileAttribute(pnfsMessage.getPnfsId(),
             "_this_entry_doesn't_exist_");
             }
            */

            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, UPDATE_METADATA);
            _nameSpaceProvider.addCacheLocation(pnfsMessage.getSubject(),
                  pnfsMessage.getPnfsId(),
                  pnfsMessage.getPoolName());
        } catch (FileNotFoundCacheException fnf) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Exception in addCacheLocation: {}", e.toString());
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Exception in addCacheLocation", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Exception in addCacheLocation");
        }


    }

    public void clearCacheLocation(PnfsClearCacheLocationMessage pnfsMessage) {
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        LOGGER.info("clearCacheLocation : {} for {}", pnfsMessage.getPoolName(), pnfsId);
        try {
            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, UPDATE_METADATA);
            _nameSpaceProvider.clearCacheLocation(pnfsMessage.getSubject(),
                  pnfsId,
                  pnfsMessage.getPoolName(),
                  pnfsMessage.removeIfLast());
        } catch (FileNotFoundCacheException fnf) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Exception in clearCacheLocation for {}: {}", pnfsId, e.toString());
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Exception in clearCacheLocation for " + pnfsId, e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
        }


    }

    public void getCacheLocations(PnfsGetCacheLocationsMessage pnfsMessage) {
        Subject subject = pnfsMessage.getSubject();
        try {
            checkRestriction(pnfsMessage, READ_METADATA);
            PnfsId pnfsId = populatePnfsId(pnfsMessage);
            LOGGER.info("get cache locations for {}", pnfsId);

            checkMask(pnfsMessage);
            pnfsMessage.setCacheLocations(_nameSpaceProvider.getCacheLocation(subject, pnfsId));
            pnfsMessage.setSucceeded();
        } catch (FileNotFoundCacheException fnf) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Exception in getCacheLocations: {}", e.toString());
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Exception in getCacheLocations", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Pnfs lookup failed");
        }
    }

    private void checkMkdirAllowed(PnfsCreateEntryMessage message)
          throws PermissionDeniedCacheException {
        if (Subjects.isRoot(message.getSubject())) {
            return;
        }

        FsPath path = message.getFsPath();
        Restriction restriction = message.getRestriction();

        /* As a special case, if the user is allowed to upload into
         * a child directory then they are also allowed to create this
         * directory.  This allows the user to create missing parent
         * directories upto (but not including) the final element in the path.
         * The final element, if missing MUST be uploaded as a file.
         *
         * For example, if the allowed path for upload is '/data/test-1/item1'
         * where 'test-1' and 'item1' do not exist, then an attempt to create
         * the directory '/data/test-1' should succeed.  However, an attempt to
         * mkdir '/data/test-1/item1' should fail.
         */
        if (restriction.hasUnrestrictedChild(UPLOAD, path)) {
            return;
        }

        /* As another special case, allow the user to create a directory if
         * the user is allowed to upload.  We want to ensure that the user
         * cannot create a directory for the final the target directory.
         *
         * For example, if the allowed path for upload is '/data/test-1/item1`
         * where 'item1' already exists as a directory, then the user should
         * be allowed to create the directory '/data/test-1/item1/subdir1'.
         *
         * Example 2, if the allowed path for upload is '/data/test-1/item1'
         * where 'test-1' already exists but 'item1' does not exist then
         * the user SHOULD NOT be allowed to create 'item1' as a directory.
         *
         * To cover these two cases, we check whether the user would be allowed
         * to upload a file as the same name as the parent for the new
         * directory.  In example 1, the parent is 'item1' and the user would
         * be allowed to upload this file (if it didn't alreaedy exist).
         * In example 2, the parent is 'test-1' and the user is not allowed to
         * upload this file.
         */
        if (!path.isRoot() && !restriction.isRestricted(UPLOAD, path.parent())) {
            return;
        }

        /**
         * A regular permissions check, categorising this operation as a MANAGE
         * activity.
         */
        checkRestrictionOnParent(message, MANAGE);
    }

    public void createEntry(PnfsCreateEntryMessage pnfsMessage) {
        checkArgument(pnfsMessage.getFileAttributes().isDefined(TYPE));
        FileAttributes assign = pnfsMessage.getFileAttributes();
        FileType type = assign.removeFileType();
        Subject subject = pnfsMessage.getSubject();
        String path = pnfsMessage.getPnfsPath();

        try {
            File file = new File(path);
            checkMask(subject, file.getParent(), pnfsMessage.getAccessMask());

            Set<FileAttribute> requested = pnfsMessage.getAcquire();
            FileAttributes attrs = null;

            switch (type) {
                case DIR:
                    LOGGER.info("create directory {}", path);
                    checkMkdirAllowed(pnfsMessage);

                    PnfsId pnfsId = _nameSpaceProvider.createDirectory(subject, path,
                          assign);

                    pnfsMessage.setPnfsId(pnfsId);

                    //
                    // FIXME : is it really true ?
                    //
                    // now we try to get the storageInfo out of the
                    // parent directory. If it fails, we don't care.
                    // We declare the request to be successful because
                    // the createEntry seem to be ok.
                    try {
                        LOGGER.info("Trying to get storageInfo for {}", pnfsId);

                        /* If we were allowed to create the entry above, then
                         * we also ought to be allowed to read it here. Hence
                         * we use ROOT as the subject.
                         */
                        attrs = _nameSpaceProvider.getFileAttributes(ROOT, pnfsId, requested);
                    } catch (CacheException e) {
                        LOGGER.warn("Can't determine storageInfo: {}", e.toString());
                    }
                    break;

                case REGULAR:
                    LOGGER.info("create file {}", path);
                    checkRestriction(pnfsMessage, UPLOAD);
                    requested.add(FileAttribute.STORAGEINFO);
                    requested.add(FileAttribute.PNFSID);

                    attrs = _nameSpaceProvider.createFile(subject, path, assign, requested);

                    StorageInfo info = attrs.getStorageInfo();
                    if (info.getKey("path") == null) {
                        info.setKey("path", path);
                    }
                    info.setKey("uid", Integer.toString(assign.getOwnerIfPresent().orElse(-1)));
                    info.setKey("gid", Integer.toString(assign.getGroupIfPresent().orElse(-1)));

                    // REVISIT: consider removing xattr injection once pools can accept FileAttribute.XATTR
                    if (assign.isDefined(XATTR)) {
                        assign.getXattrs()
                              .forEach((k, v) -> info.setKey(STORAGE_INFO_XATTR_PREFIX + k, v));
                    }

                    pnfsMessage.setPnfsId(attrs.getPnfsId());
                    break;

                case LINK:
                    checkArgument(pnfsMessage instanceof PnfsCreateSymLinkMessage);
                    String destination = ((PnfsCreateSymLinkMessage) pnfsMessage).getDestination();
                    LOGGER.info("create symlink {} to {}", path, destination);

                    checkRestrictionOnParent(pnfsMessage, MANAGE);

                    pnfsId = _nameSpaceProvider.createSymLink(subject, path,
                          destination, assign);

                    pnfsMessage.setPnfsId(pnfsId);
                    break;

                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }

            pnfsMessage.setFileAttributes(attrs);
            pnfsMessage.setSucceeded();
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Bug found when creating entry:", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void createUploadPath(PnfsCreateUploadPath message) {
        try {
            checkRestriction(message, UPLOAD);
            FsPath uploadPath = _nameSpaceProvider.createUploadPath(message.getSubject(),
                  message.getPath(),
                  message.getRootPath(),
                  message.getSize(),
                  message.getAccessLatency(),
                  message.getRetentionPolicy(),
                  message.getSpaceToken(),
                  message.getOptions());
            message.setUploadPath(uploadPath);
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Create upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void commitUpload(PnfsCommitUpload message) {
        try {
            checkRestriction(message, UPLOAD);
            FileAttributes attributes = _nameSpaceProvider.commitUpload(message.getSubject(),
                  message.getUploadPath(),
                  message.getPath(),
                  message.getOptions(),
                  message.getRequestedAttributes());
            message.setFileAttributes(attributes);
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Commit upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void cancelUpload(PnfsCancelUpload message) {
        Subject subject = message.getSubject();
        String explanation = message.getExplanation();

        try {
            checkRestriction(message, UPLOAD);

            Set<FileAttribute> requested = message.getRequestedAttributes();
            requested.addAll(EnumSet.of(PNFSID, NLINK, SIZE));
            Collection<FileAttributes> deletedFiles =
                  _nameSpaceProvider.cancelUpload(subject,
                        message.getUploadPath(), message.getPath(), requested,
                        explanation);

            deletedFiles.stream()
                  .filter(f -> f.isUndefined(SIZE)) // currently uploading
                  .filter(f -> f.getNlink() == 1) // with no hard links
                  .map(FileAttributes::getPnfsId)
                  .forEach(id ->
                        _cancelUploadNotificationTargets.stream()
                              .map(CellPath::new)
                              .forEach(p ->
                                    _stub.notify(p,
                                          new DoorCancelledUploadNotificationMessage(subject,
                                                id, explanation))));

            message.setDeletedFiles(deletedFiles);
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Cancel upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void deleteEntry(PnfsDeleteEntryMessage pnfsMessage) {
        String path = pnfsMessage.getPnfsPath();
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        Subject subject = pnfsMessage.getSubject();
        Set<FileType> allowed = pnfsMessage.getAllowedFileTypes();
        Set<FileAttribute> requested = pnfsMessage.getRequestedAttributes();

        try {
            if (path == null && pnfsId == null) {
                throw new InvalidMessageCacheException(
                      "pnfsid or path have to be defined for PnfsDeleteEntryMessage");
            }

            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, DELETE);

            FileAttributes attributes;

            if (path != null) {
                LOGGER.info("delete PNFS entry for {}", path);
                if (pnfsId != null) {
                    attributes = _nameSpaceProvider.deleteEntry(subject, allowed,
                          pnfsId, path, requested);
                } else {
                    requested.add(PNFSID);
                    attributes = _nameSpaceProvider.deleteEntry(subject, allowed,
                          path, requested);
                    pnfsMessage.setPnfsId(attributes.getPnfsId());
                }
            } else {
                LOGGER.info("delete PNFS entry for {}", pnfsId);
                attributes = _nameSpaceProvider.deleteEntry(subject, allowed,
                      pnfsId, requested);
            }

            pnfsMessage.setFileAttributes(attributes);
            pnfsMessage.setSucceeded();
        } catch (FileNotFoundCacheException e) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, e.getMessage());
        } catch (CacheException e) {
            LOGGER.warn("Failed to delete entry: {}", e.getMessage());
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Failed to delete entry", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void rename(PnfsRenameMessage msg) {
        try {
            checkMask(msg);
            PnfsId pnfsId = msg.getPnfsId();
            String sourcePath = msg.getPnfsPath();
            String destinationPath = msg.newName();
            // This case is for compatibility with versions before 2.14
            if (sourcePath == null) {
                if (pnfsId == null) {
                    throw new InvalidMessageCacheException("Either path or pnfs id is required.");
                }
                sourcePath = _nameSpaceProvider.pnfsidToPath(msg.getSubject(), pnfsId);
            }
            LOGGER.info("Rename {} to new name: {}", sourcePath, destinationPath);
            checkRestriction(msg, MANAGE, FsPath.create(sourcePath).parent());
            checkRestriction(msg, MANAGE, FsPath.create(destinationPath).parent());
            boolean overwrite = msg.getOverwrite()
                  && !msg.getRestriction().isRestricted(DELETE, FsPath.create(destinationPath));
            _nameSpaceProvider.rename(msg.getSubject(), pnfsId, sourcePath, destinationPath,
                  overwrite);
        } catch (CacheException e) {
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  "Pnfs rename failed");
        }
    }

    private String pathfinder(Subject subject, PnfsId pnfsId)
          throws CacheException {
        return _nameSpaceProvider.pnfsidToPath(subject, pnfsId);
    }

    public void mapPath(PnfsMapPathMessage pnfsMessage) {
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        Subject subject = pnfsMessage.getSubject();

        if (pnfsId == null) {
            pnfsMessage.setFailed(5, "Illegal Arguments : need pnfsid");
            return;
        }

        try {
            LOGGER.info("map:  id2path for {}", pnfsId);
            String path = pathfinder(subject, pnfsId);
            checkRestriction(pnfsMessage, READ_METADATA, FsPath.create(path));
            pnfsMessage.setGlobalPath(path);
            checkMask(pnfsMessage);
        } catch (FileNotFoundCacheException fnf) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage());
        } catch (CacheException ce) {
            pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
            LOGGER.warn("mapPath: {}", ce.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error("Exception in mapPath (pathfinder) " + e, e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void getParent(PnfsGetParentMessage msg) {
        try {
            PnfsId pnfsId = populatePnfsId(msg);
            checkMask(msg);
            _nameSpaceProvider.find(msg.getSubject(), pnfsId)
                  .forEach(l -> msg.addLocation(l.getParent(), l.getName()));
        } catch (CacheException e) {
            LOGGER.warn(e.toString());
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }


    /**
     * PnfsListDirectoryMessages can have more than one reply. This is to avoid large directories
     * exhausting available memory in the PnfsManager. In current versions of Java, the only way to
     * list a directory without building the complete result array in memory is to gather the
     * elements inside a filter.
     * <p>
     * This filter collects entries and sends partial replies for the PnfsListDirectoryMessage when
     * a certain number of entries have been collected. The filter will not send the final reply
     * (the caller has to do that).
     */
    private class ListHandlerImpl implements ListHandler {

        private final CellPath _requestor;
        private final PnfsListDirectoryMessage _msg;
        private final long _delay;
        private final UOID _uoid;
        private final FsPath _directory;
        private final Subject _subject;
        private final Restriction _restriction;
        private long _deadline;
        private int _messageCount;

        public ListHandlerImpl(CellPath requestor, UOID uoid,
              PnfsListDirectoryMessage msg,
              long initialDelay, long delay) {
            _msg = msg;
            _requestor = requestor;
            _uoid = uoid;
            _delay = delay;
            _directory = requireNonNull(_msg.getFsPath());
            _subject = _msg.getSubject();
            _restriction = _msg.getRestriction();
            _deadline =
                  (delay == Long.MAX_VALUE)
                        ? Long.MAX_VALUE
                        : System.currentTimeMillis() + initialDelay;
        }

        private void sendPartialReply() {
            _msg.setReply();

            CellMessage envelope = new CellMessage(_requestor, _msg);
            envelope.setLastUOID(_uoid);
            sendMessage(envelope);
            _messageCount++;

            _msg.clear();
        }

        @Override
        public void addEntry(String name, FileAttributes attrs) {
            if (Subjects.isRoot(_subject)
                  || !_restriction.isRestricted(READ_METADATA, _directory, name)) {
                long now = System.currentTimeMillis();
                _msg.addEntry(name, attrs);
                if (_msg.getEntries().size() >= _directoryListLimit ||
                      now > _deadline) {
                    sendPartialReply();
                    _deadline =
                          (_delay == Long.MAX_VALUE) ? Long.MAX_VALUE : now + _delay;
                }
            }
        }

        public int getMessageCount() {
            return _messageCount;
        }
    }

    private void listDirectory(CellMessage envelope, PnfsListDirectoryMessage msg) {
        if (!msg.getReplyRequired()) {
            return;
        }

        try {
            String path = msg.getPnfsPath();

            checkMask(msg.getSubject(), path, msg.getAccessMask());
            checkRestriction(msg, LIST);

            long delay = envelope.getAdjustedTtl();
            long initialDelay =
                  (delay == Long.MAX_VALUE)
                        ? Long.MAX_VALUE
                        : delay - envelope.getLocalAge();
            CellPath source = envelope.getSourcePath().revert();
            ListHandlerImpl handler =
                new ListHandlerImpl(source, envelope.getUOID(),
                                    msg, initialDelay, delay);

            if (msg.getPathType() == PnfsListDirectoryMessage.PathType.LABEL) {
                _nameSpaceProvider.listVirtualDirectory(msg.getSubject(), path.substring(1),
                      msg.getRange(),
                      msg.getRequestedAttributes(),
                      handler);

            } else {
                _nameSpaceProvider.list(msg.getSubject(), path,
                      msg.getPattern(),
                      msg.getRange(),
                      msg.getRequestedAttributes(),
                      handler);
            }
            msg.setSucceeded(handler.getMessageCount() + 1);
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (CacheException e) {
            LOGGER.warn(e.toString());
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                  e.getMessage());
        }
    }

    private static class ActivityReport {

        private final CellMessage message;
        private final Instant whenStarted;

        ActivityReport(CellMessage message, Instant whenStarted) {
            this.message = message;
            this.whenStarted = whenStarted;
        }
    }

    private class ProcessThread implements Runnable {

        private final BlockingQueue<CellMessage> _fifo;

        private volatile CellMessage _activeMessage;
        private volatile Instant _whenStarted;

        private ProcessThread(BlockingQueue<CellMessage> fifo) {
            _fifo = fifo;
        }

        public synchronized Optional<ActivityReport> getCurrentActivity() {
            if (_activeMessage == null) {
                return Optional.empty();
            } else {
                ActivityReport report = new ActivityReport(_activeMessage, _whenStarted);
                return Optional.of(report);
            }
        }

        private synchronized void recordActivity(CellMessage message) {
            _activeMessage = message;
            _whenStarted = Instant.now();
        }

        private synchronized void clearActivity() {
            _activeMessage = null;
            _whenStarted = null;
        }

        @Override
        public void run() {
            try {
                for (CellMessage message = _fifo.take(); message != SHUTDOWN_SENTINEL;
                      message = _fifo.take()) {
                    CDC.setMessageContext(message);
                    try {
                        recordActivity(message);

                        /* Discard messages if we are close to their
                         * timeout (within 10% of the TTL or 10 seconds,
                         * whatever is smaller)
                         */
                        PnfsMessage pnfs = (PnfsMessage) message.getMessageObject();
                        if (message.getLocalAge() > message.getAdjustedTtl() && useEarlyDiscard(
                              pnfs)) {
                            LOGGER.warn("Discarding {} because its time to live has been exceeded.",
                                  pnfs.getClass().getSimpleName());
                            sendTimeout(message, "TTL exceeded");
                            continue;
                        }

                        processPnfsMessage(message, pnfs);
                        fold(pnfs);
                    } catch (Throwable e) {
                        LOGGER.warn("processPnfsMessage: {} : {}", Thread.currentThread().getName(),
                              e);
                    } finally {
                        clearActivity();
                        CDC.clearMessageContext();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        protected void fold(PnfsMessage message) {
            if (_canFold && message.getReturnCode() == 0) {
                Iterator<CellMessage> i = _fifo.iterator();
                while (i.hasNext()) {
                    CellMessage envelope = i.next();
                    PnfsMessage other =
                          (PnfsMessage) envelope.getMessageObject();

                    if (other.invalidates(message)) {
                        break;
                    }

                    if (other.fold(message)) {
                        LOGGER.info("Folded {}", other.getClass().getSimpleName());
                        _foldedCounters.incrementRequests(message.getClass());

                        i.remove();
                        envelope.revertDirection();

                        sendMessage(envelope);
                    }
                }
            }
        }
    }

    /*
     *  ------------------------------------- QUOTA SYSTEM -------------------------------------
     */
    public PnfsManagerQuotaMessage messageArrived(PnfsManagerQuotaMessage message) {
        try {
            if (message instanceof PnfsManagerGetQuotaMessage) {
                processQuotaRequest((PnfsManagerGetQuotaMessage) message);
            } else if (message instanceof PnfsManagerSetQuotaMessage) {
                processQuotaUpdate((PnfsManagerSetQuotaMessage) message);
            } else if (message instanceof PnfsManagerRemoveQuotaMessage) {
                processQuotaRemoval((PnfsManagerRemoveQuotaMessage) message);
            }
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        }
        return message;
    }

    private void processQuotaRequest(PnfsManagerGetQuotaMessage message)
          throws CacheException {
        if (!quotaEnabled) {
            throw new CacheException(CacheException.SERVICE_UNAVAILABLE, "Quota is disabled.");
        }

        QuotaType type = message.getType();
        Map<Integer, Quota> quotas;

        switch (type) {
            case USER:
                quotaSystem.refreshUserQuotas();
                quotas = quotaSystem.getUserQuotas();
                break;
            case GROUP:
                quotaSystem.refreshGroupQuotas();
                quotas = quotaSystem.getGroupQuotas();
                break;
            default:
                // should not occur!
                throw new RuntimeException("No such quota type " + type
                      + "; please report to dcache.org");
        }

        Integer uid = message.getQid();
        if (uid != null) {
            Quota quota = quotas.get(uid);
            if (quota == null) {
                throw new CacheException(CacheException.NO_ATTRIBUTE,
                      type.name().toLowerCase(Locale.ROOT) + " quota does not exist.");
            }
            addQuota(quota, uid, type, message);
        } else {
            quotas.entrySet().stream()
                  .forEach(e -> addQuota(e.getValue(), e.getKey(), type, message));
        }
    }

    private void processQuotaRemoval(PnfsManagerRemoveQuotaMessage message)
          throws CacheException {
        if (!quotaEnabled) {
            throw new CacheException(CacheException.SERVICE_UNAVAILABLE, "Quota is disabled.");
        }

        Integer qid = message.getQid();
        QuotaType type = message.getType();
        Quota quota = getQuota(qid, type);

        if (quota == null) {
            throw new CacheException(CacheException.NO_ATTRIBUTE,
                  "quota for " + qid + " does not exist.");
        }

        switch (type) {
            case USER:
                quotaSystem.deleteUserQuota(qid);
                break;
            case GROUP:
                quotaSystem.deleteGroupQuota(qid);
                break;
        }
    }

    private void processQuotaUpdate(PnfsManagerSetQuotaMessage message)
          throws CacheException {
        if (!quotaEnabled) {
            throw new CacheException(CacheException.SERVICE_UNAVAILABLE, "Quota is disabled.");
        }

        Integer qid = message.getQid();
        QuotaType type = message.getType();
        Quota quota = getQuota(qid, type);

        if (message.isNew() && quota != null) {
            throw new CacheException(CacheException.ATTRIBUTE_EXISTS,
                  "quota for " + qid + " already exists.");
        }

        if (!message.isNew() && quota == null) {
            throw new CacheException(CacheException.NO_ATTRIBUTE,
                  "quota for " + qid + " does not exist.");
        }

        if (quota == null) {
            quota = new Quota(qid, 0, null,
                  0, null,
                  0, null);
        }

        QuotaRequest request = message.getRequest();

        if (request == null) {
            if (!message.isNew()) {
                throw new CacheException(CacheException.INVALID_ARGS,
                      "At least one option needs to be specified");
            }
        } else {
            String custodial = request.getCustodialLimit();
            String replica = request.getReplicaLimit();
            String output = request.getOutputLimit();

            boolean atLeastOne = false;

            if (custodial != null) {
                custodial = custodial.replaceAll(QuotaRequest.NONE, "null");
                atLeastOne = true;
            }

            if (replica != null) {
                replica = replica.replaceAll(QuotaRequest.NONE, "null");
                atLeastOne = true;
            }

            if (output != null) {
                output = output.replaceAll(QuotaRequest.NONE, "null");
                atLeastOne = true;
            }

            if (!atLeastOne) {
                throw new CacheException(CacheException.INVALID_ARGS,
                      "At least one option needs to be specified");
            }

            Quota.furnishQuota(quota, custodial, output, replica);
        }

        switch (type) {
            case USER:
                if (message.isNew()) {
                    quotaSystem.createUserQuota(quota);
                } else {
                    quotaSystem.setUserQuota(quota);
                }
                break;
            case GROUP:
                if (message.isNew()) {
                    quotaSystem.createGroupQuota(quota);
                } else {
                    quotaSystem.setGroupQuota(quota);
                }
                break;
        }
    }

    private static void addQuota(Quota quota,
          Integer id,
          QuotaType type,
          PnfsManagerGetQuotaMessage message) {
        QuotaInfo info = new QuotaInfo();
        info.setCustodial(quota.getUsedCustodialSpace());
        info.setReplica(quota.getUsedReplicaSpace());
        info.setOutput(quota.getUsedOutputSpace());
        info.setCustodialLimit(quota.getCustodialSpaceLimit());
        info.setReplicaLimit(quota.getReplicaSpaceLimit());
        info.setOutputLimit(quota.getOutputSpaceLimit());
        info.setId(id);
        info.setType(type);
        message.getQuotaInfos().add(info);
    }

    private Quota getQuota(Integer qid, QuotaType type) {
        switch (type) {
            case USER:
                quotaSystem.refreshUserQuotas();
                return quotaSystem.getUserQuotas().get(qid);
            case GROUP:
                quotaSystem.refreshGroupQuotas();
                return quotaSystem.getGroupQuotas().get(qid);
            default:
                // should not occur!
                throw new RuntimeException("No such quota type " + type
                      + "; please report to dcache.org");
        }
    }

    /*
     *  ----------------------------------------------------------------------------------------
     */

    public void messageArrived(CellMessage envelope, PnfsListDirectoryMessage message)
          throws CacheException {
        String path = message.getPnfsPath();
        if (path == null) {
            throw new InvalidMessageCacheException("Missing PNFS id and path");
        }
        if (!_listQueue.offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    public void messageArrived(CellMessage envelope, PnfsMessage message)
          throws CacheException {
        PnfsId pnfsId = message.getPnfsId();
        String path = message.getPnfsPath();

        int index;
        if (pnfsId != null) {
            index = (int) (Math.abs((long) pnfsId.hashCode()) % _threads);
            LOGGER.info("Using thread [{}] {}", pnfsId, index);
        } else if (path != null) {
            if (message instanceof PnfsCreateEntryMessage && useParentHashOnCreate) {
                try {
                    FsPath parentPath = FsPath.create(path).parent();
                    index = (int) (Math.abs((long) parentPath
                          .toString()
                          .hashCode()) % _threads);
                    LOGGER.info("Using parent hash to select thread [{}] {}", path, index);
                } catch (IllegalStateException e) {
                    index = (int) (Math.abs((long) path.hashCode()) % _threads);
                    LOGGER.info("Using thread [{}] {}", path, index);
                }
            } else {
                index = (int) (Math.abs((long) path.hashCode()) % _threads);
                LOGGER.info("Using thread [{}] {}", path, index);
            }
        } else {
            index = _random.nextInt(_fifos.length);
            LOGGER.info("Using random thread {}", index);
        }

        /*
         * try to add a message into queue.
         * tell requester, that queue is full
         */
        if (!_fifos[index].offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    @VisibleForTesting
    void processPnfsMessage(CellMessage message, PnfsMessage pnfsMessage) {
        long ctime = System.currentTimeMillis();
        try {
            if (!processMessageTransactionally(message, pnfsMessage)) {
                return;
            }
        } catch (TransactionException e) {
            if (pnfsMessage.getReturnCode() == 0) {
                LOGGER.error("Name space transaction failed: {}", e.getMessage());
                pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                      "Name space transaction failed.");
            }
        }

        if (pnfsMessage.getReturnCode() == CacheException.INVALID_ARGS) {
            LOGGER.error("Inconsistent message {} received form {}",
                  pnfsMessage.getClass(), message.getSourcePath());
        }

        long duration = System.currentTimeMillis() - ctime;
        _gauges.update(pnfsMessage.getClass(), duration);
        if (_logSlowThreshold != THRESHOLD_DISABLED && duration > _logSlowThreshold) {
            LOGGER.warn("{} processed in {} ms", pnfsMessage.getClass(), duration);
        } else {
            LOGGER.info("{} processed in {} ms", pnfsMessage.getClass(), duration);
        }

        postProcessMessage(message, pnfsMessage);
    }

    @Transactional
    private boolean processMessageTransactionally(CellMessage message, PnfsMessage pnfsMessage) {
        if (pnfsMessage instanceof PnfsAddCacheLocationMessage) {
            addCacheLocation((PnfsAddCacheLocationMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsClearCacheLocationMessage) {
            clearCacheLocation((PnfsClearCacheLocationMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsGetCacheLocationsMessage) {
            getCacheLocations((PnfsGetCacheLocationsMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCreateEntryMessage) {
            createEntry((PnfsCreateEntryMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCreateUploadPath) {
            createUploadPath((PnfsCreateUploadPath) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCommitUpload) {
            commitUpload((PnfsCommitUpload) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCancelUpload) {
            cancelUpload((PnfsCancelUpload) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsDeleteEntryMessage) {
            deleteEntry((PnfsDeleteEntryMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsMapPathMessage) {
            mapPath((PnfsMapPathMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsRenameMessage) {
            rename((PnfsRenameMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsFlagMessage) {
            updateFlag((PnfsFlagMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PoolFileFlushedMessage) {
            processFlushMessage((PoolFileFlushedMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsGetParentMessage) {
            getParent((PnfsGetParentMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsListDirectoryMessage) {
            listDirectory(message, (PnfsListDirectoryMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsGetFileAttributes) {
            getFileAttributes((PnfsGetFileAttributes) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsSetFileAttributes) {
            setFileAttributes((PnfsSetFileAttributes) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsRemoveChecksumMessage) {
            removeChecksum((PnfsRemoveChecksumMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsListExtendedAttributesMessage) {
            listExtendedAttributes((PnfsListExtendedAttributesMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsReadExtendedAttributesMessage) {
            readExtendedAttributes((PnfsReadExtendedAttributesMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsWriteExtendedAttributesMessage) {
            writeExtendedAttributes((PnfsWriteExtendedAttributesMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsRemoveExtendedAttributesMessage) {
            removeExtendedAttributes((PnfsRemoveExtendedAttributesMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsRemoveLabelsMessage) {
            removeLabel((PnfsRemoveLabelsMessage) pnfsMessage);
        } else {
            LOGGER.warn("Unexpected message class [{}] from source [{}]",
                  pnfsMessage.getClass(), message.getSourcePath());
            return false;
        }
        return true;
    }

    private void postProcessMessage(CellMessage envelope, PnfsMessage message) {
        if (_attributesRelay != null &&
              message instanceof PnfsSetFileAttributes &&
              message.getReturnCode() == 0) {
            postProcessSetFileAttributes((PnfsSetFileAttributes) message);
        } /* fall through because PnfsSetFileAttributes
             is also postprocessed for locations */

        if (message instanceof PoolFileFlushedMessage && message.getReturnCode() == 0) {
            postProcessFlush(envelope, (PoolFileFlushedMessage) message);
        } else if (_cacheModificationRelay != null && message.getReturnCode() == 0) {
            postProcessLocationModificationMessage(envelope, message);
        } else if (message.getReplyRequired()) {
            envelope.revertDirection();
            sendMessage(envelope);
        }
    }

    private void postProcessSetFileAttributes(PnfsSetFileAttributes message) {
        FileAttributes attributes = message.getFileAttributes();
        if (attributes == null) {
            return;
        }
        Optional<AccessLatency> al = attributes.getAccessLatencyIfPresent();
        Optional<RetentionPolicy> rp = attributes.getRetentionPolicyIfPresent();
        if (al.isPresent() || rp.isPresent()) {
            attributes = new FileAttributes();
            attributes.setAccessLatency(al.orElse(null));
            attributes.setRetentionPolicy(rp.orElse(null));
            sendMessage(new CellMessage(_attributesRelay,
                  new PnfsSetFileAttributes(message.getPnfsId(),
                        attributes)));
        }
    }

    private void postProcessFlush(CellMessage envelope, PoolFileFlushedMessage pnfsMessage) {
        long timeout = envelope.getAdjustedTtl() - envelope.getLocalAge();

        /* Asynchronously notify flush notification targets about the flush. */
        PoolFileFlushedMessage notification =
              new PoolFileFlushedMessage(pnfsMessage.getPoolName(), pnfsMessage.getPnfsId(),
                    pnfsMessage.getFileAttributes());
        List<ListenableFuture<PoolFileFlushedMessage>> futures = new ArrayList<>();
        for (String address : _flushNotificationTargets) {
            futures.add(_stub.send(new CellPath(address), notification, timeout));
        }

        /* Only generate positive reply if all notifications succeeded. */
        Futures.addCallback(Futures.allAsList(futures),
              new FutureCallback<List<PoolFileFlushedMessage>>() {
                  @Override
                  public void onSuccess(List<PoolFileFlushedMessage> result) {
                      pnfsMessage.setSucceeded();
                      reply();
                  }

                  @Override
                  public void onFailure(Throwable t) {
                      pnfsMessage.setFailed(CacheException.DEFAULT_ERROR_CODE,
                            "PNFS manager failed while notifying other " +
                                  "components about the flush: " + t.getMessage());
                      reply();
                  }

                  private void reply() {
                      envelope.revertDirection();
                      sendMessage(envelope);
                  }
              },
              MoreExecutors.directExecutor());
    }

    public void processFlushMessage(PoolFileFlushedMessage pnfsMessage) {
        try {
            StorageInfo info = pnfsMessage.getFileAttributes().getStorageInfo();
            FileAttributes attributesToUpdate = FileAttributes.ofStorageInfo(info);
            // Note: no Restriction check as message sent autonomously by pool.
            _nameSpaceProvider.setFileAttributes(pnfsMessage.getSubject(),
                  pnfsMessage.getPnfsId(), attributesToUpdate,
                  EnumSet.noneOf(FileAttribute.class));
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            LOGGER.warn("Failed to process flush notification", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void postProcessLocationModificationMessage(CellMessage envelope,
          PnfsMessage message) {
        if (message.getReplyRequired()) {
            envelope.revertDirection();
            sendMessage(envelope);
        }

        if (message instanceof PnfsAddCacheLocationMessage) {
            PnfsMessage msg = new PnfsAddCacheLocationMessage(message.getPnfsId(),
                  ((PnfsAddCacheLocationMessage) message).getPoolName());
            sendMessage(new CellMessage(_cacheModificationRelay, msg));
        } else if (message instanceof PnfsClearCacheLocationMessage) {
            PnfsMessage msg = new PnfsClearCacheLocationMessage(message.getPnfsId(),
                  ((PnfsClearCacheLocationMessage) message).getPoolName());
            sendMessage(new CellMessage(_cacheModificationRelay, msg));
        } else if (message instanceof PnfsSetFileAttributes) {
            Collection<String> locations
                  = ((PnfsSetFileAttributes) message).getLocations();
            if (locations == null) {
                return;
            }

            PnfsId pnfsId = message.getPnfsId();
            locations.stream().forEach((pool) -> {
                PnfsMessage msg = new PnfsAddCacheLocationMessage(pnfsId,
                      pool);
                sendMessage(new CellMessage(_cacheModificationRelay, msg));
            });
        }
    }

    /**
     * Process the request to remove a checksum value from a file.
     */
    private void removeChecksum(PnfsRemoveChecksumMessage message) {
        try {
            // REVISIT: cannot enforce restriction as no path is specified.
            _nameSpaceProvider.removeChecksum(message.getSubject(),
                  message.getPnfsId(), message.getType());
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }


    private boolean useEarlyDiscard(PnfsMessage message) {
        Class<? extends PnfsMessage> msgClass = message.getClass();
        for (Class<?> c : DISCARD_EARLY) {
            if (c.equals(msgClass)) {
                return true;
            }
        }
        return false;
    }

    private void sendTimeout(CellMessage envelope, String error) {
        Message msg = (Message) envelope.getMessageObject();
        if (msg.getReplyRequired()) {
            msg.setFailed(CacheException.TIMEOUT, error);
            envelope.revertDirection();
            sendMessage(envelope);
        }
    }

    public void getFileAttributes(PnfsGetFileAttributes message) {
        try {
            checkRestriction(message, READ_METADATA);
            Subject subject = message.getSubject();
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            Set<FileAttribute> requested = message.getRequestedAttributes();
            if (message.getUpdateAtime() && _atimeGap >= 0) {
                requested.add(ACCESS_TIME);
            }
            if (requested.contains(FileAttribute.STORAGEINFO)) {
                /*
                 * TODO: The 'classic' result of getFileAttributes was a
                 * cobination of fileMetadata + storageInfo. This was
                 * used to add the owner and group information into
                 * storageInfo's internal Map. Uid and Gid are used by the
                 * HSM flush scripts.
                 *
                 * This atavism will have to be cut out when HSM
                 * interface will undestand Subject or FileAttributes
                 * will be passed to HSM interface.
                 */
                requested = EnumSet.copyOf(requested);
                requested.add(FileAttribute.OWNER);
                requested.add(FileAttribute.OWNER_GROUP);
                requested.add(FileAttribute.XATTR);
                requested.add(FileAttribute.CREATION_TIME);
            }
            FileAttributes attrs =
                  _nameSpaceProvider.getFileAttributes(subject,
                        pnfsId,
                        requested);

            if (attrs.isDefined(FileAttribute.STORAGEINFO)) {
                StorageInfo storageInfo = attrs.getStorageInfo();
                if (storageInfo.getKey("path") == null) {
                    String path = message.getPnfsPath() != null ?
                          message.getPnfsPath() : _nameSpaceProvider.pnfsidToPath(subject, pnfsId);
                    storageInfo.setKey("path", path);
                }
                storageInfo.setKey("uid", Integer.toString(attrs.getOwner()));
                storageInfo.setKey("gid", Integer.toString(attrs.getGroup()));

                // REVISIT: consider removing xattr injection once pools can accept FileAttribute.XATTR
                if (attrs.isDefined(XATTR)) {
                    attrs.getXattrs()
                          .forEach((k, v) -> storageInfo.setKey(STORAGE_INFO_XATTR_PREFIX + k, v));
                }
            }

            message.setFileAttributes(attrs);
            message.setSucceeded();
            if (message.getUpdateAtime() && _atimeGap >= 0) {
                long now = System.currentTimeMillis();
                if (attrs.getFileType() == FileType.REGULAR
                      && Math.abs(now - attrs.getAccessTime()) > _atimeGap) {
                    _nameSpaceProvider.setFileAttributes(Subjects.ROOT, pnfsId,
                          FileAttributes.ofAccessTime(now), EnumSet.noneOf(FileAttribute.class));
                }
            }
        } catch (FileNotFoundCacheException e) {
            message.setFailed(e.getRc(), e);
        } catch (CacheException e) {
            LOGGER.warn("Error while retrieving file attributes: {}", e.getMessage());
            message.setFailed(e.getRc(), e);
        } catch (RuntimeException e) {
            LOGGER.error("Error while retrieving file attributes: " + e.getMessage(), e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void setFileAttributes(PnfsSetFileAttributes message) {
        try {
            checkRestriction(message, UPDATE_METADATA);
            FileAttributes attr = message.getFileAttributes();
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            if (attr.isDefined(FileAttribute.LOCATIONS)) {
                /*
                 * Save for post-processing.
                 */
                message.setLocations(attr.getLocations());
            }

            /*
             * update ctime on atime update
             */
            if (attr.isDefined(ACCESS_TIME) && !attr.isDefined(CHANGE_TIME)) {
                attr.setChangeTime(System.currentTimeMillis());
            }

            FileAttributes updated = _nameSpaceProvider.
                  setFileAttributes(message.getSubject(),
                        pnfsId,
                        attr,
                        message.getAcquire());

            message.setFileAttributes(updated);
            message.setSucceeded();
        } catch (FileNotFoundCacheException e) {
            message.setFailed(e.getRc(), e);
        } catch (CacheException e) {
            LOGGER.warn("Error while updating file attributes: " + e.getMessage());
            message.setFailed(e.getRc(), e);
        } catch (RuntimeException e) {
            LOGGER.error("Error while updating file attributes: " + e.getMessage(), e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void listExtendedAttributes(PnfsListExtendedAttributesMessage message) {
        try {
            if (message.getFsPath() == null) {
                throw new CacheException("PNFS-ID based xattr listing is not supported");
            }

            checkRestriction(message, READ_METADATA);
            populatePnfsId(message);
            checkMask(message);

            Set<String> names = _nameSpaceProvider.listExtendedAttributes(message.getSubject(),
                  message.getFsPath());

            message.setNames(names);
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e);
        }
    }

    private void readExtendedAttributes(PnfsReadExtendedAttributesMessage message) {
        try {
            if (message.getFsPath() == null) {
                throw new CacheException("PNFS-ID based xattr reading is not supported");
            }

            checkRestriction(message, READ_METADATA);
            populatePnfsId(message);
            checkMask(message);

            FsPath path = message.getFsPath();
            for (String name : message.getAllNames()) {
                byte[] value = _nameSpaceProvider.readExtendedAttribute(message.getSubject(), path,
                      name);
                message.putValue(name, value);
            }

            message.clearNames();
            message.setSucceeded();
        } catch (CacheException e) {
            message.clear();
            message.setFailed(e.getRc(), e);
        }
    }

    private void writeExtendedAttributes(PnfsWriteExtendedAttributesMessage message) {
        try {
            if (message.getFsPath() == null) {
                throw new CacheException("PNFS-ID based xattr writing is not supported");
            }

            checkRestriction(message, UPDATE_METADATA);
            populatePnfsId(message);
            checkMask(message);

            FsPath path = message.getFsPath();

            NameSpaceProvider.SetExtendedAttributeMode m;
            switch (message.getMode()) {
                case CREATE:
                    m = NameSpaceProvider.SetExtendedAttributeMode.CREATE;
                    break;
                case MODIFY:
                    m = NameSpaceProvider.SetExtendedAttributeMode.REPLACE;
                    break;
                case EITHER:
                    m = NameSpaceProvider.SetExtendedAttributeMode.EITHER;
                    break;
                default:
                    throw new RuntimeException("Unknown mode " + message.getMode());
            }

            for (Map.Entry<String, byte[]> modification : message.getAllValues().entrySet()) {
                _nameSpaceProvider.writeExtendedAttribute(message.getSubject(), path,
                      modification.getKey(), modification.getValue(), m);
            }

            message.clearValues();
            message.setSucceeded();
        } catch (CacheException e) {
            message.clearValues();
            message.setFailed(e.getRc(), e);
        }
    }

    private void removeExtendedAttributes(PnfsRemoveExtendedAttributesMessage message) {
        try {
            if (message.getFsPath() == null) {
                throw new CacheException("PNFS-ID based xattr removal is not supported");
            }

            checkRestriction(message, UPDATE_METADATA);
            populatePnfsId(message);
            checkMask(message);

            FsPath path = message.getFsPath();

            for (String name : message.getAllNames()) {
                _nameSpaceProvider.removeExtendedAttribute(message.getSubject(),
                      path, name);
            }

            message.clearNames();
            message.setSucceeded();
        } catch (CacheException e) {
            message.clearNames();
            message.setFailed(e.getRc(), e);
        }
    }

    private void removeLabel(PnfsRemoveLabelsMessage message) {
        try {
            if (message.getFsPath() == null) {
                throw new CacheException("PNFS-ID based label removal is not supported");
            }

            checkRestriction(message, UPDATE_METADATA);
            populatePnfsId(message);

            checkMask(message);
            FsPath path = message.getFsPath();

            for (String name : message.getLabels()) {
                _nameSpaceProvider.removeLabel(message.getSubject(),
                      path, name);

            }
            message.clearLabel();
            message.setSucceeded();

        } catch (CacheException e) {
            message.clearLabel();
            message.setFailed(e.getRc(), e);
        }

    }

    private PnfsId populatePnfsId(PnfsMessage message)
          throws CacheException {
        PnfsId pnfsId = message.getPnfsId();
        if (pnfsId == null) {
            String path = message.getPnfsPath();
            if (path == null) {
                throw new InvalidMessageCacheException("no pnfsid or path defined");
            }

            pnfsId = _nameSpaceProvider.pathToPnfsid(message.getSubject(), path,
                  message.isFollowSymlink());
            message.setPnfsId(pnfsId);
        }
        return pnfsId;
    }

    /**
     * Checks the access mask for a given message.
     */
    private void checkMask(PnfsMessage message)
          throws CacheException {
        if (message.getPnfsId() != null) {
            checkMask(message.getSubject(), message.getPnfsId(), message.getAccessMask());
        } else {
            checkMask(message.getSubject(), message.getPnfsPath(), message.getAccessMask());
        }
    }

    /**
     * Checks an access mask.
     */
    private void checkMask(Subject subject, PnfsId pnfsId, Set<AccessMask> mask)
          throws CacheException {
        if (!Subjects.isExemptFromNamespaceChecks(subject) && !mask.isEmpty()) {
            Set<FileAttribute> required =
                  _permissionHandler.getRequiredAttributes();
            FileAttributes attributes =
                  _nameSpaceProvider.getFileAttributes(subject, pnfsId, required);
            if (!checkMask(subject, mask, attributes)) {
                throw new PermissionDeniedCacheException("Access denied");
            }
        }
    }

    /**
     * Checks an access mask.
     */
    private void checkMask(Subject subject, String path, Set<AccessMask> mask)
          throws CacheException {
        if (!Subjects.isExemptFromNamespaceChecks(subject) && !mask.isEmpty()) {
            Set<FileAttribute> required =
                  _permissionHandler.getRequiredAttributes();
            PnfsId pnfsId = _nameSpaceProvider.pathToPnfsid(ROOT, path, false);
            FileAttributes attributes =
                  _nameSpaceProvider.getFileAttributes(subject, pnfsId, required);
            if (!checkMask(subject, mask, attributes)) {
                throw new PermissionDeniedCacheException("Access denied");
            }
        }
    }

    /**
     * Checks whether a subject has a certain set of access right to a file system object.
     *
     * @param subject The Subject for which to check access rights
     * @param mask    The access right to check
     * @param attr    The FileAttributes of the object to check access rights to
     * @return true if subject has all access rights in mask, false otherwise
     */
    private boolean checkMask(Subject subject,
          Set<AccessMask> mask,
          FileAttributes attr) {
        AccessType access = ACCESS_ALLOWED;
        for (AccessMask m : mask) {
            switch (m) {
                case READ_DATA:
                    access =
                          access.and(_permissionHandler.canReadFile(subject, attr));
                    break;
                case LIST_DIRECTORY:
                    access =
                          access.and(_permissionHandler.canListDir(subject, attr));
                    break;
                case WRITE_DATA:
                    access =
                          access.and(_permissionHandler.canWriteFile(subject, attr));
                    break;
                case ADD_FILE:
                    access =
                          access.and(_permissionHandler.canCreateFile(subject, attr));
                    break;
                case APPEND_DATA:
                    /* Doesn't make much sense in dCache at the moment, so
                     * we simply translate this to WRITE_DATA.
                     */
                    access =
                          access.and(_permissionHandler.canWriteFile(subject, attr));
                    break;
                case ADD_SUBDIRECTORY:
                    access =
                          access.and(_permissionHandler.canCreateSubDir(subject, attr));
                    break;
                case EXECUTE:
                    /* Doesn't make sense for files in dCache, but for
                     * directories this is the lookup permission.
                     */
                    access =
                          access.and(_permissionHandler.canLookup(subject, attr));
                    break;

                case READ_ATTRIBUTES:
                case WRITE_ATTRIBUTES:
                case READ_ACL:
                case WRITE_ACL:
                case WRITE_OWNER:
                case READ_NAMED_ATTRS:
                case WRITE_NAMED_ATTRS:
                case DELETE:
                case DELETE_CHILD:
                case SYNCHRONIZE:
                    /* These attributes are either unsupported in dCache
                     * or not readily accessible through the current
                     * PermissionHandler interface.
                     */
                    access = access.and(ACCESS_UNDEFINED);
                    break;
            }
            if (access == ACCESS_DENIED) {
                return false;
            }
        }
        return (access == ACCESS_ALLOWED);
    }

    private static Activity toActivity(AccessMask mask) {
        switch (mask) {
            case READ_DATA:
                return DOWNLOAD;
            case LIST_DIRECTORY:
                return LIST;
            case WRITE_DATA:
            case APPEND_DATA:
                return UPLOAD;
            case ADD_FILE:
            case ADD_SUBDIRECTORY:
                return MANAGE;
            case DELETE_CHILD:
            case DELETE:
                return DELETE;
            case READ_NAMED_ATTRS:
            case EXECUTE:
            case READ_ATTRIBUTES:
            case READ_ACL:
                return READ_METADATA;
            case WRITE_NAMED_ATTRS:
            case WRITE_ATTRIBUTES:
            case WRITE_ACL:
            case WRITE_OWNER:
            case SYNCHRONIZE:
                return UPDATE_METADATA;
        }
        throw new RuntimeException("Unexpected AccessMask: " + mask);
    }

    private static void checkRestrictionOnParent(PnfsMessage message, Activity activity)
          throws PermissionDeniedCacheException {
        if (!Subjects.isRoot(message.getSubject())) {
            FsPath path = message.getFsPath();
            if (path != null && !path.isRoot()) {
                checkRestriction(message.getRestriction(), message.getAccessMask(), activity,
                      path.parent());
            }
        }
    }

    private static void checkRestriction(PnfsMessage message, Activity activity)
          throws PermissionDeniedCacheException {
        if (!Subjects.isRoot(message.getSubject())) {
            FsPath path = message.getFsPath();
            if (path != null) {
                checkRestriction(message.getRestriction(), message.getAccessMask(),
                      activity, path);
            } else {
                LOGGER.warn(
                      "Restriction check by-passed due to missing path; please report this to <support@dCache.org>");
            }
        }
    }

    private static void checkRestriction(PnfsMessage message, Activity activity,
          FsPath path) throws PermissionDeniedCacheException {
        if (!Subjects.isRoot(message.getSubject())) {
            checkRestriction(message.getRestriction(), message.getAccessMask(),
                  activity, path);
        }
    }

    private static void checkRestriction(Restriction restriction, Set<AccessMask> mask,
          Activity activity, FsPath path) throws PermissionDeniedCacheException {
        if (mask.stream()
              .map(PnfsManagerV3::toActivity)
              .anyMatch(a -> restriction.isRestricted(a, path))) {

            Set<AccessMask> denied = mask.stream()
                  .filter(m -> restriction.isRestricted(toActivity(m), path))
                  .collect(Collectors.toSet());

            throw new PermissionDeniedCacheException(
                  "Restriction " + restriction + " denied access for " + denied + " on " + path);
        }

        if (restriction.isRestricted(activity, path)) {
            throw new PermissionDeniedCacheException(
                  "Restriction " + restriction + " denied activity " + activity + " on " + path);
        }
    }

    /**
     * This function to be run periodically to update FS stat cache
     */
    public void updateFsStat() throws CacheException {
        _nameSpaceProvider.updateFsStat();
    }
}
