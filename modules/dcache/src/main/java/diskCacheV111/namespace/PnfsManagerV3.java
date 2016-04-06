package diskCacheV111.namespace;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.annotation.Transactional;

import javax.security.auth.Subject;

import java.io.File;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PnfsAddCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCancelUpload;
import diskCacheV111.vehicles.PnfsClearCacheLocationMessage;
import diskCacheV111.vehicles.PnfsCommitUpload;
import diskCacheV111.vehicles.PnfsCreateDirectoryMessage;
import diskCacheV111.vehicles.PnfsCreateEntryMessage;
import diskCacheV111.vehicles.PnfsCreateUploadPath;
import diskCacheV111.vehicles.PnfsDeleteEntryMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
import diskCacheV111.vehicles.PnfsModifyCacheLocationMessage;
import diskCacheV111.vehicles.PnfsRenameMessage;
import diskCacheV111.vehicles.PnfsSetChecksumMessage;
import diskCacheV111.vehicles.PoolFileFlushedMessage;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import dmg.cells.nucleus.AbstractCellComponent;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.CommandLine;
import dmg.util.command.Option;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.auth.attributes.Activity;
import org.dcache.auth.attributes.Restriction;
import org.dcache.auth.attributes.Restrictions;
import org.dcache.cells.CellStub;
import org.dcache.chimera.UnixPermission;
import org.dcache.commons.stats.RequestExecutionTimeGauges;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.PermissionHandler;
import org.dcache.util.Args;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;
import org.dcache.util.MathUtils;
import org.dcache.util.PrefixMap;
import org.dcache.vehicles.FileAttributes;
import org.dcache.vehicles.PnfsCreateSymLinkMessage;
import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import org.dcache.vehicles.PnfsRemoveChecksumMessage;
import org.dcache.vehicles.PnfsSetFileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.namespace.FileAttribute.*;
import static org.dcache.acl.enums.AccessType.*;
import static org.dcache.auth.attributes.Activity.*;

public class PnfsManagerV3
    extends AbstractCellComponent
    implements CellCommandListener, CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(PnfsManagerV3.class);

    private static final int THRESHOLD_DISABLED = 0;

    private static final CellMessage SHUTDOWN_SENTINEL = new CellMessage();

    private final Random _random = new Random(System.currentTimeMillis());

    private final RequestExecutionTimeGauges<Class<? extends PnfsMessage>> _gauges =
        new RequestExecutionTimeGauges<>("PnfsManagerV3");

    /**
     * Cache of path prefix to database IDs mappings.
     */
    private final PrefixMap<Integer> _pathToDBCache = new PrefixMap<>();

    /**
     * These messages are subject to being discarded if their time to
     * live has been exceeded (or is expected to be exceeded).
     */
    private final Class<?>[] DISCARD_EARLY = {
        PnfsGetCacheLocationsMessage.class,
        PnfsMapPathMessage.class,
        PnfsGetParentMessage.class,
        PnfsCreateEntryMessage.class,
        PnfsCreateDirectoryMessage.class,
        PnfsCreateUploadPath.class,
        PnfsGetFileAttributes.class,
        PnfsListDirectoryMessage.class
    };

    private int _threads;
    private int _threadGroups;
    private int _directoryListLimit;
    private int _queueMaxSize;
    private int _listThreads;
    private long _logSlowThreshold;

    /**
     * Queues for list operations. There is one queue per thread
     * group.
     */
    private BlockingQueue<CellMessage>[] _listQueues;

    /**
     * Queues for non-listing messages. There is one queue per thread
     * group.
     */
    private BlockingQueue<CellMessage>[] _fifos;

    /**
     * Executor for ProcessThread instances.
     */
    private ExecutorService executor =
            Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("proc-%d").build());

    private CellPath _cacheModificationRelay;

    private PermissionHandler _permissionHandler;
    private NameSpaceProvider _nameSpaceProvider;

    /**
     * A value of difference in seconds which controls file's access time
     * updates.
     */
    private long _atimeGap;

    private CellStub _stub;

    private List<String> _flushNotificationTargets;

    private void populateRequestMap()
    {
        _gauges.addGauge(PnfsAddCacheLocationMessage.class);
        _gauges.addGauge(PnfsClearCacheLocationMessage.class);
        _gauges.addGauge(PnfsGetCacheLocationsMessage.class);
        _gauges.addGauge(PnfsCreateDirectoryMessage.class);
        _gauges.addGauge(PnfsCreateEntryMessage.class);
        _gauges.addGauge(PnfsDeleteEntryMessage.class);
        _gauges.addGauge(PnfsMapPathMessage.class);
        _gauges.addGauge(PnfsRenameMessage.class);
        _gauges.addGauge(PnfsFlagMessage.class);
        _gauges.addGauge(PnfsSetChecksumMessage.class);
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
    }

    public PnfsManagerV3()
    {
        populateRequestMap();
    }

    @Required
    public void setThreads(int threads)
    {
        _threads = threads;
    }

    @Required
    public void setListThreads(int threads)
    {
        _listThreads = threads;
    }

    @Required
    public void setThreadGroups(int threadGroups)
    {
        _threadGroups = threadGroups;
    }

    @Required
    public void setCacheModificationRelay(String path)
    {
        _cacheModificationRelay =
            Strings.isNullOrEmpty(path) ? null : new CellPath(path);
        _log.info("CacheModificationRelay = {}",
                  (_cacheModificationRelay == null) ? "NONE" : _cacheModificationRelay.toString());
    }

    @Required
    public void setLogSlowThreshold(long threshold)
    {
        _logSlowThreshold = threshold;
        _log.info("logSlowThreshold {}",
                  (_logSlowThreshold == THRESHOLD_DISABLED) ? "NONE" : String.valueOf(_logSlowThreshold));
    }

    @Required
    public void setPermissionHandler(PermissionHandler handler)
    {
        _permissionHandler = handler;
    }

    @Required
    public void setNameSpaceProvider(NameSpaceProvider provider)
    {
        _nameSpaceProvider = provider;
    }

    @Required
    public void setQueueMaxSize(int maxSize)
    {
        _queueMaxSize = maxSize;
    }

    @Required
    public void setDirectoryListLimit(int limit)
    {
        _directoryListLimit = limit;
    }

    @Required
    public void setAtimeGap(long gap) {
        _atimeGap = TimeUnit.SECONDS.toMillis(gap);
    }

    @Required
    public void setFlushNotificationTarget(String target)
    {
        _flushNotificationTargets = Splitter.on(",").omitEmptyStrings().splitToList(target);
    }

    public void init()
    {
        _stub = new CellStub(getCellEndpoint());

        _fifos = new BlockingQueue[_threadGroups];
        _log.info("Starting {} threads", _threads * _threadGroups);
        for (int i = 0; i < _threadGroups; i++) {
            if (_queueMaxSize > 0) {
                _fifos[i] = new LinkedBlockingQueue<>(_queueMaxSize);
            } else {
                _fifos[i] = new LinkedBlockingQueue<>();
            }
            for (int j = 0; j < _threads; j++) {
                executor.execute(new ProcessThread(_fifos[i]));
            }
        }

        _listQueues = new BlockingQueue[_threadGroups];
        for (int i = 0; i < _threadGroups; i++) {
            _listQueues[i] = new LinkedBlockingQueue<>();
            for (int j = 0; j < _listThreads; j++) {
                executor.execute(new ProcessThread(_listQueues[i]));
            }
        }
    }

    public void shutdown() throws InterruptedException
    {
        drainQueues(_fifos);
        drainQueues(_listQueues);
        MoreExecutors.shutdownAndAwaitTermination(executor, 1, TimeUnit.SECONDS);
    }

    private void drainQueues(BlockingQueue<CellMessage>[] queues)
    {
        String error = "Name space is shutting down.";

        for (BlockingQueue<CellMessage> queue : queues) {
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
    }

    @Override
    public void getInfo( PrintWriter pw ){
        pw.print("atime precision: ");
        if (_atimeGap < 0 ) {
            pw.println("Disabled");
        } else {
            pw.println(TimeUnit.MILLISECONDS.toSeconds(_atimeGap));
        }
        pw.println();
        pw.println("List queues (" + _listQueues.length + ")");
        for (int i = 0; i < _listQueues.length; i++) {
            pw.println("    [" + i + "] " + _listQueues[i].size());
        }
        pw.println();
        pw.println("Message queues (" + _fifos.length + ")");
        for (int i = 0; i < _fifos.length; i++) {
            pw.println( "    [" + i + "] " +  _fifos[i].size());
        }
        pw.println();

        pw.println( "Statistics:" ) ;
        pw.println(_gauges.toString());
    }

    public static final String hh_pnfsidof     = "<globalPath>" ;
    public String ac_pnfsidof_$_1( Args args )
    {
        PnfsId pnfsId;
        StringBuilder sb = new StringBuilder();
        try {
            pnfsId = pathToPnfsid(ROOT, args.argv(0), false);
            sb.append(pnfsId.toString());
        }catch(Exception e){
            sb.append("pnfsidof failed:").append(e.getMessage());
        }

        return sb.toString();
    }

    public static final String hh_cacheinfoof = "<pnfsid>|<globalPath>" ;
    public String ac_cacheinfoof_$_1( Args args )
    {
        PnfsId    pnfsId;
        StringBuilder sb = new StringBuilder();
        try {
            try{

                pnfsId   = new PnfsId( args.argv(0) ) ;

            }catch(Exception ee ){
                pnfsId = pathToPnfsid(ROOT, args.argv(0), true );
            }

            List<String> locations = _nameSpaceProvider.getCacheLocation(ROOT, pnfsId);

            for ( String location: locations ) {
                sb.append(" ").append(location);
            }

            sb.append("\n");
        }catch(Exception e) {
            sb.append("cacheinfoof failed: ").append(e.getMessage());
        }
        return sb.toString();
    }

    public static final String hh_pathfinder = "<pnfsId>" ;
    public String ac_pathfinder_$_1( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        return _nameSpaceProvider.pnfsidToPath(ROOT, pnfsId);
    }

    @Command(name = "set meta",
             hint = "set the meta-data of a file",
             description = "Set the meta-data including: new owner, group, and permissions. " +
                           "Returns 'OK' if the meta-data has been set successfully.")
    public class SetMetaCommand implements Callable<String>
    {
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
                  usage = "The file access permissions mode."+
                          "Only in decimal mode for example: set meta <pnfsid> <uid> <gid> 644(rw-r--r--).")
        String perm;

        @Override
        public String call() throws CacheException
        {

            PnfsId pnfsId;
            if (PnfsId.isValid(pnfsidOrPath)) {
                pnfsId = new PnfsId(pnfsidOrPath);
            } else {
                pnfsId = pathToPnfsid(ROOT, pnfsidOrPath, true);
            }

            FileAttributes attributes = new FileAttributes();
            attributes.setOwner(uid);
            attributes.setGroup(gid);
            attributes.setMode(Integer.parseInt(perm, 8));

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
                           "\t         \tCan be overwritten by parent directory tag (hsmType)\n\n"+
                           "\t accessLatency: \tfile's access latency (e.g. ONLINE/NEARLINE)\n" +
                           "\t retentionPolicy: \tfile's retention policy (e.g. CUSTODIAL/REPLICA).\n\n" +
                           "The next returned information is HSM specific. For OSM the following storage information" +
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

    public class StorageinfoofCommand implements Callable<String>
    {
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
        public String call() throws CacheException
        {
            PnfsId    pnfsId;
            StringBuilder sb = new StringBuilder() ;

            if (PnfsId.isValid(pnfsidOrPath)) {
                pnfsId = new PnfsId(pnfsidOrPath);
                if (verbose) {
                    sb.append("PnfsId : ").append(pnfsId).append("\n");
                }
            }else {
                pnfsId = pathToPnfsid(ROOT, pnfsidOrPath, noLinks );
                    if (verbose) {
                        sb.append("       Path : ").append(pnfsidOrPath)
                                .append("\n");
                        sb.append("       PnfsId : ").append(pnfsId).append("\n");
                    }
            }

            FileAttributes attributes =
                    _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                            EnumSet.of(FileAttribute.STORAGEINFO, FileAttribute.ACCESS_LATENCY,
                                    FileAttribute.RETENTION_POLICY,  FileAttribute.SIZE));

            StorageInfo info = StorageInfos.extractFrom(attributes);
            if(verbose) {
                sb.append(" Storage Info : ").append(info).append("\n") ;
            }else{
                sb.append(info).append("\n");
            }

            return sb.toString() ;
        }
    }

    public static final String fh_metadataof =
        "   metadataof <pnfsid>|<globalPath> [-v] [-n]\n"+
        "        -v    verbose\n"+
        "        -n    don't resolve links\n";
    public static final String hh_metadataof = "<pnfsid>|<globalPath> [-v] [-n]";
    public String ac_metadataof_$_1( Args args )
    {
        PnfsId    pnfsId;
        StringBuilder sb = new StringBuilder() ;

        boolean v = args.hasOption("v") ;
        boolean n = args.hasOption("n") ;

        try{
            try{
                pnfsId = new PnfsId( args.argv(0) ) ;
                if(v) {
                    sb.append("PnfsId : ").append(pnfsId).append("\n");
                }
            }catch(Exception ee ){
                pnfsId = pathToPnfsid(ROOT, args.argv(0) , n ) ;

                if(v) {
                    sb.append("   Local Path : ").append(args.argv(0))
                            .append("\n");
                }
                if(v) {
                    sb.append("       PnfsId : ").append(pnfsId).append("\n");
                }
            }

            FileAttributes fileAttributes = _nameSpaceProvider
                    .getFileAttributes(ROOT, pnfsId, EnumSet.of(OWNER, OWNER_GROUP, MODE, TYPE,
                            CREATION_TIME, ACCESS_TIME, MODIFICATION_TIME));

            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX");
            StringBuilder info = new StringBuilder();
            switch (fileAttributes.getFileType()) {
            case DIR:
                info.append("d");
                break;
            case LINK:
                info.append("l");
                break;
            case REGULAR:
                info.append("-");
                break;
            default:
                info.append("x");
                break;
            }
            info.append(new UnixPermission(fileAttributes.getMode()).toString().substring(1));
            info.append(";").append(fileAttributes.getOwner());
            info.append(";").append(fileAttributes.getGroup());
            info.append("[c=").append(formatter.format(fileAttributes.getCreationTime()));
            info.append(";m=").append(formatter.format(fileAttributes.getModificationTime()));
            info.append(";a=").append(formatter.format(fileAttributes.getAccessTime())).append("]");

            if(v){
                sb.append("    Meta Data : ").append(info).append("\n") ;
            }else{
                sb.append(info).append("\n");
            }
        }catch(Exception ee ){
            sb.append("matadataof failed : ").append(ee.getMessage());
        }
        return sb.toString() ;
    }

    @Command(name = "flags set", allowAnyOption = true, hint = "set flags",
            description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                          "flags. This command allows one or more flags to be set on a file.")
    class FlagsSetCommand implements Callable<String>
    {
        @Argument(valueSpec = "PATH|PNFSID -FLAG=VALUE...")
        PnfsIdOrPath file;

        @CommandLine
        Args args;

        @Override
        public String call() throws CacheException
        {
            FileAttributes attributes = new FileAttributes();
            attributes.setFlags(args.optionsAsMap());
            _nameSpaceProvider.setFileAttributes(ROOT, file.toPnfsId(_nameSpaceProvider), attributes,
                                                 EnumSet.noneOf(FileAttribute.class));
            return "";
        }
    }

    @Command(name = "flags remove", allowAnyOption = true, hint = "clear flags",
            description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                          "flags. This command allows one or more flags to be cleared on a file.")
    class FlagsRemoveCommand implements Callable<String>
    {
        @Argument(valueSpec = "PATH|PNFSID -FLAG...")
        PnfsIdOrPath file;

        @CommandLine
        Args args;

        @Override
        public String call() throws CacheException
        {
            PnfsId pnfsId = file.toPnfsId(_nameSpaceProvider);

            for (String flag : args.options().keySet()) {
                _nameSpaceProvider.removeFileAttribute(ROOT, pnfsId, flag);
            }
            return "";
        }
    }

    @Command(name = "flags ls", allowAnyOption = true, hint = "list flags",
            description = "Files in dCache can be associated with arbitrary key-value pairs called " +
                          "flags. This command lists the flags of a file.")
    class FlagsListCommand implements Callable<String>
    {
        @Argument(valueSpec = "PATH|PNFSID")
        PnfsIdOrPath file;

        @Override
        public String call() throws Exception
        {
            FileAttributes attributes =
                    _nameSpaceProvider.getFileAttributes(ROOT, file.toPnfsId(_nameSpaceProvider),
                                                         EnumSet.of(FileAttribute.FLAGS));
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String,String> e: attributes.getFlags().entrySet()) {
                sb.append("-").append(e.getKey()).append("=").append(e.getValue()).append("\n");
            }
            return sb.toString();
        }
    }
    public static final String fh_dumpthreadqueues = "   dumpthreadqueues [<threadId>]\n"
        + "        dumthreadqueus prints the context of\n"
        + "        thread[s] queue[s] into the error log file";

    public static final String hh_dumpthreadqueues = "[<group>]";

    public String ac_dumpthreadqueues_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            int group = Integer.parseInt(args.argv(0));
            dumpThreadQueue(group);
            return "dumped";
        }
        for (int group = 0; group < _fifos.length; ++group) {
            dumpThreadQueue(group);
        }
        return "dumped";
    }


    @Command(name = "set file size",
             hint = "changes registered file size",
             description = "Updates the file's size in the namespace. This command has no effect on\n" +
                           "the data stored on pools or on tape.")
    public class SetFileSizeCommand implements Callable<String>
    {
        @Argument(index = 0,
                  usage = "The unique identifier of the file within dCache.")
        PnfsId pnfsId;

        @Argument(index = 1,
                  usage = "If the value does not match the size of the stored data" +
                          "then the file may become unavailable. Use with caution!")
        String newsize;

        @Override
        public String call() throws CacheException
        {
            FileAttributes attributes = new FileAttributes();
            attributes.setSize(Long.parseLong(newsize));

            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                    EnumSet.noneOf(FileAttribute.class));

            return "";
        }
    }

    public static final String hh_add_file_cache_location = "<pnfsid> <pool name>";
    public String ac_add_file_cache_location_$_2(Args args) throws Exception {

        PnfsId pnfsId = new PnfsId( args.argv(0));
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

        PnfsId pnfsId = new PnfsId( args.argv(0));
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
    public class AddFileChecksumCommand implements Callable<String>
    {
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
        public String call() throws CacheException
        {

            Checksum checksum = new Checksum(type, checksumValue);
            FileAttributes attributes = new FileAttributes();
            attributes.setChecksums(Collections.singleton(checksum));
            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                    EnumSet.noneOf(FileAttribute.class));
            return "";
        }
    }

    @Command(name = "clear file checksum",
             hint = "clears existing checksum for the file",
             description = "Clears checksum value storage for the specific file and " +
                     "checksum type.")
    public class ClearFileChecksumCommand implements Callable<String>
    {
        @Argument(index = 0,
                  usage = "The unique identifier of the file within dCache system.")
        PnfsId pnfsId;

        @Argument(index = 1,
                  usage = "The checksums type of the file. These following checksums " +
                          "are supported: adler32, md5 and md4.")
        ChecksumType type;

        @Override
        public String call() throws CacheException
        {
            _nameSpaceProvider.removeChecksum(ROOT, pnfsId, type);
            return "";
        }
    }

    @Command(name = "get file checksum",
             hint = "returns file checksum",
             description = "Display the checksum corresponding to the specified file and " +
                     "the checksum type. Nothing is returned if there is no corresponding checksum.")
    public class GetFileChecksumCommand implements Callable<String>
    {
        @Argument(index = 0,
                  usage = "The unique identifier of the file.")
        PnfsId pnfsId;

        @Argument(index = 1,
                  usage = "The checksums type of the file. These following checksums " +
                          "are supported: adler32, md5 and md4.")
        ChecksumType type;

        @Override
        public String call() throws CacheException, NoSuchAlgorithmException
        {
            Checksum checksum = getChecksum(ROOT, pnfsId, type);
            return (checksum == null) ? "" : checksum.toString();
        }
    }

    @Command(name = "set log slow threshold",
             hint = "set the threshold for reporting slow PNFS interactions",
             description = "Enable reporting of slow PNFS interactions." +
                           " If the interaction <timeout> is greater than the timeout specified by this command," +
                           " a warning message is logged.")
    public class SetLogLowThresholdCommand implements Callable<String>
    {
        @Argument(usage = "Time in milliseconds, must be greater than zero.")
        String timeout;

        @Override
        public String call() throws NumberFormatException
        {
            int newTimeout;
            try {
                newTimeout = Integer.parseInt( timeout);
            } catch ( NumberFormatException e) {
                throw new NumberFormatException("Badly formatted number " + timeout);
            }

            if( newTimeout <= 0) {
                return "Timeout must be greater than zero.";
            }

            _logSlowThreshold = newTimeout;

            return "";
        }
    }

    @Command(name = "get log slow threshold",
             hint = "return the current threshold for reporting slow PNFS interactions",
             description = "If the threshold disabled returns" + " \"disabled\" " +"otherwise returns " +
                           "the set time in ms.")
    public class GetLogSlowThresholdCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            if( _logSlowThreshold == THRESHOLD_DISABLED) {
                return "disabled";
            }
            return String.valueOf(_logSlowThreshold) + " ms";
        }
    }

    @Command(name = "set log slow threshold disabled",
             hint = "disable reporting of slow PNFS interactions",
             description = "No warning messages are logged.")
    public class SetLogSlowThresfoldDisabledCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            _logSlowThreshold = THRESHOLD_DISABLED;
            return "";
        }

    }

    public static final String fh_show_path_cache =
        "Shows cached information about mappings from path prefixes to\n" +
        "name space database IDs. The cache is only populated if the\n" +
        "number of thread groups is larger than 1.";
    public String ac_show_path_cache(Args args)
    {
        StringBuilder s = new StringBuilder();
        for (Map.Entry<FsPath,Integer> e: _pathToDBCache.entrySet()) {
            s.append(String.format("%s -> %d\n", e.getKey(), e.getValue()));
        }
        return s.toString();
    }

    private void dumpThreadQueue(int queueId) {
        if (queueId < 0 || queueId >= _fifos.length) {
            throw new IllegalArgumentException(" illegal queue #" + queueId);
        }
        BlockingQueue<CellMessage> fifo = _fifos[queueId];
        Object[] fifoContent = fifo.toArray();

        _log.warn("PnfsManager thread #" + queueId + " queue dump (" +fifoContent.length+ "):");

        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < fifoContent.length; i++) {
            sb.append("fifo[").append(i).append("] : ");
            sb.append(fifoContent[i]).append('\n');
        }

        _log.warn( sb.toString() );
    }

    private Checksum getChecksum(Subject subject, PnfsId pnfsId,
                                 ChecksumType type)
        throws CacheException, NoSuchAlgorithmException
    {
        ChecksumFactory factory = ChecksumFactory.getFactory(type);
        FileAttributes attributes =
            _nameSpaceProvider.getFileAttributes(subject, pnfsId,
                                                 EnumSet.of(FileAttribute.CHECKSUM));
        return factory.find(attributes.getChecksums());
    }

    private void setChecksum(PnfsSetChecksumMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();
        String value     = msg.getValue() ;

        try{
            ChecksumType type = ChecksumType.getChecksumType(msg.getType());
            Checksum checksum = new Checksum(type, value);
            FileAttributes attributes = new FileAttributes();
            attributes.setChecksums(Collections.singleton(checksum));
            _nameSpaceProvider.setFileAttributes(msg.getSubject(), pnfsId,
                    attributes, EnumSet.noneOf(FileAttribute.class));
        }catch(FileNotFoundCacheException e) {
            msg.setFailed(CacheException.FILE_NOT_FOUND, e.getMessage() );
        }catch( CacheException e ){
            _log.warn("Unxpected CacheException: " + e);
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            _log.warn(e.toString()) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }

    }


    private void updateFlag(PnfsFlagMessage pnfsMessage){

        PnfsId pnfsId    = pnfsMessage.getPnfsId();
        PnfsFlagMessage.FlagOperation operation = pnfsMessage.getOperation() ;
        String flagName  = pnfsMessage.getFlagName() ;
        String value     = pnfsMessage.getValue() ;
        Subject subject = pnfsMessage.getSubject();
        _log.info("update flag "+operation+" flag="+flagName+" value="+
                  value+" for "+pnfsId);

        try{
            // Note that dcap clients may bypass restrictions by not
            // specifying a path when interacting via mounted namespace.
            checkRestriction(pnfsMessage, UPDATE_METADATA);
            if( operation == PnfsFlagMessage.FlagOperation.GET ){
                pnfsMessage.setValue( updateFlag(subject, pnfsId , operation , flagName , value ) );
            }else{
                updateFlag(subject, pnfsId , operation , flagName , value );
            }

        } catch (FileNotFoundCacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (CacheException e) {
            _log.warn("Exception in updateFlag: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Exception in updateFlag", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private String updateFlag(Subject subject, PnfsId pnfsId,
                              PnfsFlagMessage.FlagOperation operation,
                              String flagName, String value)
        throws CacheException
    {
        FileAttributes attributes;
        switch (operation) {
        case SET:
            _log.info("flags set " + pnfsId + " " + flagName + "=" + value);
            attributes = new FileAttributes();
            attributes.setFlags(Collections.singletonMap(flagName, value));
            _nameSpaceProvider.setFileAttributes(subject, pnfsId, attributes,
                    EnumSet.noneOf(FileAttribute.class));
            break;
        case SETNOOVERWRITE:
            _log.info("flags set (dontoverwrite) " + pnfsId + " " + flagName + "=" + value);
            attributes = _nameSpaceProvider.getFileAttributes(subject, pnfsId, EnumSet.of(FileAttribute.FLAGS));
            String current = attributes.getFlags().get(flagName);
            if ((current == null) || (!current.equals(value))) {
                updateFlag(subject, pnfsId, PnfsFlagMessage.FlagOperation.SET,
                           flagName, value);
            }
            break;
        case GET:
            attributes = _nameSpaceProvider.getFileAttributes(subject, pnfsId, EnumSet.of(FileAttribute.FLAGS));
            String v = attributes.getFlags().get(flagName);
            _log.info("flags ls " + pnfsId + " " + flagName + " -> " + v);
            return v;
        case REMOVE:
            _log.info("flags remove " + pnfsId + " " + flagName);
            _nameSpaceProvider.removeFileAttribute(subject, pnfsId, flagName);
            break;
        }
        return null;
    }

    public void addCacheLocation(PnfsAddCacheLocationMessage pnfsMessage){
        _log.info("addCacheLocation : "+pnfsMessage.getPoolName()+" for "+pnfsMessage.getPnfsId());
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
        } catch (FileNotFoundCacheException fnf ) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (CacheException e){
            _log.warn("Exception in addCacheLocation: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e){
            _log.error("Exception in addCacheLocation", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,"Exception in addCacheLocation");
        }


    }

    public void clearCacheLocation(PnfsClearCacheLocationMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        _log.info("clearCacheLocation : "+pnfsMessage.getPoolName()+" for "+pnfsId);
        try {
            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, UPDATE_METADATA);
            _nameSpaceProvider.clearCacheLocation(pnfsMessage.getSubject(),
                                                  pnfsId,
                                                  pnfsMessage.getPoolName(),
                                                  pnfsMessage.removeIfLast());
        } catch (FileNotFoundCacheException fnf ) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (CacheException e){
            _log.warn("Exception in clearCacheLocation for "+pnfsId+": "+e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e){
            _log.error("Exception in clearCacheLocation for "+pnfsId, e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage() );
        }


    }

    public void getCacheLocations(PnfsGetCacheLocationsMessage pnfsMessage){
        Subject subject = pnfsMessage.getSubject();
        try {
            PnfsId pnfsId = populatePnfsId(pnfsMessage);
            _log.info("get cache locations for " + pnfsId);

            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, READ_METADATA);
            pnfsMessage.setCacheLocations(_nameSpaceProvider.getCacheLocation(subject, pnfsId));
            pnfsMessage.setSucceeded();
        } catch (FileNotFoundCacheException fnf ) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (CacheException e){
            _log.warn("Exception in getCacheLocations: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e){
            _log.error("Exception in getCacheLocations", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  "Pnfs lookup failed");
        }


    }

    public void createLink(PnfsCreateSymLinkMessage pnfsMessage) {
        PnfsId pnfsId;
        _log.info("create symlink {} to {}", pnfsMessage.getPath(), pnfsMessage.getDestination() );
        try {
            File file = new File(pnfsMessage.getPath());
            checkMask(pnfsMessage.getSubject(), file.getParent(),
                      pnfsMessage.getAccessMask());
            checkRestrictionOnParent(pnfsMessage, MANAGE);
            pnfsId = _nameSpaceProvider.createSymLink(pnfsMessage.getSubject(),
                                                      pnfsMessage.getPath(),
                                                      pnfsMessage.getDestination(),
                                                      pnfsMessage.getUid(),
                                                      pnfsMessage.getGid());

            pnfsMessage.setPnfsId(pnfsId);
            pnfsMessage.setSucceeded();

        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Failed to create a symlink " +
                    pnfsMessage.getPath() + " to " + pnfsMessage.getDestination(), e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }

    }

    public void createDirectory(PnfsCreateDirectoryMessage pnfsMessage){
        PnfsId pnfsId;
        _log.info("create directory "+pnfsMessage.getPath());
        try {
            File file = new File(pnfsMessage.getPath());
            checkMask(pnfsMessage.getSubject(), file.getParent(),
                      pnfsMessage.getAccessMask());
            checkRestrictionOnParent(pnfsMessage, MANAGE);
            pnfsId = _nameSpaceProvider.createDirectory(pnfsMessage.getSubject(),
                                                        pnfsMessage.getPath(),
                                                        pnfsMessage.getUid(), pnfsMessage.getGid(),
                                                        pnfsMessage.getMode());

            pnfsMessage.setPnfsId(pnfsId);
            pnfsMessage.setSucceeded();

            //
            // FIXME : is it really true ?
            //
            // now we try to get the storageInfo out of the
            // parent directory. If it fails, we don't care.
            // We declare the request to be successful because
            // the createEntry seem to be ok.
            try{
                _log.info( "Trying to get storageInfo for "+pnfsId) ;

                /* If we were allowed to create the entry above, then
                 * we also ought to be allowed to read it here. Hence
                 * we use ROOT as the subject.
                 */
                Set<FileAttribute> requested =
                    pnfsMessage.getRequestedAttributes();
                FileAttributes attrs =
                    _nameSpaceProvider.getFileAttributes(ROOT,
                                                         pnfsId,
                                                         requested);
                pnfsMessage.setFileAttributes(attrs);
            } catch (CacheException e) {
                _log.warn("Can't determine storageInfo: " + e);
            }
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Failed to create directory", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }


    }

    public void createEntry(PnfsCreateEntryMessage pnfsMessage){

        _log.info("create entry "+pnfsMessage.getPath());
        try {
            File file = new File(pnfsMessage.getPath());
            checkMask(pnfsMessage.getSubject(), file.getParent(),
                      pnfsMessage.getAccessMask());
            checkRestriction(pnfsMessage, UPLOAD);

            Set<FileAttribute> requested =
                    pnfsMessage.getRequestedAttributes();
            requested.add(FileAttribute.STORAGEINFO);
            requested.add(FileAttribute.PNFSID);

            FileAttributes attrs =
                    _nameSpaceProvider.createFile(pnfsMessage.getSubject(),
                                                  pnfsMessage.getPath(),
                                                  pnfsMessage.getUid(),
                                                  pnfsMessage.getGid(),
                                                  pnfsMessage.getMode(),
                                                  requested);

            StorageInfo info = attrs.getStorageInfo();
            if (info.getKey("path") == null) {
                info.setKey("path", pnfsMessage.getPnfsPath());
            }
            info.setKey("uid", Integer.toString(pnfsMessage.getUid()));
            info.setKey("gid", Integer.toString(pnfsMessage.getGid()));

            pnfsMessage.setFileAttributes(attrs);
            pnfsMessage.setPnfsId(attrs.getPnfsId());
            pnfsMessage.setSucceeded();
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Create entry failed", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void createUploadPath(PnfsCreateUploadPath message)
    {
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
            _log.error("Create upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void commitUpload(PnfsCommitUpload message)
    {
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
            _log.error("Commit upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    void cancelUpload(PnfsCancelUpload message)
    {
        try {
            checkRestriction(message, UPLOAD);
            _nameSpaceProvider.cancelUpload(message.getSubject(), message.getUploadPath(), message.getPath());
            message.setSucceeded();
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Cancel upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void deleteEntry(PnfsDeleteEntryMessage pnfsMessage)
    {
        String path = pnfsMessage.getPnfsPath();
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        Subject subject = pnfsMessage.getSubject();
        Set<FileType> allowed = pnfsMessage.getAllowedFileTypes();

        try {
            if (path == null && pnfsId == null) {
                throw new InvalidMessageCacheException("pnfsid or path have to be defined for PnfsDeleteEntryMessage");
            }

            checkMask(pnfsMessage);
            checkRestriction(pnfsMessage, DELETE);
            if (path != null) {
                _log.info("delete PNFS entry for {}", path);
                if (pnfsId != null) {
                    _nameSpaceProvider.deleteEntry(subject, allowed, pnfsId, path);
                } else {
                    pnfsMessage.setPnfsId(_nameSpaceProvider.deleteEntry(subject, allowed, path));
                }
            } else {
                _log.info("delete PNFS entry for {}", pnfsId);
                _nameSpaceProvider.deleteEntry(subject, allowed, pnfsId);
            }

            pnfsMessage.setSucceeded();
        } catch (FileNotFoundCacheException e) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, e.getMessage());
        } catch (CacheException e) {
            _log.warn("Failed to delete entry: " + e.getMessage());
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Failed to delete entry", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void rename(PnfsRenameMessage msg)
    {
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
            _log.info("Rename {} to new name: {}", sourcePath, destinationPath);
            checkRestriction(msg, MANAGE, new FsPath(sourcePath).getParent());
            checkRestriction(msg, MANAGE, new FsPath(destinationPath).getParent());
            boolean overwrite = msg.getOverwrite()
                    && !msg.getRestriction().isRestricted(DELETE, new FsPath(destinationPath));
            _nameSpaceProvider.rename(msg.getSubject(), pnfsId, sourcePath, destinationPath, overwrite);
        } catch (CacheException e){
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Pnfs rename failed");
        }
    }

    private String pathfinder(Subject subject, PnfsId pnfsId )
        throws CacheException
    {
        return _nameSpaceProvider.pnfsidToPath(subject, pnfsId);
    }

    public void mapPath( PnfsMapPathMessage pnfsMessage ){
        PnfsId pnfsId     = pnfsMessage.getPnfsId() ;
        String globalPath = pnfsMessage.getGlobalPath() ;
        Subject subject = pnfsMessage.getSubject();
        boolean shouldResolve = pnfsMessage.shouldResolve();

        if( ( pnfsId == null ) && ( globalPath == null ) ){
            pnfsMessage.setFailed( 5 , "Illegal Arguments : need path or pnfsid" ) ;
            return ;
        }

        try {
            if (globalPath == null) {
                _log.info("map:  id2path for " + pnfsId);
                String path = pathfinder(subject, pnfsId);
                checkRestriction(pnfsMessage, READ_METADATA, new FsPath(path));
                pnfsMessage.setGlobalPath(path);
            } else {
                _log.info("map:  path2id for " + globalPath);
                checkRestriction(pnfsMessage, READ_METADATA, new FsPath(globalPath));
                pnfsMessage.setPnfsId(pathToPnfsid(subject, globalPath, shouldResolve));
            }
            checkMask(pnfsMessage);
        } catch(FileNotFoundCacheException fnf){
            pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch (CacheException ce) {
            pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
            _log.warn("mapPath: " + ce.getMessage());
        } catch (RuntimeException e) {
            _log.error("Exception in mapPath (pathfinder) " + e, e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void getParent(PnfsGetParentMessage msg)
    {
        try {
            PnfsId pnfsId = populatePnfsId(msg);
            checkMask(msg);
            msg.setParent(_nameSpaceProvider.getParentOf(msg.getSubject(), pnfsId));
        } catch (CacheException e) {
            _log.warn(e.toString());
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          e.getMessage());
        }
    }


    /**
     * PnfsListDirectoryMessages can have more than one reply. This is
     * to avoid large directories exhausting available memory in the
     * PnfsManager. In current versions of Java, the only way to list
     * a directory without building the complete result array in
     * memory is to gather the elements inside a filter.
     *
     * This filter collects entries and sends partial replies for the
     * PnfsListDirectoryMessage when a certain number of entries have
     * been collected. The filter will not send the final reply (the
     * caller has to do that).
     */
    private class ListHandlerImpl implements ListHandler
    {
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
                               long initialDelay, long delay)
        {
            _msg = msg;
            _requestor = requestor;
            _uoid = uoid;
            _delay = delay;
            _directory = checkNotNull(_msg.getFsPath());
            _subject = _msg.getSubject();
            _restriction = _msg.getRestriction();
            _deadline =
                (delay == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + initialDelay;
        }

        private void sendPartialReply()
        {
            _msg.setReply();

            CellMessage envelope = new CellMessage(_requestor, _msg);
            envelope.setLastUOID(_uoid);
            sendMessage(envelope);
            _messageCount++;

            _msg.clear();
        }

        @Override
        public void addEntry(String name, FileAttributes attrs)
        {
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

        public int getMessageCount()
        {
            return _messageCount;
        }
    }

    private void listDirectory(CellMessage envelope, PnfsListDirectoryMessage msg)
    {
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
            _nameSpaceProvider.list(msg.getSubject(), path,
                                    msg.getPattern(),
                                    msg.getRange(),
                                    msg.getRequestedAttributes(),
                                    handler);
            msg.setSucceeded(handler.getMessageCount() + 1);
        } catch (FileNotFoundCacheException | NotDirCacheException e) {
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (CacheException e) {
            _log.warn(e.toString());
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          e.getMessage());
        }
    }

    private class ProcessThread implements Runnable
    {
        private final BlockingQueue<CellMessage> _fifo;

        private ProcessThread(BlockingQueue<CellMessage> fifo)
        {
            _fifo = fifo;
        }

        @Override
        public void run()
        {
            try {
                for (CellMessage message = _fifo.take(); message != SHUTDOWN_SENTINEL; message = _fifo.take()) {
                    CDC.setMessageContext(message);
                    try {
                        /* Discard messages if we are close to their
                         * timeout (within 10% of the TTL or 10 seconds,
                         * whatever is smaller)
                         */
                        PnfsMessage pnfs = (PnfsMessage) message.getMessageObject();
                        if (message.getLocalAge() > message.getAdjustedTtl() && useEarlyDiscard(pnfs)) {
                            _log.warn("Discarding {} because its time to live has been exceeded.",
                                      pnfs.getClass().getSimpleName());
                            sendTimeout(message, "TTL exceeded");
                            continue;
                        }

                        processPnfsMessage(message, pnfs);
                    } catch (Throwable e) {
                        _log.warn("processPnfsMessage: {} : {}", Thread.currentThread().getName(), e);
                    } finally {
                        CDC.clearMessageContext();
                    }
                }
                /* Poison any other threads reading from the same queue */
                _fifo.offer(SHUTDOWN_SENTINEL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public void messageArrived(CellMessage envelope, PnfsListDirectoryMessage message)
        throws CacheException
    {
        String path = message.getPnfsPath();
        if (path == null) {
            throw new InvalidMessageCacheException("Missing PNFS id and path");
        }
        int group = pathToThreadGroup(path);
        _log.info("Using list queue [{}] {}", path, group);
        if (!_listQueues[group].offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    public void messageArrived(CellMessage envelope, PnfsModifyCacheLocationMessage message)
        throws CacheException
    {
        if (_cacheModificationRelay != null) {
            forwardModifyCacheLocationMessage(message);
        }

        PnfsId pnfsId = message.getPnfsId();
        int index = pnfsIdToThreadGroup(pnfsId);
        _log.info("Using thread group [{}] {}", pnfsId, index);

        if (!_fifos[index].offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    public void messageArrived(CellMessage envelope, PnfsMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        String path = message.getPnfsPath();

        int group;
        if (pnfsId != null) {
            group = pnfsIdToThreadGroup(pnfsId);
            _log.info("Using thread group [{}] {}", pnfsId, group);
        } else if (path != null) {
            group = pathToThreadGroup(path);
            _log.info("Using thread group [{}] {}", path, group);
        } else {
            group = _random.nextInt(_fifos.length);
            _log.info("Using random thread group {}", group);
        }

        if (!_fifos[group].offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    private void forwardModifyCacheLocationMessage(PnfsMessage message)
    {
        sendMessage(new CellMessage(_cacheModificationRelay, message));
    }

    public void processPnfsMessage(CellMessage message, PnfsMessage pnfsMessage)
    {
        long ctime = System.currentTimeMillis();
        try {
            if (!processMessageTransactionally(message, pnfsMessage)) {
                return;
            }
        } catch (TransactionException e) {
            if (pnfsMessage.getReturnCode() == 0) {
                _log.error("Name space transaction failed: {}", e.getMessage());
                pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, "Name space transaction failed.");
            }
        }

        if (pnfsMessage.getReturnCode() == CacheException.INVALID_ARGS) {
            _log.error("Inconsistent message {} received form {}",
                       pnfsMessage.getClass(), message.getSourcePath());
        }

        long duration = System.currentTimeMillis() - ctime;
        _gauges.update(pnfsMessage.getClass(), duration);
        if (_logSlowThreshold != THRESHOLD_DISABLED && duration > _logSlowThreshold) {
            _log.warn("{} processed in {} ms", pnfsMessage.getClass(), duration);
        } else {
            _log.info("{} processed in {} ms", pnfsMessage.getClass(), duration);
        }

        if (pnfsMessage.getReplyRequired()) {
            message.revertDirection();
            sendMessage(message);
        }
    }

    @Transactional
    private boolean processMessageTransactionally(CellMessage message, PnfsMessage pnfsMessage)
    {
        if (pnfsMessage instanceof PnfsAddCacheLocationMessage) {
            addCacheLocation((PnfsAddCacheLocationMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsClearCacheLocationMessage) {
            clearCacheLocation((PnfsClearCacheLocationMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsGetCacheLocationsMessage) {
            getCacheLocations((PnfsGetCacheLocationsMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCreateSymLinkMessage) {
            createLink((PnfsCreateSymLinkMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsCreateDirectoryMessage) {
            createDirectory((PnfsCreateDirectoryMessage) pnfsMessage);
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
        } else if (pnfsMessage instanceof PnfsSetChecksumMessage) {
            setChecksum((PnfsSetChecksumMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PoolFileFlushedMessage) {
            processFlushMessage(message, (PoolFileFlushedMessage) pnfsMessage);
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
        } else {
            _log.warn("Unexpected message class [{}] from source [{}]",
                      pnfsMessage.getClass(), message.getSourcePath());
            return false;
        }
        return true;
    }

    public void processFlushMessage(CellMessage envelope, PoolFileFlushedMessage pnfsMessage)
    {
        try {
            // Note: no Restriction check as message sent autonomously by pool.
            FileAttributes attributesToUpdate = new FileAttributes();
            attributesToUpdate.setStorageInfo(pnfsMessage.getFileAttributes().getStorageInfo());
            _nameSpaceProvider.setFileAttributes(pnfsMessage.getSubject(),
                                                 pnfsMessage.getPnfsId(), attributesToUpdate,
                                                 EnumSet.noneOf(FileAttribute.class));

            long timeout = envelope.getAdjustedTtl() - envelope.getLocalAge();

            /* Asynchronously notify flush notification targets about the flush. */
            PoolFileFlushedMessage notification =
                    new PoolFileFlushedMessage(pnfsMessage.getPoolName(), pnfsMessage.getPnfsId(),
                                               pnfsMessage.getFileAttributes());
            List<ListenableFuture<PoolFileFlushedMessage>> futures = new ArrayList<>();
            for (String address : _flushNotificationTargets) {
                futures.add(_stub.send(new CellPath(address), notification, timeout));
            }

            /* Prevent the caller from generating a reply. */
            pnfsMessage.setReplyRequired(false);

            /* Only generate positive reply if all notifications succeeded. */
            Futures.addCallback(Futures.allAsList(futures),
                                new FutureCallback<List<PoolFileFlushedMessage>>()
                                {
                                    @Override
                                    public void onSuccess(List<PoolFileFlushedMessage> result)
                                    {
                                        pnfsMessage.setSucceeded();
                                        reply();
                                    }

                                    @Override
                                    public void onFailure(Throwable t)
                                    {
                                        pnfsMessage.setFailed(CacheException.DEFAULT_ERROR_CODE,
                                                              "PNFS manager failed while notifying other " +
                                                              "components about the flush: " + t.getMessage());
                                        reply();
                                    }

                                    private void reply()
                                    {
                                        envelope.revertDirection();
                                        sendMessage(envelope);
                                    }
                                });
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.warn("Failed to process flush notification", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    /**
     * Process the request to remove a checksum value from a file.
     */
    private void removeChecksum(PnfsRemoveChecksumMessage message)
    {
        try {
            // REVISIT: cannot enforce restriction as no path is specified.
            _nameSpaceProvider.removeChecksum(message.getSubject(),
                    message.getPnfsId(), message.getType());
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch(RuntimeException e) {
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }


    /**
     * Maps path to PnfsId.
     *
     * Internally updates the path to database ID cache.
     */
    private PnfsId pathToPnfsid(Subject subject, String path, boolean resolve)
        throws CacheException
    {
        PnfsId pnfsId = _nameSpaceProvider.pathToPnfsid(subject, path, resolve);
        updatePathToDatabaseIdCache(path, pnfsId.getDatabaseId());
        return pnfsId;
    }

    /**
     * Adds an entry to the path to database ID cache if that entry
     * does not already exist. The cache is only populated if the
     * number of thread groups is large than 1.
     */
    private void updatePathToDatabaseIdCache(String path, int id)
    {
        try {
            if (_threadGroups > 1) {
                Integer db = _pathToDBCache.get(new FsPath(path));
                if (db == null || db != id) {
                    String root = getDatabaseRoot(new File(path)).getPath();
                    _pathToDBCache.put(new FsPath(root), id);
                    _log.info("Path cache updated: " + root + " -> " + id);
                }
            }
        } catch (Exception e) {
            /* Log it, but since it is only a cache update we don't
             * mind too much.
             */
            _log.warn("Error while resolving the database ID: " + e.getMessage());
        }
    }

    /**
     * Given a PNFS path, returns the database mount point of the
     * database containing the entry.
     */
    private File getDatabaseRoot(File file)
        throws Exception
    {
        /* First find the PNFS ID of file. May fail if the file does
         * not exist, but then we look at the parent instead.
         */
        PnfsId id;
        try {
            id = _nameSpaceProvider.pathToPnfsid(ROOT, file.getPath(), true);
        } catch (CacheException e) {
            file = file.getParentFile();
            if (file == null) {
                throw e;
            }
            return getDatabaseRoot(file);
        }

        /* Now look at the path back to the root and find the point at
         * which the database ID changes, or resolving the PNFS ID
         * fails. That point is the database mount point.
         */
        try {
            String parent = file.getParent();
            if (parent == null) {
                return file;
            }

            PnfsId parentId = _nameSpaceProvider.pathToPnfsid(ROOT, parent, true);
            while (parentId.getDatabaseId() == id.getDatabaseId()) {
                file = new File(parent);
                parent = file.getParent();
                if (parent == null) {
                    return file;
                }
                parentId = _nameSpaceProvider.pathToPnfsid(ROOT, parent, true);
            }

            return file;
        } catch (CacheException e) {
            return file;
        }
    }

    /**
     * Returns the thread group number for a path. The mapping is
     * based on the database ID of the path.  A cache is used to avoid
     * lookups in the name space provider once the path prefix for a
     * database has been determined. In case of a cache miss an
     * arbitrary but deterministic thread group is chosen.
     */
    private int pathToThreadGroup(String path)
    {
        if (_threadGroups == 1) {
            return 0;
        }

        Integer db = _pathToDBCache.get(new FsPath(path));
        if (db != null) {
            return db % _threadGroups;
        }

        _log.info("Path cache miss for " + path);

        return MathUtils.absModulo(path.hashCode(), _threadGroups);
    }

    /**
     * Returns the thread group number for a PNFS id. The mapping is
     * based on the database ID of the PNFS id.
     */
    private int pnfsIdToThreadGroup(PnfsId id)
    {
        return (id.getDatabaseId() % _threadGroups);
    }

    private boolean useEarlyDiscard(PnfsMessage message)
    {
        Class<? extends PnfsMessage> msgClass = message.getClass();
        for (Class<?> c: DISCARD_EARLY) {
            if (c.equals(msgClass)) {
                return true;
            }
        }
        return false;
    }

    private void sendTimeout(CellMessage envelope, String error)
    {
        Message msg = (Message) envelope.getMessageObject();
        if (msg.getReplyRequired()) {
            msg.setFailed(CacheException.TIMEOUT, error);
            envelope.revertDirection();
            sendMessage(envelope);
        }
    }

    public void getFileAttributes(PnfsGetFileAttributes message)
    {
        try {
            Subject subject = message.getSubject();
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            checkRestriction(message, READ_METADATA);
            Set<FileAttribute> requested = message.getRequestedAttributes();
            if (message.getUpdateAtime() && _atimeGap != -1) {
                requested.add(ACCESS_TIME);
            }
            if(requested.contains(FileAttribute.STORAGEINFO)) {
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
            }
            FileAttributes attrs =
                _nameSpaceProvider.getFileAttributes(subject,
                                                     pnfsId,
                                                     requested);

            if (attrs.isDefined(FileAttribute.STORAGEINFO)) {
                StorageInfo storageInfo = attrs.getStorageInfo();
                if (storageInfo.getKey("path") == null) {
                    storageInfo.setKey("path", message.getPnfsPath());
                }
                storageInfo.setKey("uid", Integer.toString(attrs.getOwner()));
                storageInfo.setKey("gid", Integer.toString(attrs.getGroup()));
            }

            message.setFileAttributes(attrs);
            message.setSucceeded();
            if (message.getUpdateAtime() && _atimeGap != -1) {
                long now = System.currentTimeMillis();
                if (attrs.getFileType() == FileType.REGULAR && Math.abs(now - attrs.getAccessTime()) > _atimeGap) {
                    FileAttributes atimeUpdateAttr = new FileAttributes();
                    atimeUpdateAttr.setAccessTime(now);
                    _nameSpaceProvider.setFileAttributes(Subjects.ROOT, pnfsId, atimeUpdateAttr, EnumSet.noneOf(FileAttribute.class));
                }
            }
        } catch (FileNotFoundCacheException e){
            message.setFailed(e.getRc(), e);
        } catch (CacheException e) {
            _log.warn("Error while retrieving file attributes: " + e.getMessage());
            message.setFailed(e.getRc(), e);
        } catch (RuntimeException e) {
            _log.error("Error while retrieving file attributes: " + e.getMessage(), e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void setFileAttributes(PnfsSetFileAttributes message)
    {
        try {

            FileAttributes attr = message.getFileAttributes();
            if (attr.getDefinedAttributes().size() == 1 && attr.isDefined(ACCESS_TIME) && message.getAcquire().isEmpty()) {
                // access time only update (legacy pools). We assume that the doors already have update atime.
                return;
           }
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            checkRestriction(message, UPDATE_METADATA);
            if (attr.getDefinedAttributes().contains(FileAttribute.LOCATIONS)) {
                for (String pool: attr.getLocations()) {
                    PnfsMessage msg =
                        new PnfsAddCacheLocationMessage(pnfsId, pool);
                    forwardModifyCacheLocationMessage(msg);
                }
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
        }catch(FileNotFoundCacheException e){
            message.setFailed(e.getRc(), e);
        }catch(CacheException e) {
            _log.warn("Error while updating file attributes: " + e.getMessage());
            message.setFailed(e.getRc(), e);
        }catch(RuntimeException e) {
            _log.error("Error while updating file attributes: " + e.getMessage(), e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private PnfsId populatePnfsId(PnfsMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        if (pnfsId == null) {
            String  path = message.getPnfsPath();
            if (path == null ) {
                throw new InvalidMessageCacheException("no pnfsid or path defined");
            }

            pnfsId = pathToPnfsid(message.getSubject(), path, true);
            message.setPnfsId(pnfsId);
        }
        return pnfsId;
    }

    /**
     * Checks the access mask for a given message.
     */
    private void checkMask(PnfsMessage message)
        throws CacheException
    {
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
        throws CacheException
    {
        if (!Subjects.isRoot(subject) && !mask.isEmpty()) {
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
        throws CacheException
    {
        if (!Subjects.isRoot(subject) && !mask.isEmpty()) {
            Set<FileAttribute> required =
                _permissionHandler.getRequiredAttributes();
            PnfsId pnfsId = pathToPnfsid(ROOT, path, false);
            FileAttributes attributes =
                _nameSpaceProvider.getFileAttributes(subject, pnfsId, required);
            if (!checkMask(subject, mask, attributes)) {
                throw new PermissionDeniedCacheException("Access denied");
            }
        }
    }

    /**
     * Checks whether a subject has a certain set of access right to a
     * file system object.
     *
     * @param subject The Subject for which to check access rights
     * @param mask The access right to check
     * @param attr The FileAttributes of the object to check access rights to
     * @return true if subject has all access rights in mask,
     *         false otherwise
     */
    private boolean checkMask(Subject subject,
                              Set<AccessMask> mask,
                              FileAttributes attr)
    {
        AccessType access = ACCESS_ALLOWED;
        for (AccessMask m: mask) {
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

    private static Activity toActivity(AccessMask mask)
    {
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
            throws PermissionDeniedCacheException
    {
        if (!Subjects.isRoot(message.getSubject())) {
            FsPath path = message.getFsPath();
            if (path != null && !path.isEmpty()) {
                checkRestriction(message.getRestriction(),
                        message.getAccessMask(), activity, path.getParent());
            }
        }
    }

    private static void checkRestriction(PnfsMessage message, Activity activity)
            throws PermissionDeniedCacheException
    {
        if (!Subjects.isRoot(message.getSubject())) {
            FsPath path = message.getFsPath();
            if (path != null) {
                checkRestriction(message.getRestriction(), message.getAccessMask(),
                        activity, path);
            }
        }
    }

    private static void checkRestriction(PnfsMessage message, Activity activity,
            FsPath path) throws PermissionDeniedCacheException
    {
        if (!Subjects.isRoot(message.getSubject())) {
            checkRestriction(message.getRestriction(), message.getAccessMask(),
                    activity, path);
        }
    }

    private static void checkRestriction(Restriction restriction, Set<AccessMask> mask,
            Activity activity, FsPath path) throws PermissionDeniedCacheException
    {
        if (mask.stream()
                    .map(PnfsManagerV3::toActivity)
                    .anyMatch(a -> restriction.isRestricted(a, path))
                || restriction.isRestricted(activity, path)) {
            throw new PermissionDeniedCacheException("Permission denied: " + path);
        }
    }
}
