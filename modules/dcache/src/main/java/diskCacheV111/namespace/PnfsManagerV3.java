// $Id: PnfsManagerV3.java,v 1.42 2007-10-02 07:11:45 tigran Exp $

package diskCacheV111.namespace;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.security.auth.Subject;

import java.io.File;
import java.io.PrintWriter;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.InvalidMessageCacheException;
import diskCacheV111.util.MissingResourceCacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
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
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PnfsGetParentMessage;
import diskCacheV111.vehicles.PnfsMapPathMessage;
import diskCacheV111.vehicles.PnfsMessage;
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

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;
import org.dcache.auth.Subjects;
import org.dcache.chimera.UnixPermission;
import org.dcache.commons.stats.RequestCounters;
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

import static org.dcache.acl.enums.AccessType.*;
import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.namespace.FileAttribute.*;

public class PnfsManagerV3
    extends AbstractCellComponent
    implements CellCommandListener, CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(PnfsManagerV3.class);

    private static final int THRESHOLD_DISABLED = 0;

    private final Random _random = new Random(System.currentTimeMillis());

    private final RequestExecutionTimeGauges<Class<? extends PnfsMessage>> _gauges =
        new RequestExecutionTimeGauges<>("PnfsManagerV3");
    private final RequestCounters<Class<?>> _foldedCounters =
        new RequestCounters<>("PnfsManagerV3.Folded");

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
        PnfsGetFileAttributes.class,
        PnfsListDirectoryMessage.class
    };

    private int _threads;
    private int _threadGroups;
    private int _directoryListLimit;
    private int _queueMaxSize;
    private int _cacheLocationThreads;
    private int _listThreads;
    private long _logSlowThreshold;

    /**
     * Whether to use folding.
     */
    private boolean _canFold;

    /**
     * Queues for list operations. There is one queue per thread
     * group.
     */
    private BlockingQueue<CellMessage>[] _listQueues;

    /**
     * Tasks queues used for cache location messages. Depending on
     * configuration, this may be the same as <code>_fifos</code>.
     */
    private BlockingQueue<CellMessage>[] _locationFifos;

    /**
     * Tasks queues used for messages that do not operate on cache
     * locations.
     */
    private BlockingQueue<CellMessage>[] _fifos;

    private CellPath _cacheModificationRelay;
    private CellPath _pnfsDeleteNotificationRelay;

    private PermissionHandler _permissionHandler;
    private NameSpaceProvider _nameSpaceProvider;
    private NameSpaceProvider _cacheLocationProvider;


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
    public void setCacheLocationThreads(int threads)
    {
        _cacheLocationThreads = threads;
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
    public void setPnfsDeleteNotificationRelay(String path)
    {
        _pnfsDeleteNotificationRelay =
            Strings.isNullOrEmpty(path) ? null : new CellPath(path);
        _log.info("pnfsDeleteRelay = {}",
                  (_pnfsDeleteNotificationRelay == null) ? "NONE" : _pnfsDeleteNotificationRelay.toString());
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
    public void setCacheLocationProvider(NameSpaceProvider provider)
    {
        _cacheLocationProvider = provider;
    }

    @Required
    public void setQueueMaxSize(int maxSize)
    {
        _queueMaxSize = maxSize;
    }

    @Required
    public void setFolding(boolean folding)
    {
        _canFold = folding;
    }

    @Required
    public void setDirectoryListLimit(int limit)
    {
        _directoryListLimit = limit;
    }

    public void init()
    {
        _fifos = new BlockingQueue[_threads * _threadGroups];
        _log.info("Starting {} threads", _fifos.length);
        for (int i = 0; i < _fifos.length; i++) {
            if (_queueMaxSize > 0) {
                _fifos[i] = new LinkedBlockingQueue<>(_queueMaxSize);
            } else {
                _fifos[i] = new LinkedBlockingQueue<>();
            }
            new Thread(new ProcessThread(_fifos[i]), "proc-" + i).start();
        }

        if (_cacheLocationThreads > 0) {
            _log.info("Starting {} cache location threads", _cacheLocationThreads);
            _locationFifos = new BlockingQueue[_cacheLocationThreads];
            for (int i = 0; i < _locationFifos.length; i++) {
                _locationFifos[i] = new LinkedBlockingQueue<>();
                new Thread(new ProcessThread(_locationFifos[i]),
                           "proc-loc-" + i).start();
            }
        } else {
            _locationFifos = _fifos;
        }

        /* Start list-threads threads per thread group for list
         * processing. We use a shared queue per thread group, as
         * list operations are read only and thus there is no need
         * to serialize the operations.
         */
        _listQueues = new BlockingQueue[_threadGroups];
        for (int i = 0; i < _threadGroups; i++) {
            _listQueues[i] = new LinkedBlockingQueue<>();
            for (int j = 0; j < _listThreads; j++) {
                new Thread(new ProcessThread(_listQueues[i]), "proc-list-" + i + "-" + j).start();
            }
        }
    }

    @Override
    public void getInfo( PrintWriter pw ){
        pw.println("$Revision$");
        pw.println( "NameSpace Provider: ");
        pw.println( _nameSpaceProvider.toString() );
        pw.println( "CacheLocation Provider: ");
        pw.println( _cacheLocationProvider.toString() );
        pw.println();
        pw.println("List queues (" + _listQueues.length + ")");
        for (int i = 0; i < _listQueues.length; i++) {
            pw.println("    [" + i + "] " + _listQueues[i].size());
        }
        pw.println();
        pw.println("Threads (" + _fifos.length + ") Queue");
        for (int i = 0; i < _fifos.length; i++) {
            pw.println( "    ["+i+"] "+_fifos[i].size() ) ;
        }
        pw.println();
        pw.println("Thread groups (" + _threadGroups + ")");
        for (int i = 0; i < _threadGroups; i++) {
            int total = 0;
            for (int j = 0; j < _threads; j++) {
                total += _fifos[i * _threads + j].size();
            }
            pw.println("    [" + i + "] " + total);
        }
        pw.println();
        if (_fifos != _locationFifos) {
            pw.println("Cache Location Queues");
            for (int i = 0; i < _locationFifos.length; i++) {
                pw.println("    [" + i + "] " + _locationFifos[i].size());
            }
            pw.println();
        }

        pw.println( "Statistics:" ) ;
        pw.println(_gauges.toString());
        pw.println(_foldedCounters.toString());
    }

    public static final String hh_flags_set    = "<pnfsId> <key=value> [...]" ;
    public static final String hh_flags_remove = "<pnfsId> <key> [...]" ;
    public static final String hh_flags_ls     = "<pnfsId>" ;
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

            List<String> locations = _cacheLocationProvider.getCacheLocation(ROOT, pnfsId);

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

    public static final String hh_rename = " # rename <old name> <new name>" ;
    public String ac_rename_$_2( Args args ){

        PnfsId    pnfsId;
        String newName;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;

        }catch(Exception ee ){
            try {
                pnfsId = pathToPnfsid(ROOT, args.argv(0), true );
            }catch(Exception e) {
                return "rename failed: " + e.getMessage() ;
            }
        }


        try {
            newName = args.argv(1);
            rename(ROOT, pnfsId, newName, true);
        }catch( Exception e) {
            return "rename failed: " + e.getMessage() ;
        }

        return "" ;
    }

    public final static String hh_set_meta =
        "<pnfsid>|<globalPath> <uid> <gid> <perm>";
    public String ac_set_meta_$_4(Args args)
    {
        try {
            PnfsId pnfsId;
            if (PnfsId.isValid(args.argv(0))) {
                pnfsId = new PnfsId(args.argv(0));
            } else {
                pnfsId = pathToPnfsid(ROOT, args.argv(0), true);
            }

            FileAttributes attributes = new FileAttributes();
            attributes.setOwner(Integer.parseInt(args.argv(1)));
            attributes.setGroup(Integer.parseInt(args.argv(2)));
            attributes.setMode(Integer.parseInt(args.argv(3), 8));

            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                    EnumSet.noneOf(FileAttribute.class));

            return "Ok";
        }catch(Exception e) {
            return "set metadata failed: + " + e.getMessage();
        }
    }

    public static final String fh_storageinfoof =
        "   storageinfoof <pnfsid>|<globalPath> [-v] [-n] [-se]\n"+
        "        -v    verbose\n"+
        "        -n    don't resolve links\n"+
        "        -se   suppress exceptions\n" ;
    public static final String hh_storageinfoof = "<pnfsid>|<globalPath> [-v] [-n] [-se]" ;
    public String ac_storageinfoof_$_1( Args args )
    {
        PnfsId    pnfsId;
        boolean v = args.hasOption("v") ;
        boolean n = args.hasOption("n") ;

        StringBuilder sb = new StringBuilder() ;

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

            FileAttributes attributes =
                _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                        EnumSet.of(FileAttribute.STORAGEINFO, FileAttribute.ACCESS_LATENCY,
                                FileAttribute.RETENTION_POLICY,  FileAttribute.SIZE));

            StorageInfo info = StorageInfos.extractFrom(attributes);
            if(v) {
                sb.append(" Storage Info : ").append(info).append("\n") ;
            }else{
                sb.append(info.toString()).append("\n");
            }

        }catch(Exception ee ){
            sb.append("storageinfoof failed : ").append(ee.getMessage());
        }

        return sb.toString() ;
    }
    public static final String fh_metadataof =
        "   storageinfoof <pnfsid>|<globalPath> [-v] [-n] [-se]\n"+
        "        -v    verbose\n"+
        "        -n    don't resolve links\n"+
        "        -se   suppress exceptions\n" ;
    public static final String hh_metadataof = "<pnfsid>|<globalPath> [-v] [-n] [-se]" ;
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

    public String ac_flags_set_$_2_99(Args args) throws CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        Map<String,String> flags = new HashMap<>();
        for (int i = 1; i < args.argc(); i++) {
            String t = args.argv(i);
            int l = t.length();
            if (l > 0) {
                int p = t.indexOf('=');
                if ((p < 0) || (p == (l - 1))) {
                    flags.put(t, "");
                } else if (p > 0) {
                    flags.put(t.substring(0, p), t.substring(p + 1));
                }
            }
        }

        FileAttributes attributes = new FileAttributes();
        attributes.setFlags(flags);
        _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                EnumSet.noneOf(FileAttribute.class));

        return "" ;
    }

    public String ac_flags_remove_$_2_99( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;

        for( int i = 1 ; i < args.argc() ; i++ ){
            String t = args.argv(i) ;
            _nameSpaceProvider.removeFileAttribute(ROOT, pnfsId, t);
        }

        return "" ;
    }

    public String ac_flags_ls_$_1(Args args) throws CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        FileAttributes attributes =
            _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                                                 EnumSet.of(FileAttribute.FLAGS));
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<String,String> e: attributes.getFlags().entrySet()) {
            sb.append(e.getKey()).append(" -> ").append(e.getValue()).append("\n");
        }
        return sb.toString();
    }

    public static final String fh_dumpthreadqueues = "   dumpthreadqueues [<threadId>]\n"
        + "        dumthreadqueus prints the context of\n"
        + "        thread[s] queue[s] into the error log file";

    public static final String hh_dumpthreadqueues = "[<threadId>]\n";

    public String ac_dumpthreadqueues_$_0_1(Args args)
    {
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


    public static final String fh_set_file_size =
            "Updates the file's size in the namespace. This command has no effect on\n"
            + "the data stored on pools or on tape.\n\n"
            + "Syntax:\n"
            + "  set file size <pnfsid> <new size>\n\n"
            + "If the value of <new size> does not match the size of the stored data\n"
            + "then the file may become unavailable. Use with caution!\n";

    public final static String hh_set_file_size =
            "<pnfsid> <new size> # changes registered file size";
    public String ac_set_file_size_$_2(Args args) throws Exception
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));

        FileAttributes attributes = new FileAttributes();
        attributes.setSize(Long.valueOf(args.argv(1)));

        _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                EnumSet.noneOf(FileAttribute.class));

        return "";
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

        _cacheLocationProvider.addCacheLocation(ROOT, pnfsId, cacheLocation);

        return "";

    }

    public static final String hh_clear_file_cache_location = "<pnfsid> <pool name>";
    public String ac_clear_file_cache_location_$_2(Args args) throws Exception {

        PnfsId pnfsId = new PnfsId( args.argv(0));
        String cacheLocation = args.argv(1);

        _cacheLocationProvider.clearCacheLocation(ROOT, pnfsId, cacheLocation, false);

        return "";
    }

    public static final String hh_add_file_checksum = "<pnfsid> <type> <checksum>";
    public String ac_add_file_checksum_$_3(Args args)
        throws CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        ChecksumType type = ChecksumType.getChecksumType(args.argv(1));
        Checksum checksum = new Checksum(type, args.argv(2));
        FileAttributes attributes = new FileAttributes();
        attributes.setChecksums(Collections.singleton(checksum));
        _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes,
                EnumSet.noneOf(FileAttribute.class));
        return "";
    }

    public static final String hh_clear_file_checksum = "<pnfsid> <type>";
    public String ac_clear_file_checksum_$_2(Args args) throws CacheException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        ChecksumType type = ChecksumType.getChecksumType(args.argv(1));
        _nameSpaceProvider.removeChecksum(ROOT, pnfsId, type);
        return "";
    }

    public static final String hh_get_file_checksum = "<pnfsid> <type>";
    public String ac_get_file_checksum_$_2(Args args)
        throws CacheException, NoSuchAlgorithmException
    {
        PnfsId pnfsId = new PnfsId(args.argv(0));
        ChecksumType type = ChecksumType.getChecksumType(args.argv(1));
        Checksum checksum = getChecksum(ROOT, pnfsId, type);
        return (checksum == null) ? "" : checksum.toString();
    }

    public static final String hh_set_log_slow_threshold = "<timeout in ms>";
    public static final String fh_set_log_slow_threshold = "Set the threshold for reporting slow PNFS interactions.";
    public String ac_set_log_slow_threshold_$_1(Args args) {

        int newTimeout;

        try {
            newTimeout = Integer.parseInt( args.argv(0));
        } catch ( NumberFormatException e) {
            return "Badly formatted number " + args.argv(0);
        }

        if( newTimeout <= 0) {
            return "Timeout must be greater than zero";
        }

        _logSlowThreshold = newTimeout;

        return "";
    }

    public static final String fh_get_log_slow_threshold = "Return the current threshold for reporting slow PNFS interactions.";
    public String ac_get_log_slow_threshold_$_0( Args args) {
        if( _logSlowThreshold == THRESHOLD_DISABLED) {
                return "disabled";
            }
        return String.valueOf(_logSlowThreshold) + " ms";
    }

    public static final String fh_set_log_slow_threshold_disabled = "Disable reporting of slow PNFS interactions.";
    public String ac_set_log_slow_threshold_disabled_$_0( Args args) {
        _logSlowThreshold = THRESHOLD_DISABLED;
        return "";
    }

    public final static String fh_show_path_cache =
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
            _cacheLocationProvider.addCacheLocation(pnfsMessage.getSubject(),
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
            _cacheLocationProvider.clearCacheLocation(pnfsMessage.getSubject(),
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
            _log.info("get cache locations for "+pnfsId);

            checkMask(pnfsMessage);
            pnfsMessage.setCacheLocations(_cacheLocationProvider.getCacheLocation(subject, pnfsId));
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
            if (attrs.getStorageInfo().getKey("path") == null) {
                attrs.getStorageInfo().setKey("path", pnfsMessage.getPnfsPath());
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

    private void createUploadPath(PnfsCreateUploadPath message)
    {
        try {
            FsPath uploadPath = _nameSpaceProvider.createUploadPath(message.getSubject(),
                                                                    message.getPath(),
                                                                    message.getUid(),
                                                                    message.getGid(),
                                                                    message.getMode(),
                                                                    message.getSize(),
                                                                    message.getAccessLatency(),
                                                                    message.getRetentionPolicy(),
                                                                    message.getSpaceToken(),
                                                                    message.getOptions());
            message.setUploadPath(uploadPath);
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Create upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void commitUpload(PnfsCommitUpload message)
    {
        try {
            PnfsId pnfsId = _nameSpaceProvider.commitUpload(message.getSubject(),
                                                            message.getUploadPath(),
                                                            message.getPath(),
                                                            message.getOptions());
            message.setPnfsId(pnfsId);
            Set<FileAttribute> attributes = message.getRequestedAttributes();
            if (!attributes.isEmpty()) {
                message.setFileAttributes(
                        _nameSpaceProvider.getFileAttributes(Subjects.ROOT, pnfsId, attributes));
            }
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Create upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private void cancelUpload(PnfsCancelUpload message)
    {
        try {
            _nameSpaceProvider.cancelUpload(message.getSubject(), message.getUploadPath(), message.getPath());
        } catch (CacheException e) {
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Create upload path failed", e);
            message.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    /**
     * Returns true if and only if pnfsid is of one of the given
     * types.
     */
    private boolean isOfType(PnfsId pnfsid, Set<FileType> types)
        throws CacheException
    {
        /* Assuming the file exists, it must be of some type. Hence we
         * can avoid the query if types contains all types.
         */
        if (types.equals(EnumSet.allOf(FileType.class))) {
            return true;
        }

        FileAttributes attributes =
            _nameSpaceProvider.getFileAttributes(Subjects.ROOT, pnfsid,
                                                 EnumSet.of(FileAttribute.TYPE));
        return types.contains(attributes.getFileType());
    }

    public void deleteEntry(PnfsDeleteEntryMessage pnfsMessage){

        String path = pnfsMessage.getPath();
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        Subject subject = pnfsMessage.getSubject();
        Set<FileType> allowed = pnfsMessage.getAllowedFileTypes();

        try {

            if( path == null && pnfsId == null) {
                throw new CacheException(CacheException.INVALID_ARGS, "pnfsid or path have to be defined for PnfsDeleteEntryMessage");
            }

            if( path != null ) {

                PnfsId pnfsIdFromPath = pathToPnfsid(subject, path, false);

                checkMask(pnfsMessage.getSubject(), pnfsIdFromPath,
                          pnfsMessage.getAccessMask());

                /*
                 * raice condition check:
                 *
                 * in some cases ( srm overwrite ) one failed transfer may remove a file
                 * which belongs to an other transfer.
                 *
                 * If both path and id defined check that path points to defined id
                 */
                if( pnfsId != null ) {
                    if( !pnfsIdFromPath.equals(pnfsId) ) {
                        throw new FileNotFoundCacheException("Pnfsid does not correspond to provided file");
                    }
                } else {
                    pnfsId = pnfsIdFromPath;
                    pnfsMessage.setPnfsId(pnfsId);
                }

                if (!isOfType(pnfsId, allowed)) {
                    if (allowed.contains(FileType.DIR)) {
                        throw new NotDirCacheException("Path exists but is not of the expected type");
                    } else {
                        throw new NotFileCacheException("Path exists but is not of the expected type");
                    }
                }

                _log.info("delete PNFS entry for "+ path );
                _nameSpaceProvider.deleteEntry(subject, path);
            } else {
                if (!isOfType(pnfsId, allowed)) {
                    if (allowed.contains(FileType.DIR)) {
                        throw new NotDirCacheException("Path exists but is not of the expected type");
                    } else {
                        throw new NotFileCacheException("Path exists but is not of the expected type");
                    }
                }

                checkMask(pnfsMessage);

                _log.info("delete PNFS entry for "+ pnfsId );
                _nameSpaceProvider.deleteEntry(subject, pnfsId);
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

        if( pnfsMessage.getReturnCode() == 0 &&
            _pnfsDeleteNotificationRelay != null ) {
            PnfsDeleteEntryNotificationMessage deleteNotification =
                new PnfsDeleteEntryNotificationMessage(pnfsId,path);
            try{

                sendMessage( new CellMessage( _pnfsDeleteNotificationRelay,
                                              deleteNotification ) ) ;

            } catch (NoRouteToCellException e) {
                _log.error("Failed to relay " + deleteNotification + " to "+
                           _cacheModificationRelay + ": " + e.getMessage());
            }
        }


    }

    public void rename(PnfsRenameMessage msg)
    {
        try {
            PnfsId pnfsId = populatePnfsId(msg);
            String newName = msg.newName();
            _log.info("rename " + pnfsId + " to new name : " + newName);
            checkMask(msg);
            rename(msg.getSubject(), pnfsId, newName, msg.getOverwrite());
        } catch (CacheException e){
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error(e.toString(), e);
            msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                          "Pnfs rename failed");
        }
    }

    private void rename(Subject subject, PnfsId pnfsId,
                        String newName, boolean overwrite)
        throws CacheException
    {
        _log.info("Renaming " + pnfsId + " to " + newName );
        _nameSpaceProvider.renameEntry(subject, pnfsId, newName, overwrite);
    }


    private void removeByPnfsId(Subject subject, PnfsId pnfsId )
        throws CacheException
    {
        _log.info("removeByPnfsId : "+pnfsId );

        _nameSpaceProvider.deleteEntry(subject, pnfsId);
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
                pnfsMessage.setGlobalPath(pathfinder(subject, pnfsId));
            } else {
                _log.info("map:  path2id for " + globalPath);
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
            _deadline =
                (delay == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : System.currentTimeMillis() + initialDelay;
        }

        private void sendPartialReply()
        {
            _msg.setReply();

            try {
                CellMessage envelope = new CellMessage(_requestor, _msg);
                envelope.setLastUOID(_uoid);
                sendMessage(envelope);
                _messageCount++;
            } catch (NoRouteToCellException e){
                /* We cannot cancel, so log and ignore.
                 */
                _log.warn("Failed to send reply to " + _requestor + ": " + e.getMessage());
            }

            _msg.clear();
        }

        @Override
        public void addEntry(String name, FileAttributes attrs)
        {
            long now = System.currentTimeMillis();
            _msg.addEntry(name, attrs);
            if (_msg.getEntries().size() >= _directoryListLimit ||
                now > _deadline) {
                sendPartialReply();
                _deadline =
                    (_delay == Long.MAX_VALUE) ? Long.MAX_VALUE : now + _delay;
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

    private class ProcessThread implements Runnable {
        private final BlockingQueue<CellMessage> _fifo ;
        private ProcessThread( BlockingQueue<CellMessage> fifo ){ _fifo = fifo ; }
        @Override
        public void run(){

            _log.info("Thread <"+Thread.currentThread().getName()+"> started");

            boolean done = false;
            while( !done ){
                CellMessage message;
                try {
                    message = _fifo.take();
                } catch (InterruptedException e) {
                    done = true;
                    continue;
                }

                CDC.setMessageContext(message);
                try {
                    /* Discard messages if we are close to their
                     * timeout (within 10% of the TTL or 10 seconds,
                     * whatever is smaller)
                     */
                    PnfsMessage pnfs =
                        (PnfsMessage)message.getMessageObject();
                    if (message.getLocalAge() > message.getAdjustedTtl()
                        && useEarlyDiscard(pnfs)) {
                        _log.warn("Discarding " + pnfs.getClass().getSimpleName() +
                                  " because its time to live has been exceeded.");
                        sendTimeout(message, "TTL exceeded");
                        continue;
                    }

                    processPnfsMessage(message, pnfs);
                    fold(pnfs);
                } catch(Throwable processException) {
                    _log.warn( "processPnfsMessage : "+
                               Thread.currentThread().getName()+" : "+
                               processException );
                } finally {
                    CDC.clearMessageContext();
                }
            }
            _log.info("Thread <"+Thread.currentThread().getName()+"> finished");
        }

        protected void fold(PnfsMessage message)
        {
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
                        _log.info("Folded {}", other.getClass().getSimpleName());
                        _foldedCounters.incrementRequests(message.getClass());

                        i.remove();
                        envelope.revertDirection();

                        try {
                            sendMessage(envelope);
                        } catch (NoRouteToCellException e) {
                            _log.warn("Failed to send reply: " + e.getMessage());
                        }
                    }
                }
            }
        }
    }

    public void messageArrived(CellMessage envelope, PnfsListDirectoryMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        String path = message.getPnfsPath();
        int group;
        if (pnfsId != null) {
            group = pnfsIdToThreadGroup(pnfsId);
            _log.info("Using list queue [{}] {}", pnfsId, group);
        } else if (path != null) {
            group = pathToThreadGroup(path);
            _log.info("Using list queue [{}] {}", path, group);
        } else {
            throw new InvalidMessageCacheException("Missing PNFS id and path");
        }
        if (!_listQueues[group].offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    public void messageArrived(CellMessage envelope, PnfsMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        String path = message.getPnfsPath();

        if ((_cacheModificationRelay != null) &&
            ((message instanceof PnfsAddCacheLocationMessage) ||
             (message instanceof PnfsClearCacheLocationMessage))) {
            forwardModifyCacheLocationMessage(message);
        }

        boolean isCacheOperation =
            ((message instanceof PnfsAddCacheLocationMessage) ||
             (message instanceof PnfsClearCacheLocationMessage) ||
             (message instanceof PnfsGetCacheLocationsMessage));
        BlockingQueue<CellMessage> fifo;
        if (isCacheOperation && _locationFifos != _fifos) {
            int index;
            if (pnfsId != null) {
                index =
                    (int) (Math.abs((long) pnfsId.hashCode()) % _locationFifos.length);
                _log.info("Using location thread [{}] {}", pnfsId, index);
            } else if (path != null) {
                index =
                    (int) (Math.abs((long) path.hashCode()) % _locationFifos.length);
                _log.info("Using location thread [{}] {}", path, index);
            } else {
                index = _random.nextInt(_locationFifos.length);
                _log.info("Using random location thread {}", index);
            }
            fifo = _locationFifos[index];
        } else {
            int index;
            if (pnfsId != null) {
                index =
                    pnfsIdToThreadGroup(pnfsId) * _threads +
                    (int) (Math.abs((long) pnfsId.hashCode()) % _threads);
                _log.info("Using thread [{}] {}", pnfsId, index);
            } else if (path != null) {
                index =
                    pathToThreadGroup(path) * _threads +
                    (int) (Math.abs((long) path.hashCode()) % _threads);
                _log.info("Using thread [{}] {}", path, index);
            } else {
                index = _random.nextInt(_fifos.length);
                _log.info("Using random thread {}", index);
            }
            fifo = _fifos[index];
        }

        /*
         * try to add a message into queue.
         * tell requester, that queue is full
         */
        if (!fifo.offer(envelope)) {
            throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
        }
    }

    private void forwardModifyCacheLocationMessage(PnfsMessage message)
    {
        try {
            sendMessage(new CellMessage(_cacheModificationRelay, message));
        } catch (NoRouteToCellException e) {
            _log.error("Failed to relay " + message + " to " +
                               _cacheModificationRelay + ": " + e.getMessage());
        }
    }

    public void processPnfsMessage(CellMessage message, PnfsMessage pnfsMessage)
    {
        long ctime = System.currentTimeMillis();

        if (pnfsMessage instanceof PnfsAddCacheLocationMessage){
            addCacheLocation((PnfsAddCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsClearCacheLocationMessage){
            clearCacheLocation((PnfsClearCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetCacheLocationsMessage){
            getCacheLocations((PnfsGetCacheLocationsMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateSymLinkMessage) {
            createLink((PnfsCreateSymLinkMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateDirectoryMessage){
            createDirectory((PnfsCreateDirectoryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateEntryMessage){
            createEntry((PnfsCreateEntryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateUploadPath){
            createUploadPath((PnfsCreateUploadPath) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCommitUpload){
            commitUpload((PnfsCommitUpload) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCancelUpload){
            cancelUpload((PnfsCancelUpload) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsDeleteEntryMessage){
            deleteEntry((PnfsDeleteEntryMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsMapPathMessage){
            mapPath((PnfsMapPathMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsRenameMessage){
            rename((PnfsRenameMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsFlagMessage){
            updateFlag((PnfsFlagMessage) pnfsMessage);
        }
        else if ( pnfsMessage instanceof PnfsSetChecksumMessage){
            setChecksum((PnfsSetChecksumMessage) pnfsMessage);
        }
        else if( pnfsMessage instanceof PoolFileFlushedMessage ) {
            processFlushMessage((PoolFileFlushedMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetParentMessage){
            getParent((PnfsGetParentMessage) pnfsMessage);
        } else if (pnfsMessage instanceof PnfsListDirectoryMessage) {
            listDirectory(message, (PnfsListDirectoryMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetFileAttributes) {
            getFileAttributes((PnfsGetFileAttributes) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetFileAttributes) {
            setFileAttributes((PnfsSetFileAttributes) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsRemoveChecksumMessage) {
            removeChecksum((PnfsRemoveChecksumMessage) pnfsMessage);
        }
        else {
            _log.warn("Unexpected message class [" + pnfsMessage.getClass() + "] from source [" + message.getSourcePath() + "]");
            return;
        }
        if( pnfsMessage.getReturnCode() == CacheException.INVALID_ARGS ) {
            _log.error("Inconsistent message " + pnfsMessage.getClass() + " received form " + message.getSourcePath() );
        }

        long duration = System.currentTimeMillis() - ctime;
        _gauges.update(pnfsMessage.getClass(), duration);
        String logMsg = pnfsMessage.getClass() + " processed in " + duration + " ms";
        if( _logSlowThreshold != THRESHOLD_DISABLED && duration > _logSlowThreshold) {
            _log.warn(logMsg);
        } else {
            _log.info(logMsg);
        }


        if (! pnfsMessage.getReplyRequired() ){
            return;
        }
        try {
            message.revertDirection();
            sendMessage(message);
        } catch (NoRouteToCellException e) {
            _log.warn("Failed to send reply: " + e.getMessage());
        }
    }

    public void processFlushMessage(PoolFileFlushedMessage pnfsMessage)
    {
        try {
            FileAttributes attributesToUpdate = new FileAttributes();
            attributesToUpdate.setStorageInfo(pnfsMessage.getFileAttributes().getStorageInfo());
            _nameSpaceProvider.setFileAttributes(pnfsMessage.getSubject(),
                    pnfsMessage.getPnfsId(), attributesToUpdate,
                    EnumSet.noneOf(FileAttribute.class));
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
            try {
                msg.setFailed(CacheException.TIMEOUT, error);
                envelope.revertDirection();
                sendMessage(envelope);
            } catch (NoRouteToCellException e) {
                _log.warn("Failed to send reply: " + e.getMessage());
            }
        }
    }

    public void getFileAttributes(PnfsGetFileAttributes message)
    {
        try {
            Subject subject = message.getSubject();
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            Set<FileAttribute> requested = message.getRequestedAttributes();
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
                if (attrs.getStorageInfo().getKey("path") == null) {
                    attrs.getStorageInfo().setKey("path", message.getPnfsPath());
                }
                attrs.getStorageInfo().setKey("uid",  Integer.toString(attrs.getOwner()));
                attrs.getStorageInfo().setKey("gid", Integer.toString(attrs.getGroup()));
            }

            message.setFileAttributes(attrs);
            message.setSucceeded();
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
            PnfsId pnfsId = populatePnfsId(message);
            checkMask(message);
            if (attr.getDefinedAttributes().contains(FileAttribute.LOCATIONS)) {
                for (String pool: attr.getLocations()) {
                    PnfsMessage msg =
                        new PnfsAddCacheLocationMessage(pnfsId, pool);
                    forwardModifyCacheLocationMessage(msg);
                }
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
        checkMask(message.getSubject(),
                  message.getPnfsId(),
                  message.getAccessMask());
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
}
