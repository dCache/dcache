// $Id: PnfsManagerV3.java,v 1.42 2007-10-02 07:11:45 tigran Exp $

package diskCacheV111.namespace;

import org.dcache.vehicles.PnfsGetFileAttributes;
import org.dcache.vehicles.PnfsSetFileAttributes;
import org.dcache.vehicles.PnfsListDirectoryMessage;
import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.namespace.provider.*;

import dmg.cells.nucleus.* ;
import dmg.util.* ;

import org.dcache.util.PrefixMap;
import org.dcache.acl.handler.singleton.AclHandler;

import java.io.* ;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.auth.Subjects;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.ListHandler;
import org.dcache.namespace.FileType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.namespace.ChainedPermissionHandler;
import org.dcache.namespace.PosixPermissionHandler;
import org.dcache.namespace.ACLPermissionHandler;
import org.dcache.namespace.PermissionHandler;

import org.dcache.commons.stats.RequestCounters;
import javax.security.auth.Subject;

import org.dcache.acl.enums.AccessMask;
import org.dcache.acl.enums.AccessType;

import static org.dcache.auth.Subjects.ROOT;
import static org.dcache.acl.enums.AccessType.*;

public class PnfsManagerV3 extends CellAdapter
{
    private static final Logger _log =
        LoggerFactory.getLogger(PnfsManagerV3.class);

    private static final int THRESHOLD_DISABLED = 0;
    private static final int DEFAULT_LIST_THREADS = 2;
    private static final int DEFAULT_DIR_LIST_LIMIT = 100;
    private static final int TTL_BUFFER_MAXIMUM = 10000;
    private static final float TTL_BUFFER_FRACTION = 0.10f;

    private final String      _cellName  ;
    private final Args        _args      ;
    private final CellNucleus _nucleus   ;
    private final Random      _random   = new Random(System.currentTimeMillis());
    private int _threads = 1;
    private int _threadGroups = 1;
    private int _directoryListLimit = DEFAULT_DIR_LIST_LIMIT;

    /**
     * Queues for list operations. There is one queue per thread
     * group.
     */
    private final BlockingQueue<CellMessage>[] _listQueues;

    /**
     * Tasks queues used for cache location messages. Depending on
     * configuration, this may be the same as <code>_fifos</code>.
     */
    private final BlockingQueue<CellMessage>[] _locationFifos;

    /**
     * Tasks queues used for messages that do not operate on cache
     * locations.
     */
    private final BlockingQueue<CellMessage>[] _fifos;


    private CellPath     _cacheModificationRelay = null ;
    private CellPath     _pnfsDeleteNotificationRelay = null;

    /**
     * Whether to use folding.
     */
    private boolean      _canFold = false;

    private final PermissionHandler _permissionHandler;
    private final NameSpaceProvider _nameSpaceProvider;
    private final NameSpaceProvider _cacheLocationProvider;
    private final static String defaultNameSpaceProvider      = "diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory";
    private final static String defaultCacheLocationProvider  = "diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory";

    private final  RequestCounters<Class> _counters = new RequestCounters<Class> ("PnfsManagerV3");
    private final  RequestCounters<Class> _foldedCounters = new RequestCounters<Class> ("PnfsManagerV3.Folded");

    private void populateRequestMap()
    {
        _counters.addCounter(PnfsAddCacheLocationMessage.class);
        _counters.addCounter(PnfsClearCacheLocationMessage.class);
        _counters.addCounter(PnfsGetCacheLocationsMessage.class);
        _counters.addCounter(PnfsCreateDirectoryMessage.class);
        _counters.addCounter(PnfsCreateEntryMessage.class);
        _counters.addCounter(PnfsDeleteEntryMessage.class);
        _counters.addCounter(PnfsGetStorageInfoMessage.class);
        _counters.addCounter(PnfsSetStorageInfoMessage.class);
        _counters.addCounter(PnfsGetFileMetaDataMessage.class);
        _counters.addCounter(PnfsSetFileMetaDataMessage.class);
        _counters.addCounter(PnfsSetLengthMessage.class);
        _counters.addCounter(PnfsMapPathMessage.class);
        _counters.addCounter(PnfsRenameMessage.class);
        _counters.addCounter(PnfsFlagMessage.class);
        _counters.addCounter(PnfsSetChecksumMessage.class);
        _counters.addCounter(PnfsGetChecksumMessage.class);
        _counters.addCounter(PoolFileFlushedMessage.class);
        _counters.addCounter(PnfsGetChecksumAllMessage.class);
        _counters.addCounter(PnfsGetParentMessage.class);
        _counters.addCounter(PnfsSetFileAttributes.class);
        _counters.addCounter(PnfsGetFileAttributes.class);
        _counters.addCounter(PnfsListDirectoryMessage.class);
    }

    /**
     * Cache of path prefix to database IDs mappings.
     */
    PrefixMap<Integer> _pathToDBCache = new PrefixMap();

    private int _logSlowThreshold;

    /**
     * These messages are subject to being discarded if their time to
     * live has been exceeded (or is expected to be exceeded).
     */
    private final Class[] DISCARD_EARLY = {
        PnfsGetCacheLocationsMessage.class,
        PnfsGetStorageInfoMessage.class,
        PnfsMapPathMessage.class,
        PnfsGetFileMetaDataMessage.class,
        PnfsGetParentMessage.class,
        PnfsGetChecksumAllMessage.class,
        PnfsGetChecksumMessage.class,
        PnfsCreateEntryMessage.class,
        PnfsCreateDirectoryMessage.class,
        PnfsGetFileAttributes.class,
        PnfsListDirectoryMessage.class
    };

    public PnfsManagerV3( String cellName , String args ) throws Exception {

        super( cellName , PnfsManagerV3.class.getName(),  args , false ) ;

        _cellName = cellName ;
        _args     = getArgs() ;
        _nucleus  = getNucleus() ;

        populateRequestMap();

        useInterpreter( true ) ;

        try {
            //
            // make it backward compatible
            //
            if( _args.argc() < 1 )
                throw new
                    IllegalArgumentException(
                                             "Usage : ... [-pnfs=<pnfsMountpoint>] "+
                                             "-defaultPnfsServer=<serverName> "+
                                             "-cmRelay=<cellPathOfCacheModificationRelay> "+
                                             "-logSlowThreshold=<min. time in milliseconds> " +
                                             "-queueMaxSize=<pnfs message queue>" +
                                             "<StorageInfoExctractorClass>");

            //
            // get the thread multiplier
            //
            String tmp = _args.getOpt("threads") ;
            if( tmp != null ){
                try{
                    _threads = Integer.parseInt(tmp) ;
                }catch(NumberFormatException e){
                    _log.warn( "Threads not changed ("+e+")");
                }
            }
            tmp = _args.getOpt("threadGroups") ;
            if( tmp != null ){
                try{
                    _threadGroups = Integer.parseInt(tmp) ;
                }catch(NumberFormatException e){
                    _log.warn( "Thread groups not changed ("+e+")");
                }
            }
            tmp = _args.getOpt("cmRelay") ;
            if( tmp != null )_cacheModificationRelay = new CellPath(tmp) ;
            _log.info("CacheModificationRelay = "+
                      ( _cacheModificationRelay == null ? "NONE" : _cacheModificationRelay.toString() ) );

            tmp = _args.getOpt("pnfsDeleteRelay") ;
            if( tmp != null )_pnfsDeleteNotificationRelay = new CellPath(tmp) ;
            _log.info("pnfsDeleteRelay = "+
                      ( _pnfsDeleteNotificationRelay == null ? "NONE" : _pnfsDeleteNotificationRelay.toString() ) );

            tmp = _args.getOpt("logSlowThreshold") ;
            _logSlowThreshold = (tmp != null) ? Integer.parseInt(tmp) : THRESHOLD_DISABLED;
            _log.info("logSlowThreshold = "+
                      ( _logSlowThreshold == THRESHOLD_DISABLED ? "NONE" : Integer.toString(_logSlowThreshold) ) );

            //
            //
            String nameSpace_provider = _args.getOpt("namespace-provider") ;
            if( nameSpace_provider == null ) {
                nameSpace_provider = defaultNameSpaceProvider;
            }

            _log.info("Namespace provider: " + nameSpace_provider);
            DcacheNameSpaceProviderFactory nameSpaceProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(nameSpace_provider).newInstance();
            _permissionHandler = new ChainedPermissionHandler(new ACLPermissionHandler(), new PosixPermissionHandler());
            _nameSpaceProvider =
                new PermissionHandlerNameSpaceProvider(nameSpaceProviderFactory.getProvider(_args, _nucleus), _permissionHandler);


            String cacheLocation_provider = _args.getOpt("cachelocation-provider") ;
            if( cacheLocation_provider == null ) {
                cacheLocation_provider = defaultCacheLocationProvider;
            }

            _log.info("CacheLocation provider: " + cacheLocation_provider);
            DcacheNameSpaceProviderFactory cacheLocationProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(cacheLocation_provider).newInstance();
            _cacheLocationProvider = cacheLocationProviderFactory.getProvider(_args, _nucleus);

            String queueMaxSizeOption = _args.getOpt("queueMaxSize");
            int queueMaxSize =  queueMaxSizeOption == null ? 0 : Integer.parseInt(queueMaxSizeOption);
            //
            // and now the threads and fifos
            //
            _fifos = new BlockingQueue[_threads * _threadGroups];
            _log.info("Starting " + _fifos.length + " threads");
            for (int i = 0; i < _fifos.length; i++) {
                if (queueMaxSize > 0) {
                    _fifos[i] = new LinkedBlockingQueue<CellMessage>(queueMaxSize);
                } else {
                    _fifos[i] = new LinkedBlockingQueue<CellMessage>();
                }
                _nucleus.newThread(new ProcessThread(_fifos[i]), "proc-" + i).start();
            }

            tmp = _args.getOpt("cachelocation-threads");
            int threads = (tmp == null) ? 0 : Integer.parseInt(tmp);
            if (threads > 0) {
                _log.info("Starting " +  threads + " cache location threads");
                _locationFifos = new BlockingQueue[threads];
                for (int i = 0; i < _locationFifos.length; i++) {
                    _locationFifos[i] = new LinkedBlockingQueue();
                    _nucleus.newThread(new ProcessThread(_locationFifos[i]),
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
            tmp = _args.getOpt("list-threads");
            int listThreads =
                (tmp == null) ? DEFAULT_LIST_THREADS : Integer.parseInt(tmp);
            _listQueues = new BlockingQueue[_threadGroups];
            for (int i = 0; i < _threadGroups; i++) {
                _listQueues[i] = new LinkedBlockingQueue();
                for (int j = 0; j < listThreads; j++) {
                    _nucleus.newThread(new ProcessThread(_listQueues[i]), "proc-list-" + i + "-" + j).start();
                }
            }

            tmp = _args.getOpt("folding");
            if (tmp != null && (tmp.equals("true") || tmp.equals("yes") ||
                                tmp.equals("enabled"))) {
                _canFold = true;
            }

            tmp = _args.getOpt("directoryListLimit");
            if (tmp != null) {
                _directoryListLimit = Integer.parseInt(tmp);
            }

            /* Initialize ACL.
             */
            Properties props = new Properties();
            getAclProperty("aclEnabled", props, _args);
            getAclProperty("aclTable", props, _args);
            getAclProperty("aclConnDriver", props, _args);
            getAclProperty("aclConnUrl", props, _args);
            getAclProperty("aclConnUser", props, _args);
            getAclProperty("aclConnPswd", props, _args);
            AclHandler.setAclConfig(props);
        } catch (RuntimeException e){
            _log.error("Exception occurred", e);
            start();
            kill();
            throw e;
        }
        //Make the cell name well-known
        getNucleus().export();
        start() ;
    }

    private void getAclProperty(String key, Properties props, Args args)
    {
        String value = args.getOpt(key);
        if (value != null) {
            props.setProperty(key, value);
        }
    }

    @Override
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision$" ); }
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
        pw.println(_counters.toString());
        pw.println(_foldedCounters.toString());
        return ;
    }

    public String hh_flags_set    = "<pnfsId> <key=value> [...]" ;
    public String hh_flags_remove = "<pnfsId> <key> [...]" ;
    public String hh_flags_ls     = "<pnfsId>" ;
    public String hh_pnfsidof     = "<globalPath>" ;
    public String ac_pnfsidof_$_1( Args args )throws Exception {
        PnfsId pnfsId = null;
        StringBuffer sb = new StringBuffer();
        try {
            pnfsId = pathToPnfsid(ROOT, args.argv(0), false);
            sb.append(pnfsId.toString());
        }catch(Exception e){
            sb.append("pnfsidof failed:" +e.getMessage());
        }

        return sb.toString();
    }

    public String hh_cacheinfoof = "<pnfsid>|<globalPath>" ;
    public String ac_cacheinfoof_$_1( Args args )throws Exception {
        PnfsId    pnfsId   = null ;
        StringBuffer sb = new StringBuffer();
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
            sb.append("cacheinfoof failed: " + e.getMessage());
        }
        return sb.toString();
    }

    public String hh_pathfinder = "<pnfsId>" ;
    public String ac_pathfinder_$_1( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        return _nameSpaceProvider.pnfsidToPath(ROOT, pnfsId);
    }

    public String hh_rename = " # rename <old name> <new name>" ;
    public String ac_rename_$_2( Args args ){

        PnfsId    pnfsId   = null ;
        String newName = null;
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

            _nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes);

            return "Ok";
        }catch(Exception e) {
            return "set metadata failed: + " + e.getMessage();
        }
    }

    public String hh_set_storageinfo = "<pnfsid>|<globalPath> [-<option>=<value>] # depricated";
    public String ac_set_storageinfo_$_1(Args args) throws Exception {

        PnfsId pnfsId = null;
        String reply = "";

        try {

            try {

                pnfsId = new PnfsId(args.argv(0));

            } catch (IllegalArgumentException e) {
                pnfsId = pathToPnfsid(ROOT,args.argv(0), true);
            }

            FileAttributes attributes =
                _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                                                     EnumSet.of(FileAttribute.STORAGEINFO));

            StorageInfo storageInfo = attributes.getStorageInfo();

            String accessLatency = args.getOpt("accessLatency");
            if( accessLatency != null ) {
                AccessLatency al = AccessLatency.getAccessLatency(accessLatency);
                if( !al.equals(storageInfo.getAccessLatency()) ) {
                    storageInfo.setAccessLatency(al);
                    storageInfo.isSetAccessLatency(true);
                    _nameSpaceProvider.setStorageInfo(ROOT, pnfsId, storageInfo, NameSpaceProvider.SI_OVERWRITE);

                    // get list of known locations and modify sticky flag
                    List<String> locations = _cacheLocationProvider.getCacheLocation(ROOT, pnfsId);
                    for( String pool : locations ) {

                        PoolSetStickyMessage setSticky = new PoolSetStickyMessage(pool, pnfsId, al.equals(AccessLatency.ONLINE));
                        setSticky.setReplyRequired(false);
                        CellMessage cellMessage = new CellMessage(new CellPath(pool), setSticky);

                        try {
                            sendMessage(cellMessage);
                        }catch(NoRouteToCellException nrtc) {
                            // TODO: report it
                        }

                    }
                }
            }

        } catch (Exception e) {
            reply = "set storageinfo failed: + " + e.getMessage();
        }

        return reply;
    }

    public String fh_storageinfoof =
        "   storageinfoof <pnfsid>|<globalPath> [-v] [-n] [-se]\n"+
        "        -v    verbose\n"+
        "        -n    don't resolve links\n"+
        "        -se   suppress exceptions\n" ;
    public String hh_storageinfoof = "<pnfsid>|<globalPath> [-v] [-n] [-se]" ;
    public String ac_storageinfoof_$_1( Args args )throws Exception {
        PnfsId    pnfsId = null ;
        boolean v = args.getOpt("v") != null ;
        boolean n = args.getOpt("n") != null ;

        StringBuffer sb = new StringBuffer() ;

        try{
            try{
                pnfsId = new PnfsId( args.argv(0) ) ;
                if(v)sb.append("PnfsId : "+pnfsId).append("\n") ;
            }catch(Exception ee ){
                pnfsId = pathToPnfsid(ROOT, args.argv(0) , n ) ;

                if(v)sb.append("   Local Path : ").append(args.argv(0)).append("\n") ;
                if(v)sb.append("       PnfsId : ").append(pnfsId).append("\n") ;
            }

            FileAttributes attributes =
                _nameSpaceProvider.getFileAttributes(ROOT, pnfsId,
                                                     EnumSet.of(FileAttribute.STORAGEINFO));

            StorageInfo info = attributes.getStorageInfo();
            if(v) {
                sb.append(" Storage Info : "+info ).append("\n") ;
            }else{
                sb.append(info.toString()).append("\n");
            }

        }catch(Exception ee ){
            sb.append("storageinfoof failed : "+ee.getMessage() ) ;
        }

        return sb.toString() ;
    }
    public String fh_metadataof =
        "   storageinfoof <pnfsid>|<globalPath> [-v] [-n] [-se]\n"+
        "        -v    verbose\n"+
        "        -n    don't resolve links\n"+
        "        -se   suppress exceptions\n" ;
    public String hh_metadataof = "<pnfsid>|<globalPath> [-v] [-n] [-se]" ;
    public String ac_metadataof_$_1( Args args )throws Exception {
        PnfsId    pnfsId = null ;
        StringBuffer sb = new StringBuffer() ;

        boolean v = args.getOpt("v") != null ;
        boolean n = args.getOpt("n") != null ;

        try{
            try{
                pnfsId = new PnfsId( args.argv(0) ) ;
                if(v)sb.append("PnfsId : "+pnfsId).append("\n") ;
            }catch(Exception ee ){
                pnfsId = pathToPnfsid(ROOT, args.argv(0) , n ) ;

                if(v)sb.append("   Local Path : ").append(args.argv(0)).append("\n") ;
                if(v)sb.append("       PnfsId : ").append(pnfsId).append("\n") ;
            }

            FileMetaData info =
                new FileMetaData(_nameSpaceProvider.getFileAttributes(ROOT,  pnfsId, FileMetaData.getKnownFileAttributes()));
            if(v){
                sb.append("    Meta Data : ").append(info ).append("\n") ;
            }else{
                sb.append(info.toString()).append("\n");
            }
        }catch(Exception ee ){
            sb.append("matadataof failed : "+ee.getMessage() ) ;
        }
        return sb.toString() ;
    }

    public String ac_flags_set_$_2_99( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;

        for( int i = 1 ; i < args.argc() ; i++ ){
            String t = args.argv(i) ;
            int l = t.length() ;
            if( l == 0 )continue ;
            int p = t.indexOf('=');
            if( ( p < 0 ) || ( p == (l-1) ) ){
                _nameSpaceProvider.setFileAttribute(ROOT, pnfsId, t, "");
            }else if( p > 0 ){
                _nameSpaceProvider.setFileAttribute(ROOT, pnfsId, t.substring(0,p), t.substring(p+1));
            }
        }

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

    public String ac_flags_ls_$_1( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;
        String[] keys = _nameSpaceProvider.getFileAttributeList(ROOT, pnfsId);

        StringBuffer sb = new StringBuffer();
        for( int i = 0; i < keys.length; i++) {
            sb.append(keys[i]).append(" -> ").
                append(  _nameSpaceProvider.getFileAttribute(ROOT, pnfsId, keys[i] ) ).append("\n");

        }


        return sb.toString() ;
    }

    public String fh_dumpthreadqueues = "   dumpthreadqueues [<threadId>]\n"
        + "        dumthreadqueus prints the context of\n"
        + "        thread[s] queue[s] into the error log file";

    public String hh_dumpthreadqueues = "[<threadId>]\n";

    public String ac_dumpthreadqueues_$_0_1(Args args) throws Exception {
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

    public final static String hh_set_file_size =
        "<pnfsid> <new size> # DANGER";
    public String ac_set_file_size_$_2(Args args) throws Exception
    {
    	PnfsId pnfsId = new PnfsId(args.argv(0));

        FileAttributes attributes = new FileAttributes();
        attributes.setSize(Long.valueOf(args.argv(1)));

    	_nameSpaceProvider.setFileAttributes(ROOT, pnfsId, attributes);

    	return "";
    }

    public String hh_add_file_cache_location = "<pnfsid> <pool name>";
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

    public String hh_clear_file_cache_location = "<pnfsid> <pool name>";
    public String ac_clear_file_cache_location_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));
    	String cacheLocation = args.argv(1);

    	_cacheLocationProvider.clearCacheLocation(ROOT, pnfsId, cacheLocation, false);

    	return "";
    }

    public String hh_add_file_checksum = "<pnfsid> <type> <checksum>";
    public String ac_add_file_checksum_$_3(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));
    	_nameSpaceProvider.addChecksum(ROOT, pnfsId, Integer.parseInt(args.argv(1)), args.argv(2));

    	return "";

    }

    public String hh_clear_file_checksum = "<pnfsid> <type>";
    public String ac_clear_file_checksum_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));

    	_nameSpaceProvider.removeChecksum(ROOT, pnfsId, Integer.parseInt(args.argv(1)));

    	return "";
    }

    public String hh_get_file_checksum = "<pnfsid> <type>";
    public String ac_get_file_checksum_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));

    	String checkSum = _nameSpaceProvider.getChecksum(ROOT, pnfsId, Integer.parseInt(args.argv(1)));

    	return checkSum == null ? "" : checkSum;
    }

    public String hh_set_log_slow_threshold = "<timeout in ms>";
    public String fh_set_log_slow_threshold = "Set the threshold for reporting slow PNFS interactions.";
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

    public String fh_get_log_slow_threshold = "Return the current threshold for reporting slow PNFS interactions.";
    public String ac_get_log_slow_threshold_$_0( Args args) {
    	if( _logSlowThreshold == THRESHOLD_DISABLED)
            return "disabled";

    	return Integer.toString( _logSlowThreshold) + " ms";
    }


    public String fh_set_log_slow_threshold_disabled = "Disable reporting of slow PNFS interactions.";
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
        StringBuffer s = new StringBuffer();
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

    private void getChecksum(PnfsGetChecksumMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();
        int    type      = msg.getType();

        try{
            if(pnfsId == null ) {
                throw new InvalidMessageCacheException("no pnfsid defined");
            }
            String checksumValue =
                _nameSpaceProvider.getChecksum(msg.getSubject(), pnfsId,type);
            msg.setValue(checksumValue);
        }catch( CacheException e ){
            _log.warn(e.toString()) ;
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            _log.warn(e.toString()) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }
    }

    private void listChecksumTypes(PnfsGetChecksumAllMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();

        try{
            int []types =
                _nameSpaceProvider.listChecksumTypes(msg.getSubject(), pnfsId);
            msg.setValue(types);
        }catch( CacheException e ){
            _log.warn(e.toString()) ;
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            _log.warn(e.toString()) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }
    }

    private void setChecksum(PnfsSetChecksumMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();
        String value     = msg.getValue() ;
        int type         = msg.getType();

        try{
            _nameSpaceProvider.addChecksum(msg.getSubject(), pnfsId,type,value);
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

        } catch (CacheException e) {
            _log.warn("Exception in updateFlag: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Exception in updateFlag", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    private String updateFlag(Subject subject, PnfsId pnfsId, PnfsFlagMessage.FlagOperation operation, String flagName,
                              String value) throws CacheException {

        switch (operation) {

        case SET:
            _log.info("flags set " + pnfsId + " " + flagName + "=" + value);
            _nameSpaceProvider.setFileAttribute(subject, pnfsId, flagName, value);
            break;
        case SETNOOVERWRITE:
            _log.info("flags set (dontoverwrite) " + pnfsId + " " + flagName + "=" + value);
            String x = (String) _nameSpaceProvider.getFileAttribute(subject, pnfsId, flagName);
            if ((x == null) || (!x.equals(value))) {
                _log.info("flags set " + pnfsId + " " + flagName + "=" + value);
                _nameSpaceProvider.setFileAttribute(subject, pnfsId, flagName, value);
            }
            break;
        case GET:
            String v = (String) _nameSpaceProvider.getFileAttribute(subject, pnfsId, flagName);
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

    public void createDirectory(PnfsCreateDirectoryMessage pnfsMessage){
        PnfsId pnfsId = null;
        _log.info("create directory "+pnfsMessage.getPath());
        try {
            File file = new File(pnfsMessage.getPath());
            checkMask(pnfsMessage.getSubject(), file.getParent(),
                      pnfsMessage.getAccessMask());

            pnfsId = _nameSpaceProvider.createEntry(pnfsMessage.getSubject(), pnfsMessage.getPath(), pnfsMessage.getUid(), pnfsMessage.getGid(), pnfsMessage.getMode(), true);

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

        PnfsId pnfsId = null;
        _log.info("create entry "+pnfsMessage.getPath());
        try {
            File file = new File(pnfsMessage.getPath());
            checkMask(pnfsMessage.getSubject(), file.getParent(),
                      pnfsMessage.getAccessMask());

            pnfsId = _nameSpaceProvider.createEntry(pnfsMessage.getSubject(), pnfsMessage.getPath(), pnfsMessage.getUid(), pnfsMessage.getGid(), pnfsMessage.getMode(), false);

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

                Set<FileAttribute> requested =
                    pnfsMessage.getRequestedAttributes();
                requested.add(FileAttribute.STORAGEINFO);

                /* If we were allowed to create the entry above, then
                 * we also ought to be allowed to read it here. Hence
                 * we use ROOT as the subject.
                 */
                FileAttributes attrs =
                    _nameSpaceProvider.getFileAttributes(ROOT,
                                                         pnfsId,
                                                         requested);

                StorageInfo info = attrs.getStorageInfo();
                info.setKey("path", pnfsMessage.getPath());
                info.setKey("uid", Integer.toString(pnfsMessage.getUid()));
                info.setKey("gid", Integer.toString(pnfsMessage.getGid()));

                pnfsMessage.setFileAttributes(attrs);

            } catch (CacheException e){
                _log.warn("Can't determine storageInfo: " + e);
            }
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.error("Create entry failed", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void setStorageInfo( PnfsSetStorageInfoMessage pnfsMessage ){
        Subject subject = pnfsMessage.getSubject();
        try {
            PnfsId pnfsId = populatePnfsId(pnfsMessage);
            _log.info( "setStorageInfo : "+pnfsId ) ;

            checkMask(pnfsMessage);
            _nameSpaceProvider.setStorageInfo(subject, pnfsId, pnfsMessage.getStorageInfo(), pnfsMessage.getAccessMode());

        } catch (FileNotFoundCacheException e) {
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, e.getMessage());
        } catch (CacheException e) {
            _log.warn("Failed to set storage info: " + e);
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.warn("Failed to set storage info", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }
    }

    public void setFileMetaData(PnfsSetFileMetaDataMessage message)
    {
        try {
            PnfsId pnfsId = populatePnfsId(message);
            FileMetaData meta = message.getMetaData();
            _log.info("setFileMetaData=" + meta + " for " + pnfsId);

            checkMask(message);

            _nameSpaceProvider.setFileAttributes(message.getSubject(), pnfsId,
                                                 meta.toFileAttributes());
        } catch (CacheException e) {
            _log.warn("Failed to set meta data: " + e);
            message.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.warn("Failed to set meta data", e);
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
                }

                if (!isOfType(pnfsId, allowed)) {
                    throw new CacheException(CacheException.INVALID_ARGS,
                                             "Path exists but is not of the expected type");
                }

                _log.info("delete PNFS entry for "+ path );
                _nameSpaceProvider.deleteEntry(subject, path);
            } else {
                if (!isOfType(pnfsId, allowed)) {
                    throw new CacheException(CacheException.INVALID_ARGS,
                                             "Path exists but is not of the expected type");
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

    public void setLength(PnfsSetLengthMessage pnfsMessage){

        /*
         * While new pools will send PnfsSetFileAttributes old pools
         * will send setLength. This is will happen only during a transition
         * period when old pools combined with new PnfsManager.
         *
         * Let use this to update default AccessLatency and RetentionPloicy as well.
         *
         */
        try {
            PnfsId pnfsId = populatePnfsId(pnfsMessage);
            long   length = pnfsMessage.getLength();
            Subject subject = pnfsMessage.getSubject();

            _log.info("Set length of " + pnfsId + " to " + length);

            checkMask(pnfsMessage);

            FileAttributes fileAttributes = new FileAttributes();
            fileAttributes.setSize(length);
            fileAttributes.setDefaultAccessLatency();
            fileAttributes.setDefaultRetentionPolicy();

            _nameSpaceProvider.setFileAttributes(subject, pnfsId, fileAttributes);

        } catch (FileNotFoundCacheException e) {
            // file is gone.....
            pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, e.getMessage());
        } catch (CacheException ce) {
            pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
            _log.warn("Failed to set length: " + ce.getMessage());
        } catch (RuntimeException e){
            _log.warn("Failed to set length", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
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
                pnfsMessage.setPnfsId(pathToPnfsid(subject, globalPath, false));
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
            _msg.setFinal(false);
            _msg.setReply();

            try {
                CellMessage envelope = new CellMessage(_requestor, _msg);
                envelope.setLastUOID(_uoid);
                sendMessage(envelope);
            } catch (NoRouteToCellException e){
                /* We cannot cancel, so log and ignore.
                 */
                _log.warn("Failed to send reply to " + _requestor + ": " + e.getMessage());
            }

            _msg.clear();
        }

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
    }

    private void listDirectory(CellMessage envelope, PnfsListDirectoryMessage msg)
    {
        if (!msg.getReplyRequired()) {
            return;
        }

        try {
            String path = msg.getPnfsPath();

            checkMask(msg.getSubject(), path, msg.getAccessMask());

            long delay = getAdjustedTtl(envelope);
            long initialDelay =
                (delay == Long.MAX_VALUE)
                ? Long.MAX_VALUE
                : delay - envelope.getLocalAge();
            CellPath source = (CellPath)envelope.getSourcePath().clone();
            source.revert();
            ListHandler handler =
                new ListHandlerImpl(source, envelope.getUOID(),
                                    msg, initialDelay, delay);
            _nameSpaceProvider.list(msg.getSubject(), path,
                                    msg.getPattern(),
                                    msg.getRange(),
                                    msg.getRequestedAttributes(),
                                    handler);
            msg.setFinal(true);
            msg.setSucceeded();
        } catch (FileNotFoundCacheException e) {
            msg.setFailed(e.getRc(), e.getMessage());
        } catch (NotDirCacheException e) {
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
                    if (message.getLocalAge() > getAdjustedTtl(message)
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
                    CellMessage envelope = (CellMessage) i.next();
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

    @Override
    public void messageArrived(CellMessage message)
    {
        Object pnfsMessage  = message.getMessageObject();
        if (! (pnfsMessage instanceof PnfsMessage) ){
            _log.warn("Unexpected message class [" + pnfsMessage.getClass() + "] from source [" + message.getSourceAddress() + "]");
            return;
        }
        PnfsMessage pnfs   = (PnfsMessage)pnfsMessage ;
        PnfsId      pnfsId = pnfs.getPnfsId() ;
        String      path = pnfs.getPnfsPath() ;

        if( ( _cacheModificationRelay != null ) &&
            ( ( pnfs instanceof PnfsAddCacheLocationMessage   ) ||
              ( pnfs instanceof PnfsClearCacheLocationMessage )    ) ){


            forwardModifyCacheLocationMessage( pnfs ) ;

        }

        try {
            if (pnfs instanceof PnfsListDirectoryMessage) {
                int group;
                if (pnfsId != null) {
                    group = pnfsIdToThreadGroup(pnfsId);
                    _log.info("Using list queue [" + pnfsId + "] " + group);
                } else if (path != null) {
                    group = pathToThreadGroup(path);
                    _log.info("Using list queue [" + path + "] " + group);
                } else {
                    throw new InvalidMessageCacheException("Missing PNFS id and path");
                }
                if (!_listQueues[group].offer(message)) {
                    throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
                }
                return;
            }

            boolean isCacheOperation =
                ((pnfs instanceof PnfsAddCacheLocationMessage) ||
                 (pnfs instanceof PnfsClearCacheLocationMessage) ||
                 (pnfs instanceof PnfsGetCacheLocationsMessage));
            BlockingQueue<CellMessage> fifo;
            if (isCacheOperation && _locationFifos != _fifos) {
                int index;
                if (pnfsId != null) {
                    index =
                        (int) (Math.abs((long) pnfsId.hashCode()) % _locationFifos.length);
                    _log.info("Using location thread [" + pnfsId + "] " + index);
                } else if (path != null) {
                    index =
                        (int) (Math.abs((long) path.hashCode()) % _locationFifos.length);
                    _log.info("Using location thread [" + path + "] " + index);
                } else {
                    index = _random.nextInt(_locationFifos.length);
                    _log.info("Using random location thread " + index);
                }
                fifo = _locationFifos[index];
            } else {
                int index;
                if (pnfsId != null) {
                    index =
                        pnfsIdToThreadGroup(pnfsId) * _threads +
                        (int) (Math.abs((long) pnfsId.hashCode()) % _threads);
                    _log.info("Using thread [" + pnfsId + "] " + index);
                } else if (path != null) {
                    index =
                        pathToThreadGroup(path) * _threads +
                        (int) (Math.abs((long) path.hashCode()) % _threads);
                    _log.info("Using thread [" + path + "] " + index);
                } else {
                    index = _random.nextInt(_fifos.length);
                    _log.info("Using random thread " + index);
                }
                fifo = _fifos[index];
            }

            /*
             * try to add a message into queue.
             * tell requester, that queue is full
             */
            if (!fifo.offer(message)) {
                throw new MissingResourceCacheException("PnfsManager queue limit exceeded");
            }
        } catch (CacheException e) {
            pnfs.setFailed(e.getRc(), e);
            try {
                message.revertDirection();
                sendMessage(message);
            } catch (NoRouteToCellException f) {
                _log.warn("Requester cell disappeared: " + f.getMessage());
            }
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
        _counters.incrementRequests(pnfsMessage.getClass());

        if (pnfsMessage instanceof PnfsAddCacheLocationMessage){
            addCacheLocation((PnfsAddCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsClearCacheLocationMessage){
            clearCacheLocation((PnfsClearCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetCacheLocationsMessage){
            getCacheLocations((PnfsGetCacheLocationsMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateDirectoryMessage){
            createDirectory((PnfsCreateDirectoryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateEntryMessage){
            createEntry((PnfsCreateEntryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsDeleteEntryMessage){
            deleteEntry((PnfsDeleteEntryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetStorageInfoMessage){
            setStorageInfo((PnfsSetStorageInfoMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetFileMetaDataMessage){
            setFileMetaData((PnfsSetFileMetaDataMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetLengthMessage){
            setLength((PnfsSetLengthMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsMapPathMessage){
            mapPath((PnfsMapPathMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsRenameMessage){
            rename((PnfsRenameMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsFlagMessage){
            updateFlag((PnfsFlagMessage)pnfsMessage);
        }
        else if ( pnfsMessage instanceof PnfsSetChecksumMessage){
            setChecksum((PnfsSetChecksumMessage)pnfsMessage);
        }
        else if ( pnfsMessage instanceof PnfsGetChecksumMessage){
            getChecksum((PnfsGetChecksumMessage)pnfsMessage);
        }
        else if( pnfsMessage instanceof PoolFileFlushedMessage ) {
            processFlushMessage((PoolFileFlushedMessage) pnfsMessage );
        }
        else if ( pnfsMessage instanceof PnfsGetChecksumAllMessage ){
            listChecksumTypes((PnfsGetChecksumAllMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetParentMessage){
            getParent((PnfsGetParentMessage)pnfsMessage);
        } else if (pnfsMessage instanceof PnfsListDirectoryMessage) {
            listDirectory(message, (PnfsListDirectoryMessage) pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetFileAttributes) {
            getFileAttributes((PnfsGetFileAttributes)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetFileAttributes) {
            setFileAttributes((PnfsSetFileAttributes)pnfsMessage);
        }
        else {
            _log.warn("Unexpected message class [" + pnfsMessage.getClass() + "] from source [" + message.getSourceAddress() + "]");
            return;
        }
        if(pnfsMessage.getReturnCode() != 0) {
            _counters.incrementFailed(pnfsMessage.getClass());
        }
        if( pnfsMessage.getReturnCode() == CacheException.INVALID_ARGS ) {
            _log.error("Inconsistent message " + pnfsMessage.getClass() + " received form " + message.getSourceAddress() );
        }

        long duration = System.currentTimeMillis() - ctime;
        String logMsg = pnfsMessage.getClass() + " processed in " + duration + " ms";
        if( _logSlowThreshold != THRESHOLD_DISABLED && duration > _logSlowThreshold)
            _log.warn(logMsg.toString());
        else
            _log.info(logMsg.toString());


        if (! ((Message)pnfsMessage).getReplyRequired() ){
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
            _nameSpaceProvider.setStorageInfo(pnfsMessage.getSubject(), pnfsMessage.getPnfsId(), pnfsMessage.getStorageInfo(), NameSpaceProvider.SI_APPEND);
        } catch (CacheException e) {
            pnfsMessage.setFailed(e.getRc(), e.getMessage());
        } catch (RuntimeException e) {
            _log.warn("Failed to process flush notification", e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
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
                if (db == null || ((int) db) != id) {
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
        if (_threadGroups == 1)
            return 0;

        Integer db = _pathToDBCache.get(new FsPath(path));
        if (db != null) {
            return db % _threadGroups;
        }

        _log.info("Path cache miss for " + path);

        return (int) (Math.abs(path.hashCode())) % _threadGroups;
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
        Class msgClass = message.getClass();
        for (Class c: DISCARD_EARLY) {
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
                 * TODO: The 'classic' result of getStorageInfo was a
                 * cobination of fileMetadata + storageInfo. This was
                 * used add the owner and group information into
                 * sorageInfo's internal Map. Uid and Gid used by the
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
                attrs.getStorageInfo().setKey("path", message.getPnfsPath());
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
            _log.error("Error while retriving file attributes: " + e.getMessage(), e);
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

            _nameSpaceProvider.setFileAttributes(message.getSubject(),
                                                 pnfsId,
                                                 attr);
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
     * Returns the adjusted TTL of a message. The adjusted TTL is the
     * TTL with some time subtracted to allow for cell communication
     * overhead. Returns Long.MAX_VALUE if the TTL is infinite.
     */
    private static long getAdjustedTtl(CellMessage message)
    {
        long ttl = message.getTtl();
        return
            (ttl == Long.MAX_VALUE)
            ? Long.MAX_VALUE
            : ttl - Math.min(TTL_BUFFER_MAXIMUM,
                             (long) (ttl * TTL_BUFFER_FRACTION));
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
        throws PermissionDeniedCacheException
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
