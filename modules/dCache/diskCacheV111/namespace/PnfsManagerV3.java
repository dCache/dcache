// $Id: PnfsManagerV3.java,v 1.42 2007-10-02 07:11:45 tigran Exp $

package diskCacheV111.namespace;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;
import  diskCacheV111.namespace.provider.*;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

public class PnfsManagerV3 extends CellAdapter {


    private static final Logger _logDeveloper = Logger.getLogger(PnfsManagerV3.class.getName());
    private static final int THRESHOLD_DISABLED = 0;

    private final String      _cellName  ;
    private final Args        _args      ;
    private final CellNucleus _nucleus   ;
    private final Random      _random   = new Random(System.currentTimeMillis());
    private int          _threads  = 1 ;
    private int          _threadGroups  = 1 ;

    /**
     * Tasks queues used for cache location messges. Depending on
     * configuration, this may be the same as <code>_fifos</code>.
     */
    private final BlockingQueue<CellMessage> [] _locationFifos;

    /**
     * Tasks queues used for messages that do not operate on cache
     * locations.
     */
    private final BlockingQueue<CellMessage> [] _fifos    ;


    private CellPath     _cacheModificationRelay = null ;
    private boolean      _simulateLargeFiles     = false ;
    private boolean      _storeFilesize          = false ;
    private CellPath     _pnfsDeleteNotificationRelay = null;

    private final NameSpaceProvider     _nameSpaceProvider;
    private final CacheLocationProvider _cacheLocationProvider;
    private final StorageInfoProvider   _storageInfoProvider;
    private final static String defaultNameSpaceProvider      = "diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory";
    private final static String defaultCacheLocationProvider  = "diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory";
    private final static String defaultStorageInfoProvider    = "diskCacheV111.namespace.provider.BasicNameSpaceProviderFactory";


    /**
     * default access latency for newly created files
     */
    private final AccessLatency _defaultAccessLatency;

    /**
     * default retention policy for newly created files
     */
    private final RetentionPolicy _defaultRetentionPolicy;

    private static class StatItem {

        private final String _name ;
        int    _requests = 0 ;
        int    _failed   = 0 ;
        private  StatItem( String name ){ _name = name ; }
        private  void request(){ _requests ++ ; }
        private  void failed(){ _failed ++ ; }
        @Override
        public String toString(){
        	StringBuilder sb = new StringBuilder();

        	Formatter formatter = new Formatter(sb);

        	formatter.format("%-32s %9d %9d", new Object[]{_name,_requests,  _failed});
        	formatter.flush();
        	formatter.close();

            return sb.toString();
        }
    }
    private final StatItem _xaddCacheLocation      = new StatItem("addCacheLocation");
    private final StatItem _xclearCacheLocation    = new StatItem("clearCacheLocation");
    private final StatItem _xgetCacheLocations     = new StatItem("getCacheLocations");
    private final StatItem _xcreateDirectory       = new StatItem("createDirectory");
    private final StatItem _xcreateEntry           = new StatItem("createEntry");
    private final StatItem _xdeleteEntry           = new StatItem("deleteEntry");
    private final StatItem _xgetStorageInfo        = new StatItem("getStorageInfo");
    private final StatItem _xsetStorageInfo        = new StatItem("setStorageInfo");
    private final StatItem _xgetMetadataInfo       = new StatItem("getMetadataInfo");
    private final StatItem _xsetMetadataInfo       = new StatItem("setMetadataInfo");
    private final StatItem _xsetLength             = new StatItem("setLength");
    private final StatItem _xgetCacheStatistics    = new StatItem("getCacheStatistics");
    private final StatItem _xupdateCacheStatistics = new StatItem("updateCacheStatistics");
    private final StatItem _xrename                = new StatItem("rename");
    private final StatItem _xsetLevelData          = new StatItem("setLevelData");
    private final StatItem _xfileFlushed           = new StatItem("fileFlushed");
    private final StatItem _xmapPath2Id            = new StatItem("mapPath2Id");
    private final StatItem _xmapId2Path            = new StatItem("mapId2Path");
    private final StatItem _xflag                  = new StatItem("flag");
    private final StatItem _xgetChecksum           = new StatItem("getChecksum");
    private final StatItem _xsetChecksum           = new StatItem("setChecksum");
    private final StatItem _xlistChecksumTypes     = new StatItem("listChecksumTypes");

    private int _logSlowThreshold;

    private final StatItem [] _requestSet = {
            _xaddCacheLocation ,
            _xclearCacheLocation ,
            _xgetCacheLocations ,
            _xcreateDirectory ,
            _xcreateEntry ,
            _xdeleteEntry ,
            _xgetStorageInfo ,
            _xsetStorageInfo ,
            _xsetLength ,
            _xgetCacheStatistics ,
            _xupdateCacheStatistics ,
            _xrename,
            _xsetLevelData,
            _xfileFlushed,
            _xmapPath2Id,
            _xmapId2Path,
            _xgetMetadataInfo,
            _xsetMetadataInfo
    } ;


    public PnfsManagerV3( String cellName , String args ) throws Exception {

        super( cellName , PnfsManagerV3.class.getName(),  args , false ) ;

        _cellName = cellName ;
        _args     = getArgs() ;
        _nucleus  = getNucleus() ;

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
                        "-enableLargeFileSimulation "+
                        "-logSlowThreshold=<min. time in milliseconds> " +
                        "-storeFilesize "+
                "<StorageInfoExctractorClass>");

            //
            // get the thread multiplier
            //
            String tmp = _args.getOpt("threads") ;
            if( tmp != null ){
                try{
                    _threads = Integer.parseInt(tmp) ;
                }catch(NumberFormatException e){
                    esay( "Threads not changed ("+e+")");
                }
            }
            tmp = _args.getOpt("threadGroups") ;
            if( tmp != null ){
                try{
                    _threadGroups = Integer.parseInt(tmp) ;
                }catch(NumberFormatException e){
                    esay( "Thread groups not changed ("+e+")");
                }
            }
            tmp = _args.getOpt("cmRelay") ;
            if( tmp != null )_cacheModificationRelay = new CellPath(tmp) ;
            say("CacheModificationRelay = "+
                    ( _cacheModificationRelay == null ? "NONE" : _cacheModificationRelay.toString() ) );

            tmp = _args.getOpt("pnfsDeleteRelay") ;
            if( tmp != null )_pnfsDeleteNotificationRelay = new CellPath(tmp) ;
            say("pnfsDeleteRelay = "+
                    ( _pnfsDeleteNotificationRelay == null ? "NONE" : _pnfsDeleteNotificationRelay.toString() ) );

            tmp = _args.getOpt("logSlowThreshold") ;
           	_logSlowThreshold = (tmp != null) ? Integer.parseInt(tmp) : THRESHOLD_DISABLED;
            say("logSlowThreshold = "+
                    ( _logSlowThreshold == THRESHOLD_DISABLED ? "NONE" : Integer.toString(_logSlowThreshold) ) );

            //
            //
            _simulateLargeFiles = _args.getOpt("enableLargeFileSimulation") != null ;
            say("enableLargeFileSimulation = "+_simulateLargeFiles);
            String storeFilesize = _args.getOpt("storeFilesize");
            _storeFilesize = ( ( storeFilesize != null ) && ( ! storeFilesize.equals("off") ) ) || _simulateLargeFiles ;
            say("storeFilesize = "+_storeFilesize);

            String nameSpace_provider = _args.getOpt("namespace-provider") ;
            if( nameSpace_provider == null ) {
                nameSpace_provider = defaultNameSpaceProvider;
            }

            say("Namespace provider: " + nameSpace_provider);
            DcacheNameSpaceProviderFactory nameSpaceProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(nameSpace_provider).newInstance();
            _nameSpaceProvider = (NameSpaceProvider)nameSpaceProviderFactory.getProvider(_args, _nucleus);


            String cacheLocation_provider = _args.getOpt("cachelocation-provider") ;
            if( cacheLocation_provider == null ) {
                cacheLocation_provider = defaultCacheLocationProvider;
            }

            say("CacheLocation provider: " + cacheLocation_provider);
            DcacheNameSpaceProviderFactory cacheLocationProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(cacheLocation_provider).newInstance();
            _cacheLocationProvider = (CacheLocationProvider)cacheLocationProviderFactory.getProvider(_args, _nucleus);


            String storageInfo_provider = _args.getOpt("storageinfo-provider") ;
            if( storageInfo_provider == null ) {
                storageInfo_provider = defaultStorageInfoProvider;
            }

            say("StorageInfo provider: " + storageInfo_provider);
            DcacheNameSpaceProviderFactory storageInfoProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(storageInfo_provider).newInstance();
            _storageInfoProvider = (StorageInfoProvider)storageInfoProviderFactory.getProvider(_args, _nucleus);


            String accessLatensyOption = _args.getOpt("DefaultAccessLatency");
            if( accessLatensyOption != null && accessLatensyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultAccessLatency = AccessLatency.getAccessLatency(accessLatensyOption);
            }else{
                _defaultAccessLatency = AccessLatency.NEARLINE;
            }

            String retentionPolicyOption = _args.getOpt("DefaultRetentionPolicy");
            if( retentionPolicyOption != null && retentionPolicyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultRetentionPolicy = RetentionPolicy.getRetentionPolicy(retentionPolicyOption);
            }else{
                _defaultRetentionPolicy = RetentionPolicy.CUSTODIAL;
            }

            //
            // and now the threads and fifos
            //
            _fifos = new BlockingQueue[_threads * _threadGroups];
            say("Starting " + _fifos.length + " threads");
            for( int i = 0 ; i < _fifos.length ; i++ ){
                _fifos[i] = new LinkedBlockingQueue<CellMessage>() ;
                _nucleus.newThread( new ProcessThread(_fifos[i]), "proc-"+i ).start();
            }

            tmp = _args.getOpt("cachelocation-threads");
            int threads = (tmp == null) ? 0 : Integer.parseInt(tmp);
            if (threads > 0) {
                say("Starting " +  threads + " cache location threads");
                _locationFifos = new BlockingQueue[threads];
                for (int i = 0; i < _locationFifos.length; i++) {
                    _locationFifos[i] = new LinkedBlockingQueue();
                    _nucleus.newThread(new ProcessThread(_locationFifos[i]),
                                       "proc-loc-" + i).start();
                }
            } else {
                _locationFifos = _fifos;
            }
        } catch (Exception e){
            esay ("Exception occurred: "+e);
            start();
            kill();
            throw e;
        }
        //Make the cell name well-known
        getNucleus().export();
        start() ;
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
        pw.println( "StorageInfo Provider: " );
        pw.println( _storageInfoProvider.toString() );
        pw.println();
        pw.println("Default Access   Latency: " + _defaultAccessLatency);
        pw.println("Default Retention Policy: " + _defaultRetentionPolicy);
        pw.println();
        pw.println("Threads (" + _fifos.length + ") Queue");
        for (int i = 0; i < _fifos.length; i++) {
            pw.println("    [" + i + "] " + _fifos[i].size());
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
        for( int i = 0 , n = _requestSet.length ; i < n ; i++ ){
            pw.println("  " + _requestSet[i].toString());
        }
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
           pnfsId = _nameSpaceProvider.pathToPnfsid(args.argv(0), false);
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
                pnfsId = _nameSpaceProvider.pathToPnfsid( args.argv(0), true );
            }

            List<String> locations = _cacheLocationProvider.getCacheLocation( pnfsId );

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
        return _nameSpaceProvider.pnfsidToPath(pnfsId);
    }

    public String hh_rename = " # rename <old name> <new name>" ;
    public String ac_rename_$_1_2( Args args ){

        PnfsId    pnfsId   = null ;
        boolean isUnique = false;
        String newName = null;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;

        }catch(Exception ee ){
            try {
                pnfsId = _nameSpaceProvider.pathToPnfsid( args.argv(0), true );
            }catch(Exception e) {
                return "rename failed: " + e.getMessage() ;
            }
        }


        try {
            if ( args.argc() == 1 ) {
                isUnique = true;
            }else{
                newName = args.argv(1);
            }
            this.rename(pnfsId, newName , isUnique);
        }catch( Exception e) {
            return "rename failed: " + e.getMessage() ;
        }

        return "" ;
    }

    public String hh_set_meta = "<pnfsid>|<globalPath> <uid> <gid> <perm> <level1> ..." ;
    public String ac_set_meta_$_5_20( Args args )throws Exception {
        PnfsId    pnfsId   = null ;
        String reply = null;
        try {
            try{

                pnfsId   = new PnfsId( args.argv(0) ) ;

            }catch(Exception ee ){
                pnfsId = _nameSpaceProvider.pathToPnfsid( args.argv(0), true);
            }

            FileMetaData metaData = _nameSpaceProvider.getFileMetaData(pnfsId);

            int uid = Integer.parseInt(args.argv(1));
            int gid = Integer.parseInt(args.argv(2));
            int mode = Integer.parseInt(args.argv(3),8);

            FileMetaData newMetaData = new FileMetaData(metaData.isDirectory(), uid, gid, mode);

            _nameSpaceProvider.setFileMetaData( pnfsId , newMetaData );
            reply = "Ok";
        }catch(Exception e) {
            reply = "set metadata failed: + " + e.getMessage();
        }

        return reply ;
    }

    public String hh_set_storageinfo = "<pnfsid>|<globalPath> [-<option>=<value>] # depricated";
    public String ac_set_storageinfo_$_1(Args args) throws Exception {

        PnfsId pnfsId = null;
        String reply = "";

        try {

            try {

                pnfsId = new PnfsId(args.argv(0));

            } catch (Exception ee) {
                pnfsId = _nameSpaceProvider.pathToPnfsid(args.argv(0), true);
            }

            StorageInfo storageInfo = _storageInfoProvider.getStorageInfo(pnfsId);

            String accessLatency = args.getOpt("accessLatency");
            if( accessLatency != null ) {
                AccessLatency al = AccessLatency.getAccessLatency(accessLatency);
                if( !al.equals(storageInfo.getAccessLatency()) ) {
                    storageInfo.setAccessLatency(al);
                    storageInfo.isSetAccessLatency(true);
                    _storageInfoProvider.setStorageInfo(pnfsId, storageInfo, StorageInfoProvider.SI_OVERWRITE);

                    // get list of known locations and modify sticky flag
                    List<String> locations = _cacheLocationProvider.getCacheLocation(pnfsId);
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
                pnfsId = _nameSpaceProvider.pathToPnfsid( args.argv(0) , n ) ;

                if(v)sb.append("   Local Path : ").append(args.argv(0)).append("\n") ;
                if(v)sb.append("       PnfsId : ").append(pnfsId).append("\n") ;
            }

            StorageInfo info = _storageInfoProvider.getStorageInfo( pnfsId);
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
                pnfsId = _nameSpaceProvider.pathToPnfsid( args.argv(0) , n ) ;

                if(v)sb.append("   Local Path : ").append(args.argv(0)).append("\n") ;
                if(v)sb.append("       PnfsId : ").append(pnfsId).append("\n") ;
            }

            FileMetaData info = _nameSpaceProvider.getFileMetaData(  pnfsId ) ;
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
                _nameSpaceProvider.setFileAttribute(pnfsId, t, "");
            }else if( p > 0 ){
                _nameSpaceProvider.setFileAttribute(pnfsId, t.substring(0,p), t.substring(p+1));
            }
        }

        return "" ;
    }

    public String ac_flags_remove_$_2_99( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;

        for( int i = 1 ; i < args.argc() ; i++ ){
            String t = args.argv(i) ;
            _nameSpaceProvider.removeFileAttribute(pnfsId, t);
        }

        return "" ;
    }

    public String ac_flags_ls_$_1( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;
        String[] keys = _nameSpaceProvider.getFileAttributeList( pnfsId);

        StringBuffer sb = new StringBuffer();
        for( int i = 0; i < keys.length; i++) {
            sb.append(keys[i]).append(" -> ").
            append(  _nameSpaceProvider.getFileAttribute(pnfsId, keys[i] ) ).append("\n");

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

    public String hh_set_file_size = " <pnfsid> <new size> # DANGER";
    public String ac_set_file_size_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));
    	long size = Long.valueOf( args.argv(1));


    	FileMetaData dummyMetaData = new FileMetaData();

    	dummyMetaData.setSize(size);

    	_nameSpaceProvider.setFileMetaData(pnfsId, dummyMetaData);

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

    	_cacheLocationProvider.addCacheLocation(pnfsId, cacheLocation);

    	return "";

    }

    public String hh_clear_file_cache_location = "<pnfsid> <pool name>";
    public String ac_clear_file_cache_location_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));
    	String cacheLocation = args.argv(1);

    	_cacheLocationProvider.clearCacheLocation(pnfsId, cacheLocation, false);

    	return "";
    }

    public String hh_add_file_checksum = "<pnfsid> <type> <checksum>";
    public String ac_add_file_checksum_$_3(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));
    	_nameSpaceProvider.addChecksum(pnfsId, Integer.parseInt(args.argv(1)), args.argv(2));

    	return "";

    }

    public String hh_clear_file_checksum = "<pnfsid> <type>";
    public String ac_clear_file_checksum_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));

    	_nameSpaceProvider.removeChecksum(pnfsId, Integer.parseInt(args.argv(1)));

    	return "";
    }

    public String hh_get_file_checksum = "<pnfsid> <type>";
    public String ac_get_file_checksum_$_2(Args args) throws Exception {

    	PnfsId pnfsId = new PnfsId( args.argv(0));

    	String checkSum = _nameSpaceProvider.getChecksum(pnfsId, Integer.parseInt(args.argv(1)));

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

    private void dumpThreadQueue(int queueId) {
        if (queueId < 0 || queueId >= _fifos.length) {
            throw new IllegalArgumentException(" illegal queue #" + queueId);
        }
        BlockingQueue<CellMessage> fifo = _fifos[queueId];
        Object[] fifoContent = fifo.toArray();

        esay("PnfsManager thread #" + queueId + " queue dump (" +fifoContent.length+ "):");

        StringBuilder sb = new StringBuilder();

        for(int i = 0; i < fifoContent.length; i++) {
                   sb.append("fifo[").append(i).append("] : ");
                   sb.append(fifoContent[i]).append('\n');
        }

        esay( sb.toString() );
    }

    private void getChecksum(PnfsGetChecksumMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();
        int    type      = msg.getType();

        try{
            if(pnfsId == null ) {
                throw new InvalidMessageCacheException("no pnfsid defined");
            }
            String checksumValue = _nameSpaceProvider.getChecksum(pnfsId,type);
            msg.setValue(checksumValue);
        }catch( CacheException e ){
            esay(e) ;
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            esay(e) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }
        if( msg.getReturnCode() != 0 ) {
            _xgetChecksum.failed();
        }
    }

    private void listChecksumTypes(PnfsGetChecksumAllMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();

        try{
            int []types = _nameSpaceProvider.listChecksumTypes(pnfsId);
            msg.setValue(types);
        }catch( CacheException e ){
            esay(e) ;
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            esay(e) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }
        if( msg.getReturnCode() != 0 ) {
            _xlistChecksumTypes.failed();
        }
    }

    private void setChecksum(PnfsSetChecksumMessage msg){

        PnfsId pnfsId    = msg.getPnfsId();
        String value     = msg.getValue() ;
        int type         = msg.getType();

        try{
            if (value != null) {
                String old = _nameSpaceProvider.getChecksum(pnfsId, type);
                if (old != null) {
                    byte[] oldSum = Checksum.stringToBytes(old);
                    byte[] newSum = Checksum.stringToBytes(value);
                    if (!oldSum.equals(newSum)) {
                        throw new CacheException(CacheException.INVALID_ARGS,
                                                 "Checksum mismatch");
                    }
                }
            }

           _nameSpaceProvider.addChecksum(pnfsId,type,value);
        }catch( CacheException e ){
            esay(e);
            msg.setFailed( e.getRc() , e.getMessage() ) ;
        }catch ( Exception e){
            esay(e) ;
            msg.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }

        if( msg.getReturnCode() != 0 ) {
            _xsetChecksum.failed();
        }

    }


    private void updateFlag(PnfsFlagMessage pnfsMessage){

        PnfsId pnfsId    = pnfsMessage.getPnfsId();
        PnfsFlagMessage.FlagOperation operation = pnfsMessage.getOperation() ;
        String flagName  = pnfsMessage.getFlagName() ;
        String value     = pnfsMessage.getValue() ;
        say("update flag "+operation+" flag="+flagName+" value="+
                value+" for "+pnfsId);

        try{

            if( operation == PnfsFlagMessage.FlagOperation.GET ){
                pnfsMessage.setValue( updateFlag( pnfsId , operation , flagName , value ) );
            }else{
                updateFlag( pnfsId , operation , flagName , value );
            }

        }catch( Exception e ){
            esay("Exception in updateFlag "+e);
            esay(e) ;
            pnfsMessage.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xflag.failed();
        }

    }
    private String updateFlag(PnfsId pnfsId, PnfsFlagMessage.FlagOperation operation, String flagName,
            String value) throws Exception {

        switch (operation) {

            case SET:
                say("flags set " + pnfsId + " " + flagName + "=" + value);
                _nameSpaceProvider.setFileAttribute(pnfsId, flagName, value);
                break;
            case SETNOOVERWRITE:
                say("flags set (dontoverwrite) " + pnfsId + " " + flagName + "=" + value);
                String x = (String) _nameSpaceProvider.getFileAttribute(pnfsId, flagName);
                if ((x == null) || (!x.equals(value))) {
                    say("flags set " + pnfsId + " " + flagName + "=" + value);
                    _nameSpaceProvider.setFileAttribute(pnfsId, flagName, value);
                }
                break;
            case GET:
                String v = (String) _nameSpaceProvider.getFileAttribute(pnfsId, flagName);
                say("flags ls " + pnfsId + " " + flagName + " -> " + v);
                return v;
            case REMOVE:
                say("flags remove " + pnfsId + " " + flagName);
                _nameSpaceProvider.removeFileAttribute(pnfsId, flagName);
                break;
        }
        return null;
    }

    public void addCacheLocation(PnfsAddCacheLocationMessage pnfsMessage){
        say("addCacheLocation : "+pnfsMessage.getPoolName()+" for "+pnfsMessage.getPnfsId());
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
            _cacheLocationProvider.addCacheLocation(pnfsMessage.getPnfsId(),
                                                    pnfsMessage.getPoolName());
        } catch (FileNotFoundCacheException fnf ) {
        	pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (Exception e){
            esay("Exception in addCacheLocation "+e);
            esay(e) ;
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,"Exception in addCacheLocation");
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xaddCacheLocation.failed() ;
        }

    }

    public void clearCacheLocation(PnfsClearCacheLocationMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("clearCacheLocation : "+pnfsMessage.getPoolName()+" for "+pnfsId);
        try {
            _cacheLocationProvider.clearCacheLocation( pnfsId , pnfsMessage.getPoolName(), pnfsMessage.removeIfLast() );
        } catch (FileNotFoundCacheException fnf ) {
            	pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (Exception e){
            esay("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            esay(e) ;
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage() );
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xclearCacheLocation.failed() ;
        }

    }

    public void getCacheLocations(PnfsGetCacheLocationsMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        try {
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String  pnfsPath    = pnfsMessage.getPnfsPath() ;
                if(pnfsPath == null ) {
                    throw new InvalidMessageCacheException("no pnfsid or path defined");
                }

                say("get cache locations (by path) global : "+pnfsPath  ) ;
                PnfsId     id       = _nameSpaceProvider.pathToPnfsid( pnfsPath, true);

                if( id == null ) {
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                }
                pnfsId = id ;
                pnfsMessage.setPnfsId(pnfsId);
            }

            say("get cache locations for "+pnfsId);

            pnfsMessage.setCacheLocations( _cacheLocationProvider.getCacheLocation(pnfsId) );
            pnfsMessage.setSucceeded();
        } catch (FileNotFoundCacheException fnf ) {
        	pnfsMessage.setFailed(CacheException.FILE_NOT_FOUND, fnf.getMessage() );
        } catch (Exception exc){
            esay("Exception in getCacheLocations "+exc);
            esay(exc) ;
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,"Pnfs lookup failed");
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xgetCacheLocations.failed() ;
        }

    }

    public void createDirectory(PnfsCreateDirectoryMessage pnfsMessage){
        PnfsId pnfsId = null;
        say("create directory "+pnfsMessage.getPath());
        try {
        	FileMetaData metadata = new FileMetaData(true, pnfsMessage.getUid(), pnfsMessage.getGid(), pnfsMessage.getMode());
            pnfsId = _nameSpaceProvider.createEntry( pnfsMessage.getPath(), metadata, true );

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
                say( "Trying to get storageInfo for "+pnfsId) ;

                StorageInfo info = _storageInfoProvider.getStorageInfo( pnfsId)  ;

                pnfsMessage.setStorageInfo( info ) ;
                pnfsMessage.setMetaData( _nameSpaceProvider.getFileMetaData( pnfsId ) ) ;

            }catch(Exception eeee){
                esay( "Can't determine storageInfo : "+eeee ) ;
            }

        }catch(CacheException fe) {
            pnfsMessage.setFailed(fe.getRc(), fe.getMessage());
        }catch ( Exception ia ) {
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, ia.getMessage());
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xcreateDirectory.failed() ;
        }

    }

    public void createEntry(PnfsCreateEntryMessage pnfsMessage){

        PnfsId pnfsId = null;
        say("create entry "+pnfsMessage.getPath());
        try {
        	FileMetaData metadata = new FileMetaData(false, pnfsMessage.getUid(), pnfsMessage.getGid(), pnfsMessage.getMode());
            pnfsId = _nameSpaceProvider.createEntry( pnfsMessage.getPath(),metadata, false );


            /*
             * apply defaults
             *
             * TODO: as soon as pnfs obsolete we can move this functionality in Chimera
             * as a database trigger
             *
             */
             StorageInfo si = new GenericStorageInfo();
             si.setAccessLatency(_defaultAccessLatency);
             si.isSetAccessLatency(true);
             si.setRetentionPolicy(_defaultRetentionPolicy);
             si.isSetRetentionPolicy(true);
             _storageInfoProvider.setStorageInfo(pnfsId, si, StorageInfoProvider.SI_OVERWRITE);

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
                say( "Trying to get storageInfo for "+pnfsId) ;

                StorageInfo info = _storageInfoProvider.getStorageInfo( pnfsId)  ;
                info.setKey("path", pnfsMessage.getPath() );

                /*
                 * due to legacy use of level to probe isNew() and while storing default
                 * access latency and retention policy on create breaking this rule we have to
                 * fix it.
                 */
                info.setIsNew(true);

                pnfsMessage.setStorageInfo( info ) ;
                pnfsMessage.setMetaData( _nameSpaceProvider.getFileMetaData( pnfsId ) ) ;

            }catch(Exception eeee){
                esay( "Can't determine storageInfo : "+eeee ) ;
            }

        }catch (CacheException ce) {
            pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
        }catch ( Exception ia ) {
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, ia.getMessage());
            esay(ia);
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xcreateEntry.failed() ;
        }

    }

    public void setStorageInfo( PnfsSetStorageInfoMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String  pnfsPath    = pnfsMessage.getPnfsPath() ;
                if(pnfsPath == null ) {
                    throw new InvalidMessageCacheException("no pnfsid or path defined");
                }
                say("setStorageInfo (by path) global : "+pnfsPath  ) ;
                PnfsId     id       = _nameSpaceProvider.pathToPnfsid( pnfsPath, true);

                if( id == null ) {
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                }
                pnfsId = id ;
            }
            say( "setStorageInfo : "+pnfsId ) ;

            _storageInfoProvider.setStorageInfo( pnfsId , pnfsMessage.getStorageInfo() , pnfsMessage.getAccessMode() ) ;

        }catch(FileNotFoundCacheException fnf) {
        	// file is gone.....
        	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch(CacheException ee ){
            esay( "Failed : "+ee ) ;
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , iee.getMessage() ) ;
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xsetStorageInfo.failed() ;
        }

    }

    public void getStorageInfo( PnfsGetStorageInfoMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String  pnfsPath    = pnfsMessage.getPnfsPath() ;
                if(pnfsPath == null ) {
                    throw new InvalidMessageCacheException("no pnfsid or path defined");
                }

                say("getStorageInfo (by path) global : "+pnfsPath  ) ;
                PnfsId     id       = _nameSpaceProvider.pathToPnfsid( pnfsPath, true);

                if( id == null ) {
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                }
                pnfsId = id ;
                pnfsMessage.setPnfsId(pnfsId);
            }
            say( "getStorageInfo : "+pnfsId ) ;

            StorageInfo info = _storageInfoProvider.getStorageInfo( pnfsId) ;
            pnfsMessage.setStorageInfo(info ) ;
            pnfsMessage.setPnfsId( pnfsId ) ;
            pnfsMessage.setSucceeded() ;
            say( "Storage info "+info ) ;
            pnfsMessage.setMetaData( _nameSpaceProvider.getFileMetaData( pnfsId ) ) ;
            if (pnfsMessage.isChecksumsRequested()) {
                pnfsMessage.setChecksums(_nameSpaceProvider.getChecksums(pnfsId));
            }

        }catch(FileNotFoundCacheException fnf) {
        	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch(CacheException ee ){
            //
            // won't use esay here because this is not really an error. Some
            // services are using this trick just to findout if the file exists,
            // where it is fine that it doesn't.
            //
            say( "Failed : "+ee ) ;
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , iee.getMessage() ) ;
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xgetStorageInfo.failed() ;
        }

    }
    public void getFileMetaData( PnfsGetFileMetaDataMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        boolean resolve = pnfsMessage.resolve() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String pnfsPath = pnfsMessage.getPnfsPath();
                if(pnfsPath == null ) {
                    throw new InvalidMessageCacheException("no pnfsid or path defined");
                }

                say("getFileMetaData (by path) : "+  pnfsPath ) ;
                PnfsId   id         = _nameSpaceProvider.pathToPnfsid(pnfsPath, resolve );
                if( id == null ) {
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                }
                pnfsId = id ;
                pnfsMessage.setPnfsId(pnfsId);
            }
            say( "getFileMetaData : "+pnfsId ) ;

            pnfsMessage.setPnfsId( pnfsId ) ;
            pnfsMessage.setSucceeded() ;
            pnfsMessage.setMetaData( _nameSpaceProvider.getFileMetaData( pnfsId ) ) ;

            if (pnfsMessage.isChecksumsRequested()) {
                pnfsMessage.setChecksums(_nameSpaceProvider.getChecksums(pnfsId));
            }


        }catch(FileNotFoundCacheException fnf ) {
        	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch(CacheException ee ){
            esay( "Failed : "+ee ) ;
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , iee.getMessage() ) ;
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xgetMetadataInfo.failed();
        }

    }

    public void setFileMetaData( PnfsSetFileMetaDataMessage pnfsMessage ) {
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        FileMetaData meta = pnfsMessage.getMetaData() ;
        say( "setFileMetaData="+meta+" for "+pnfsId ) ;

        try {
            _nameSpaceProvider.setFileMetaData(pnfsId, meta);
        }catch ( Exception e) {
            esay(e);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                  e.getMessage());
        }

        if (pnfsMessage.getReturnCode() != 0) {
            _xsetMetadataInfo.failed();
        }

        return ;

    }

    public void deleteEntry(PnfsDeleteEntryMessage pnfsMessage){

        String path = pnfsMessage.getPath();
        PnfsId pnfsId = pnfsMessage.getPnfsId();

        try {

            if( path == null && pnfsId == null) {
                throw new CacheException(CacheException.INVALID_ARGS, "pnfsid or path have to be defined for PnfsDeleteEntryMessage");
            }

            if( path != null ) {

                PnfsId pnfsIdFromPath = _nameSpaceProvider.pathToPnfsid(path, false);

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
                        throw new FileNotFoundCacheException("pnfsid do not corresopnds to provided file");
                    }
                } else {
                	pnfsId = pnfsIdFromPath;
                }

                say("delete PNFS entry for "+ path );
                _nameSpaceProvider.deleteEntry(path);
            } else {
                say("delete PNFS entry for "+ pnfsId );
                _nameSpaceProvider.deleteEntry( pnfsId );
            }

            pnfsMessage.setSucceeded();

        }catch(FileNotFoundCacheException fnf) {
            	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch( CacheException ce) {
        	pnfsMessage.setFailed( ce.getRc() , ce.getMessage() ) ;
        	esay("delete entry: " + ce.getMessage());
        } catch (Exception e) {
            esay("delete failed "+e);
            esay(e) ;
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e);
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xdeleteEntry.failed() ;
        } else if( _pnfsDeleteNotificationRelay != null ) {
            PnfsDeleteEntryNotificationMessage deleteNotification =
                new PnfsDeleteEntryNotificationMessage(pnfsId,path);
            try{

                sendMessage( new CellMessage( _pnfsDeleteNotificationRelay,
                    deleteNotification ) ) ;

            }catch(Exception ee ){
                esay("Problem "+ee.getMessage()+" relaying to "+
                    _cacheModificationRelay+" : "+deleteNotification ) ;
            }
        }


    }

    public void setLength(PnfsSetLengthMessage pnfsMessage){

        PnfsId pnfsId = pnfsMessage.getPnfsId();
        long   length = pnfsMessage.getLength();

        say("Set length of "+pnfsId+" to "+length+" (Simulate Large = "+_simulateLargeFiles+"; store size = "+_storeFilesize+")");
        try {
            if( _storeFilesize )updateFlag( pnfsId , PnfsFlagMessage.FlagOperation.SET , "l" , ""+length ) ;

            FileMetaData metadata = _nameSpaceProvider.getFileMetaData(pnfsId);
            if( _simulateLargeFiles ){
                metadata.setSize( length > 0x7fffffffL ? 1L : length );
            }else{
                metadata.setSize( length );
            }
            _nameSpaceProvider.setFileMetaData(pnfsId,metadata );
        }catch(FileNotFoundCacheException fnf) {
        	// file is gone.....
        	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch (CacheException ce) {
        	pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
        	esay("setLength failed: " + ce.getMessage());
        }catch(Exception exc){
            esay("Exception in setLength"+exc);
            esay(exc);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,"Pnfs lookup failed");
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xsetLength.failed();
        }
    }


    public void rename(PnfsRenameMessage pnfsMessage) {

        PnfsId pnfsId = pnfsMessage.getPnfsId();
        String newName = pnfsMessage.newName();
        say(" rename "+pnfsId+" to new name : "+newName);
        try {
            this.rename(pnfsId, newName , pnfsMessage.isUnique() );
        }catch( Exception exc){
            esay("Exception in rename "+exc);
            esay(exc);
            _xrename.failed() ;
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,"Pnfs rename failed");
        }

    }

    private void rename(PnfsId pnfsId, String newName, boolean makeUnique) throws Exception {


        if( (newName == null) && makeUnique) {

            StringBuffer uniqueName = new StringBuffer( _nameSpaceProvider.pnfsidToPath(pnfsId) );
            uniqueName.append(";").append( uniqueName.hashCode() ).append("_").append( new Date( System.currentTimeMillis() ) );
            newName = uniqueName.toString();
        }

        say("Renaming " + pnfsId + " to " + newName );
        _nameSpaceProvider.renameEntry(pnfsId, newName);
    }


    private void removeByPnfsId( PnfsId pnfsId ) throws Exception {

        say("removeByPnfsId : "+pnfsId );

        _nameSpaceProvider.deleteEntry(pnfsId);

        return ;
    }

    private String pathfinder( PnfsId pnfsId ) throws Exception {
        return _nameSpaceProvider.pnfsidToPath(pnfsId);
    }

    public void mapPath( PnfsMapPathMessage pnfsMessage ){
        PnfsId pnfsId     = pnfsMessage.getPnfsId() ;
        String globalPath = pnfsMessage.getGlobalPath() ;

        if( ( pnfsId == null ) && ( globalPath == null ) ){
            pnfsMessage.setFailed( 5 , "Illegal Arguments : need path or pnfsid" ) ;
            return ;
        }

        try {
            if (globalPath == null) {
                say("map:  id2path for " + pnfsId);
                pnfsMessage.setGlobalPath(pathfinder(pnfsId));
            } else {
                say("map:  path2id for " + globalPath);
                pnfsMessage.setPnfsId(_nameSpaceProvider.pathToPnfsid( globalPath, false));
            }
        } catch(FileNotFoundCacheException fnf){
        	pnfsMessage.setFailed( CacheException.FILE_NOT_FOUND , fnf.getMessage() ) ;
        }catch (CacheException ce) {
        	pnfsMessage.setFailed(ce.getRc(), ce.getMessage());
        	esay("mapPath: " + ce.getMessage());
        } catch (Exception eee) {
            esay("Exception in mapPath (pathfinder) " + eee);
            esay(eee);
            pnfsMessage.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, eee);
        }

        if( pnfsMessage.getReturnCode() != 0 ) {
            _xmapPath2Id.failed();
        }
    }

    public void getCacheStatistics(PnfsGetCacheStatisticsMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("get cache statistics for "+pnfsId);
        pnfsMessage.setFailed( 5 , "Not supported" ) ;
    }

    public void updateCacheStatistics(PnfsUpdateCacheStatisticsMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("update cache statistics for "+pnfsId);
        pnfsMessage.setFailed( 5 , "Not supported" ) ;
    }


    private class ProcessThread implements Runnable {
        private final BlockingQueue<CellMessage> _fifo ;
        private ProcessThread( BlockingQueue<CellMessage> fifo ){ _fifo = fifo ; }
        public void run(){

            say("Thread <"+Thread.currentThread().getName()+"> started");

            boolean done = false;
            while( !done ){
            	CellMessage message;
            	try {
                    message = _fifo.take();
                } catch (InterruptedException e) {
                    done = true;
                    continue;
                }

                PnfsMessage pnfsMessage = (PnfsMessage)message.getMessageObject() ;
                CDC.setMessageContext(message);
                try{
                    processPnfsMessage( message , pnfsMessage );
                }catch(Throwable processException ){
                    esay( "processPnfsMessage : "+
                            Thread.currentThread().getName()+" : "+
                            processException );
                } finally {
                    CDC.clearMessageContext();
                }
            }
            say("Thread <"+Thread.currentThread().getName()+"> finished");
        }
    }

    @Override
    public void messageArrived( CellMessage message ){

        Object pnfsMessage  = message.getMessageObject();
        if (! (pnfsMessage instanceof Message) ){
            say("Unexpected message class "+pnfsMessage.getClass());
            say("source = "+message.getSourceAddress());
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

        boolean isCacheOperation =
            ((pnfs instanceof PnfsAddCacheLocationMessage) ||
             (pnfs instanceof PnfsClearCacheLocationMessage) ||
             (pnfs instanceof PnfsGetCacheLocationsMessage));
        BlockingQueue<CellMessage> fifo;
        if (isCacheOperation && _locationFifos != _fifos) {
            int index;
            if (pnfsId != null) {
                index = (Math.abs(pnfsId.hashCode()) % _locationFifos.length);
                say("Using location thread [" + pnfsId + "] " + index);
            } else {
                index = _random.nextInt(_locationFifos.length);
                say("Using location thread [" + path + "] " + index);
            }
            fifo = _locationFifos[index];
        } else {
            int index;
            if (pnfsId != null) {
                index =
                    (pnfsId.getDatabaseId() % _threadGroups) * _threads +
                    (Math.abs(pnfsId.hashCode()) % _threads);
                say("Using thread [" + pnfsId + "] " + index);
            } else if (path != null) {
                index = Math.abs(path.hashCode()) % _fifos.length;
                say("Using thread [" + path + "] " + index);
            }else{
                index = _random.nextInt(_fifos.length);
                say("Using thread [" + pnfsId + "] " + index);
            }
            fifo = _fifos[index];
        }

        try {
            fifo.put( message ) ;
        } catch (InterruptedException e) {
            esay("failed to add a message into queue "+e.getMessage()) ;
        }
    }
    private void forwardModifyCacheLocationMessage( PnfsMessage message ){
        try{

            sendMessage( new CellMessage( _cacheModificationRelay , message ) ) ;

        }catch(Exception ee ){
            esay("Problem "+ee.getMessage()+" relaying to "+_cacheModificationRelay+" : "+message ) ;
        }
    }
    public void processPnfsMessage( CellMessage message , PnfsMessage pnfsMessage ){

        long ctime = System.currentTimeMillis();

        if (pnfsMessage instanceof PnfsAddCacheLocationMessage){
            _xaddCacheLocation.request() ;
            addCacheLocation((PnfsAddCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsClearCacheLocationMessage){
            _xclearCacheLocation.request() ;
            clearCacheLocation((PnfsClearCacheLocationMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetCacheLocationsMessage){
            _xgetCacheLocations.request() ;
            getCacheLocations((PnfsGetCacheLocationsMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateDirectoryMessage){
            _xcreateDirectory.request() ;
            createDirectory((PnfsCreateDirectoryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsCreateEntryMessage){
            _xcreateEntry.request() ;
            createEntry((PnfsCreateEntryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsDeleteEntryMessage){
            _xdeleteEntry.request() ;
            deleteEntry((PnfsDeleteEntryMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetStorageInfoMessage){
            _xgetStorageInfo.request() ;
            getStorageInfo((PnfsGetStorageInfoMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetStorageInfoMessage){
            _xsetStorageInfo.request() ;
            setStorageInfo((PnfsSetStorageInfoMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetFileMetaDataMessage){
            _xgetMetadataInfo.request() ;
            getFileMetaData((PnfsGetFileMetaDataMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetFileMetaDataMessage){
            _xsetMetadataInfo.request() ;
            setFileMetaData((PnfsSetFileMetaDataMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetLengthMessage){
            _xsetLength.request() ;
            setLength((PnfsSetLengthMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsGetCacheStatisticsMessage){
            _xgetCacheStatistics.request() ;
            getCacheStatistics((PnfsGetCacheStatisticsMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsUpdateCacheStatisticsMessage){
            _xupdateCacheStatistics.request() ;
            updateCacheStatistics((PnfsUpdateCacheStatisticsMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsMapPathMessage){
            _xmapPath2Id.request();
            mapPath((PnfsMapPathMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsRenameMessage){
            _xrename.request();
            rename((PnfsRenameMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsFlagMessage){
            _xflag.request();
            updateFlag((PnfsFlagMessage)pnfsMessage);
        }
        else if ( pnfsMessage instanceof PnfsSetChecksumMessage){
            _xsetChecksum.request();
            setChecksum((PnfsSetChecksumMessage)pnfsMessage);
        }
        else if ( pnfsMessage instanceof PnfsGetChecksumMessage){
            _xgetChecksum.request();
            getChecksum((PnfsGetChecksumMessage)pnfsMessage);
        }
        else if( pnfsMessage instanceof PoolFileFlushedMessage ) {
        	_xfileFlushed.request();
        	processFlushMessage((PoolFileFlushedMessage) pnfsMessage );
        }
        else if ( pnfsMessage instanceof PnfsGetChecksumAllMessage ){
            _xlistChecksumTypes.request();
            listChecksumTypes((PnfsGetChecksumAllMessage)pnfsMessage);
        }

        else {
            say("Unexpected message class "+pnfsMessage.getClass());
            say("source = "+message.getSourceAddress());
            return;
        }

        if( pnfsMessage.getReturnCode() == CacheException.INVALID_ARGS ) {
            _logDeveloper.error("Inconsistent message " + pnfsMessage.getClass() + " received form " + message.getSourceAddress() );
        }

        long duration = System.currentTimeMillis() - ctime;
        String logMsg = pnfsMessage.getClass() + " processed in " + duration + " ms";
        if( _logSlowThreshold != THRESHOLD_DISABLED && duration > _logSlowThreshold)
        	esay( logMsg);
        else
        	say( logMsg);


        if (! ((Message)pnfsMessage).getReplyRequired() ){
            return;
        }
        try {
            message.revertDirection();
            sendMessage(message);
        } catch (Exception e){
            esay("Exception sending message "+e);
            esay(e);
        }
    }

    public void processFlushMessage(PoolFileFlushedMessage pnfsMessage) {

		try {

		    _storageInfoProvider.setStorageInfo(pnfsMessage.getPnfsId(), pnfsMessage.getStorageInfo(), StorageInfoProvider.SI_APPEND);

		}catch(CacheException ce) {
			pnfsMessage.setFailed( ce.getRc() , ce.getMessage() ) ;
		}catch(Exception e) {
			pnfsMessage.setFailed( CacheException.UNEXPECTED_SYSTEM_EXCEPTION , e.getMessage() ) ;
			esay(e);
		}

                if (pnfsMessage.getReturnCode() != 0) {
                    _xfileFlushed.failed();
                }
	}

	public static int fileMetaDataToUnixMode( FileMetaData meta){

        int mode = 0;


        //FIXME: to be done more elegant

        // user
        if( meta.getUserPermissions().canRead() ) {
            mode |= 0400;
        }

        if( meta.getUserPermissions().canWrite() ) {
            mode |= 0200;
        }
        if( meta.getUserPermissions().canExecute() ) {
            mode |= 0100;
        }


        // group
        if( meta.getGroupPermissions().canRead() ) {
            mode |= 0040;
        }

        if( meta.getGroupPermissions().canWrite() ) {
            mode |= 0020;
        }
        if( meta.getGroupPermissions().canExecute() ) {
            mode |= 0010;
        }

        // world
        if( meta.getWorldPermissions().canRead() ) {
            mode |= 0004;
        }

        if( meta.getWorldPermissions().canWrite() ) {
            mode |= 0002;
        }
        if( meta.getWorldPermissions().canExecute() ) {
            mode |= 0001;
        }

        return mode;
    }
}
