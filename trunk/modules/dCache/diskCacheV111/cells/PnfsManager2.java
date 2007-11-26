// $Id: PnfsManager2.java,v 1.46 2007-07-26 13:42:45 tigran Exp $

package diskCacheV111.cells ;

import  diskCacheV111.vehicles.*;
import  diskCacheV111.util.*;

import  dmg.cells.nucleus.* ;
import  dmg.util.* ;

import  java.io.* ;
import  java.util.*;

public class PnfsManager2 extends CellAdapter {
    private final String      _cellName   ;
    private final Args        _args       ;
    private final CellNucleus _nucleus   ;
    private final StorageInfoExtractable _extractor;
    private final Random       _random    = new Random(System.currentTimeMillis());
    private int          _threads    = 1 ;
    private final SyncFifo2 [] _fifos    ;

    private List         _virtualMountPoints     = null ;
    private PathManager  _pathManager            = null ;
    private CellPath     _cacheModificationRelay = null ;
    private boolean      _simulateLargeFiles     = false ;

    private static class StatItem {
        private final String _name ;
        long    _requests = 0 ;
        long   _failed   = 0 ;
        private  StatItem( String name ){ _name = name ; }
        private  void request(){ _requests ++ ; }
        private  void failed(){ _failed ++ ; }
        public String toString(){
            return _name+" "+_requests+" "+_failed ;
        }
    }
    private StatItem _xaddCacheLocation      = new StatItem("addClacheLocation");
    private StatItem _xclearCacheLocation    = new StatItem("clearClacheLocation");
    private StatItem _xgetCacheLocations     = new StatItem("getClacheLocations");
    private StatItem _xcreateDirectory       = new StatItem("createDirectory");
    private StatItem _xcreateEntry           = new StatItem("createEntry");
    private StatItem _xdeleteEntry           = new StatItem("deleteEntry");
    private StatItem _xgetStorageInfo        = new StatItem("getStorageInfo");
    private StatItem _xsetStorageInfo        = new StatItem("setStorageInfo");
    private StatItem _xsetLength             = new StatItem("setLength");
    private StatItem _xgetCacheStatistics    = new StatItem("getCacheStatistics");
    private StatItem _xupdateCacheStatistics = new StatItem("updateCacheStatistics");
    private StatItem _xupdateFlag            = new StatItem("updateFlag");
    private StatItem _xmapPath               = new StatItem("mapPath");
    private StatItem _xrename                = new StatItem("rename");

    private StatItem [] _requestSet = {
        _xaddCacheLocation ,
        _xclearCacheLocation ,
        _xgetCacheLocations ,
        _xcreateDirectory ,
        _xcreateEntry ,
        _xdeleteEntry ,
        _xgetStorageInfo ,
        _xsetStorageInfo ,
        _xsetLength ,
        _xgetCacheStatistics  ,
        _xupdateCacheStatistics  ,
        _xupdateFlag  ,
        _xmapPath,
        _xrename

    } ;
    public PnfsManager2( String cellName , String args ) throws Exception {

        super( cellName , args , false ) ;

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
                "<StorageInfoExctractorClass>");

            //
            // get the extractor
            //
            Class exClass = Class.forName( _args.argv(0)) ;
            _extractor = (StorageInfoExtractable)exClass.newInstance() ;
            //
            // get the thread multiplier
            //
            String tmp = _args.getOpt("threads") ;
            if( tmp != null ){
                try{
                    _threads = Integer.parseInt(tmp) ;
                }catch(Exception e){
                    esay( "Threads not changed ("+e+")");
                }
            }
            tmp = _args.getOpt("cmRelay") ;
            if( tmp != null )_cacheModificationRelay = new CellPath(tmp) ;
            say("CacheModificationRelay = "+
            ( _cacheModificationRelay == null ? "NONE" : _cacheModificationRelay.toString() ) );
            //
            //
            _simulateLargeFiles = _args.getOpt("enableLargeFileSimulation") != null ;
            say("enableLargeFileSimulation = "+_simulateLargeFiles);

            //
            // get our filesystems
            //
            String mountPoint = _args.getOpt("pnfs")  ;
            if( ( mountPoint != null ) && ( ! mountPoint.equals("") ) ){
                say( "PnfsFilesystem enforced : "+mountPoint) ;
                PnfsFile pf = new PnfsFile(mountPoint);
                if( ! (pf.isDirectory() && pf.isPnfs() ) )
                    throw new
                    IllegalArgumentException(
                    "not a pnfs directory: "+mountPoint);
                _virtualMountPoints = PnfsFile.getVirtualMountPoints(new File(mountPoint));

            }else{
                //
                // autodetection of pnfs filesystems
                //
                say( "Starting PNFS autodetect" ) ;
                _virtualMountPoints = PnfsFile.getVirtualMountPoints();
            }
            if( _virtualMountPoints.size() == 0 )
                throw new
                Exception("No mountpoints left ... ");

            Iterator mp = _virtualMountPoints.iterator() ;
            while( mp.hasNext() ){
                PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)mp.next() ;
                say( " Server         : "+vmp.getServerId()+"("+vmp.getServerName()+")" ) ;
                say( " RealMountPoint : "+vmp.getRealMountId()+" "+vmp.getRealMountPoint() ) ;
                say( "       VirtualMountId : "+vmp.getVirtualMountId() ) ;
                say( "      VirtualPnfsPath : "+vmp.getVirtualPnfsPath() ) ;
                say( "     VirtualLocalPath : "+vmp.getVirtualLocalPath() ) ;
                say( "    VirtualGlobalPath : "+vmp.getVirtualGlobalPath() ) ;
            }

            _pathManager = new PathManager( _virtualMountPoints ) ;
            String defaultServerName = _args.getOpt("defaultPnfsServer") ;

            if( ( _pathManager.getServerCount() > 2  ) &&
            ( ( defaultServerName == null ) ||
            ( defaultServerName.equals("")   ) ) )
                throw new
                IllegalArgumentException("No default server specified") ;

            if( ( defaultServerName != null ) &&
            ( ! defaultServerName.equals("*") ) )
                _pathManager.setDefaultServerName( defaultServerName ) ;

            say("Using default pnfs server : "+_pathManager.getDefaultServerName());
            //
            // and now the thread and fifos
            //
            say("Starting "+_threads+" threads");
            _fifos = new SyncFifo2[_threads] ;
            for( int i = 0 ; i < _threads ; i++ ){
                _fifos[i] = new SyncFifo2() ;
                _nucleus.newThread( new ProcessThread(_fifos[i]), "proc-"+i ).start();
            }
        } catch (Exception e){
            say("Exception occurred: "+e);
            start();
            kill();
            throw e;
        }
        //Make the cell name well-known
        getNucleus().export();
        start() ;
    }
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.46 $" ); }

    private class PathManager {
        private PathMap _globalPathMap = new PathMap() ;
        private Map     _servers       = new HashMap() ;
        private File    _defaultMountPoint = null ;
        private String  _defaultServerName = null ;

        private PathManager( List virtualMountPoints ){
            Iterator mp = _virtualMountPoints.iterator() ;
            while( mp.hasNext() ){
                PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)mp.next() ;
                _globalPathMap.add( vmp.getVirtualGlobalPath() , vmp ) ;
                _servers.put( vmp.getServerName()  , vmp ) ;
            }
            if( _servers.size() == 0 )
                throw new
                NoSuchElementException("Emtpy server list" ) ;

            PnfsFile.VirtualMountPoint vmp =
            (PnfsFile.VirtualMountPoint)virtualMountPoints.get(0);

            _defaultServerName = vmp.getServerName() ;
            _defaultMountPoint = getVmpByDomain( _defaultServerName ).getRealMountPoint()  ;


        }
        private PnfsFile.VirtualMountPoint getVmpByDomain( String domain ){
            domain = ( domain == null ) || ( domain.equals("") ) ? _defaultServerName : domain ;
            PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)_servers.get( domain ) ;
            if( vmp == null )
                throw new
                NoSuchElementException("No server found for : "+domain ) ;

            return vmp ;
        }
        public File getMountPointByPnfsId( PnfsId pnfsId ){
            String domain = pnfsId.getDomain() ;
            if( ( domain == null ) || ( domain.equals("") ) )
                return _defaultMountPoint ;

            PnfsFile.VirtualMountPoint vmp = getVmpByDomain( domain ) ;

            return vmp.getRealMountPoint() ;
        }
        public PnfsFile getFileByPnfsId( PnfsId pnfsId ) throws FileNotFoundCacheException{
            return PnfsFile.getFileByPnfsId(
            getMountPointByPnfsId(pnfsId) ,
            pnfsId ) ;
        }
        public String getDefaultServerName(){ return _defaultServerName ; }
        public void setDefaultServerName( String serverName )throws NoSuchElementException{

            PnfsFile.VirtualMountPoint vmp = getVmpByDomain( serverName ) ;

            _defaultMountPoint = vmp.getRealMountPoint() ;
            _defaultServerName = serverName ;
        }
        private String globalToLocal( String globalPath ){
            PathMap.Entry entry = _globalPathMap.match( globalPath ) ;
            if( entry.getNode() instanceof Map )return null ;
            PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)entry.getNode() ;
            return vmp.getVirtualLocalPath()+entry.getRest() ;
        }
        public int getServerCount(){ return _servers.size() ; }

    }
    public String hh_pathfinder = "<pnfsId> [-global|-local]" ;
    public String ac_pathfinder_$_1( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        String pnfsPath = pathfinder( pnfsId ) ;
        String domain = pnfsId.getDomain() ;
        PnfsFile.VirtualMountPoint vmp = _pathManager.getVmpByDomain(domain);
        if( vmp == null )
            throw new
            IllegalArgumentException("Can't find default VMP");

        if( args.getOpt("global") != null ){

            String pvm = vmp.getVirtualPnfsPath() ;
            if( ! pnfsPath.startsWith(pvm) )
                throw new
                IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);
            return vmp.getVirtualGlobalPath()+pnfsPath.substring(pvm.length());

        }else if( args.getOpt("local") != null ){

            String pvm = vmp.getVirtualPnfsPath() ;
            if( ! pnfsPath.startsWith(pvm) )
                throw new
                IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);
            return vmp.getVirtualLocalPath()+pnfsPath.substring(pvm.length());

        }else{
            return pnfsPath ;
        }
    }
    public String hh_get_stat = " # get method call counter" ;
    public String ac_get_stat( Args args ){
        StringBuffer sb = new StringBuffer() ;
        for( int i = 0 , n = _requestSet.length ; i < n ; i++ ){
            sb.append(_requestSet[i].toString()).append("\n");
        }
        return sb.toString() ;
    }
    public String hh_g2l = "<globalPath>" ;
    public String ac_g2l_$_1( Args args )throws Exception {
        return  _pathManager.globalToLocal( args.argv(0) ) ;
    }

    public String hh_rename = " # rename <old name> <new name>" ;
    public String ac_rename_$_2( Args args ){

        PnfsId    pnfsId   = null ;
        PnfsFile  pnfsFile = null ;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;
            pnfsFile = _pathManager.getFileByPnfsId( pnfsId );

        }catch(Exception ee ){
            String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
            pnfsFile  = new PnfsFile( localPath ) ;
        }


        try {
            this.rename(pnfsId, args.argv(1) , false);
        }catch( Exception e) {
            return "rename: " + e.getMessage() ;
        }

        return "" ;
    }

    public String ac_rename_$_1( Args args ){

        PnfsId    pnfsId   = null ;
        PnfsFile  pnfsFile = null ;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;
            pnfsFile = _pathManager.getFileByPnfsId( pnfsId );

        }catch(Exception ee ){
            String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
            pnfsFile  = new PnfsFile( localPath ) ;
        }


        try {
            this.rename(pnfsId, null , true);
        }catch( Exception e) {
            return "rename: " + e.getMessage() ;
        }

        return "" ;
    }


    public void getInfo( PrintWriter pw ){
        Iterator mp = _virtualMountPoints.iterator() ;
        while( mp.hasNext() ){
            PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)mp.next() ;
            pw.println( " $Id: PnfsManager2.java,v 1.46 2007-07-26 13:42:45 tigran Exp $" ) ;
            pw.println( " Server         : "+vmp.getServerId()+"("+vmp.getServerName()+")" ) ;
            pw.println( " RealMountPoint : "+vmp.getRealMountId()+" "+vmp.getRealMountPoint() ) ;
            pw.println( "       VirtualMountId : "+vmp.getVirtualMountId() ) ;
            pw.println( "      VirtualPnfsPath : "+vmp.getVirtualPnfsPath() ) ;
            pw.println( "     VirtualLocalPath : "+vmp.getVirtualLocalPath() ) ;
            pw.println( "    VirtualGlobalPath : "+vmp.getVirtualGlobalPath() ) ;
        }
        pw.println(" Threads ("+_threads+") Queue" ) ;
        for( int i = 0 ; i < _threads ; i++ ){
            pw.println( "    ["+i+"] "+_fifos[i].size() ) ;
        }
        return ;
    }

    public void say( String str ){
        pin( str ) ;
        super.say( str ) ;
    }
    public void esay( String str ){
        pin( str ) ;
        super.esay( str ) ;
    }
    public void esay( Exception e ){
        pin( e.toString() ) ;
        super.esay( e ) ;
    }
    public String hh_flags_set    = "<pnfsId> <key=value> [...]" ;
    public String hh_flags_remove = "<pnfsId> <key> [...]" ;
    public String hh_flags_ls     = "<pnfsId>" ;
    public String hh_pnfsidof     = "<globalPath>" ;
    public String ac_pnfsidof_$_1( Args args )throws Exception {

        String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
        PnfsFile pnfsFile  = new PnfsFile( localPath ) ;
        PnfsId   pnfsId    = pnfsFile.getPnfsId() ;

        return pnfsId.toString() ;
    }
    public String hh_cacheinfoof = "<pnfsid>|<globalPath>" ;
    public String ac_cacheinfoof_$_1( Args args )throws Exception {
        PnfsId    pnfsId   = null ;
        PnfsFile  pnfsFile = null ;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;
            pnfsFile = _pathManager.getFileByPnfsId( pnfsId );

        }catch(Exception ee ){
            String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
            pnfsFile  = new PnfsFile( localPath ) ;
        }

        CacheInfo info   = new CacheInfo( pnfsFile ) ;
        return info.toString()+"\n" ;
    }
    public String hh_set_meta = "<pnfsid>|<globalPath> <uid> <gid> <perm> <level1> ..." ;
    public String ac_set_meta_$_5_20( Args args )throws Exception {
        PnfsId    pnfsId   = null ;
        PnfsFile  pnfsFile = null ;
        try{

            pnfsId   = new PnfsId( args.argv(0) ) ;
            pnfsFile = _pathManager.getFileByPnfsId( pnfsId );

        }catch(Exception ee ){
            String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
            pnfsFile  = new PnfsFile( localPath ) ;
        }
        File mp = _pathManager.getMountPointByPnfsId(pnfsFile.getPnfsId());
        int uid = Integer.parseInt(args.argv(1));
        int gid = Integer.parseInt(args.argv(2));
        int mode = Integer.parseInt(args.argv(3),8);
        for( int i = 4 ; i < args.argc() ; i ++  ){
            int level = Integer.parseInt(args.argv(i)) ;
            setFileMetaData( mp , pnfsFile.getPnfsId() , level , uid , gid , mode,false );
        }
        return "" ;
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
        boolean e = args.getOpt("se") != null ;

        StringBuffer sb = new StringBuffer() ;

        try{
            try{
                pnfsId = new PnfsId( args.argv(0) ) ;
                if(v)sb.append("PnfsId : "+pnfsId).append("\n") ;
            }catch(Exception ee ){
                String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
                if( localPath == null )
                    throw new
                    IllegalArgumentException("Global Path can't be mapped : "+args.argv(0) ) ;

                if(v)sb.append("   Local Path : "+localPath ).append("\n") ;
                String resolvedPath = n ? localPath : new File(localPath).getCanonicalPath() ;
                if(v)sb.append("Resolved Path : "+resolvedPath).append("\n");
                PnfsFile pnfsFile   = new PnfsFile( resolvedPath ) ;
                pnfsId    = pnfsFile.getPnfsId() ;
                if(v)sb.append("       PnfsId : "+pnfsId).append("\n") ;
            }
            File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            StorageInfo info = _extractor.getStorageInfo(
            mountpoint.getAbsolutePath() ,
            pnfsId ) ;
            if(v)sb.append(" Storage Info : "+info ).append("\n") ;
            else sb.append(info.toString()).append("\n");
        }catch(Exception ee ){
            sb.append("Got exception : "+ee ) ;
            if( ! e )throw ee ;
        }
        long simsize = getSimulatedFilesize( pnfsId ) ;
        if( simsize > -1L )sb.append("Simulated Filesize : ").append(simsize).append("\n") ;
        return sb.toString() ;
    }
    public String fh_metadataof =
    "   storageinfoof <pnfsid>|<globalPath> [-v] [-n] [-se]\n"+
    "        -v    verbose\n"+
    "        -n    don't resolve links\n"+
    "        -se   suppress exceptions\n" ;
    public String hh_metadataof = "<pnfsid>|<globalPath> [-v] [-n] [-se]" ; ;
    public String ac_metadataof_$_1( Args args )throws Exception {
        PnfsId    pnfsId = null ;
        boolean v = args.getOpt("v") != null ;
        boolean n = args.getOpt("n") != null ;
        boolean e = args.getOpt("se") != null ;

        StringBuffer sb = new StringBuffer() ;

        try{
            try{
                pnfsId = new PnfsId( args.argv(0) ) ;
                if(v)sb.append("PnfsId : "+pnfsId).append("\n") ;
            }catch(Exception ee ){
                String   localPath = _pathManager.globalToLocal( args.argv(0) ) ;
                if( localPath == null )
                    throw new
                    IllegalArgumentException("Global Path can't be mapped : "+args.argv(0) ) ;

                if(v)sb.append("   Local Path : "+localPath ).append("\n") ;
                String resolvedPath = n ? localPath : new File(localPath).getCanonicalPath() ;
                if(v)sb.append("Resolved Path : "+resolvedPath).append("\n");
                PnfsFile pnfsFile   = new PnfsFile( resolvedPath ) ;
                pnfsId    = pnfsFile.getPnfsId() ;
                if(v)sb.append("       PnfsId : "+pnfsId).append("\n") ;
            }
            File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            FileMetaData info = getFileMetaData(  mountpoint , pnfsId ) ;
            if(v)sb.append("    Meta Data : "+info ).append("\n") ;
            else sb.append(info.toString()).append("\n");
        }catch(Exception ee ){
            sb.append("Got exception : "+ee ) ;
            if( ! e )throw ee ;
        }
        return sb.toString() ;
    }
    public String ac_flags_set_$_2_99( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;
        PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
        CacheInfo info   = new CacheInfo( pf ) ;
        CacheInfo.CacheFlags flags = info.getFlags() ;

        for( int i = 1 ; i < args.argc() ; i++ ){
            String t = args.argv(i) ;
            int l = t.length() ;
            if( l == 0 )continue ;
            int p = t.indexOf('=');
            if( ( p < 0 ) || ( p == (l-1) ) ){
                flags.put( t , "" ) ;
            }else if( p > 0 ){
                flags.put( t.substring(0,p) , t.substring(p+1) ) ;
            }
        }
        info.writeCacheInfo( pf ) ;
        return "" ;
    }

    public String ac_flags_remove_$_2_99( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;
        PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
        CacheInfo info   = new CacheInfo( pf ) ;
        CacheInfo.CacheFlags flags = info.getFlags() ;

        for( int i = 1 ; i < args.argc() ; i++ ){
            String t = args.argv(i) ;
            flags.remove( t ) ;
        }
        info.writeCacheInfo( pf ) ;
        return "" ;
    }
    public String ac_flags_ls_$_1( Args args )throws Exception {
        PnfsId    pnfsId = new PnfsId( args.argv(0) ) ;
        PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
        CacheInfo info   = new CacheInfo( pf ) ;
        CacheInfo.CacheFlags flags = info.getFlags() ;

        Iterator i = flags.entrySet().iterator() ;
        StringBuffer sb = new StringBuffer() ;
        int l = 0 ;
        while( i.hasNext() ){
            Map.Entry entry = (Map.Entry)i.next() ;
            sb.append(entry.getKey()).append(" -> ").
            append(entry.getValue()).append("\n");
        }
        return sb.toString() ;
    }
    private void updateFlag(PnfsFlagMessage pnfsMessage){

        PnfsId pnfsId    = pnfsMessage.getPnfsId();
        String operation = pnfsMessage.getOperation() ;
        String flagName  = pnfsMessage.getFlagName() ;
        String value     = pnfsMessage.getValue() ;
        say("update flag "+operation+" flag="+flagName+" value="+
            value+" for "+pnfsId);
        try{

            if( operation.equals( "get" ) ){
                pnfsMessage.setValue( updateFlag( pnfsId , operation , flagName , value ) );
            }else{
                updateFlag( pnfsId , operation , flagName , value );
            }

        }catch( Exception e ){
            esay("Exception in updateFlag "+e);
            esay(e) ;
            pnfsMessage.setFailed( 77 , e.getMessage() ) ;
        }
    }
    private String updateFlag( PnfsId pnfsId , String operation , String flagName , String value )
    throws Exception {

        PnfsFile   pnfsFile = _pathManager.getFileByPnfsId( pnfsId );
        if( pnfsFile == null )
            throw new
            IllegalArgumentException("PnfsId not found : "+pnfsId ) ;

        CacheInfo  info     = new CacheInfo( pnfsFile ) ;
        CacheInfo.CacheFlags flags    = info.getFlags() ;

        if( operation.equals( "put" ) ){
            say( "flags set "+pnfsId+" "+flagName+"="+value ) ;
            flags.put( flagName , value ) ;
            info.writeCacheInfo(pnfsFile);
        }else if( operation.equals( "put-dont-overwrite" ) ){
            say( "flags set (dontoverwrite) "+pnfsId+" "+flagName+"="+value ) ;
            String x = flags.get( flagName ) ;
            if( ( x == null ) || ( ! x.equals(value) ) ){
                say( "flags set "+pnfsId+" "+flagName+"="+value ) ;
                flags.put( flagName , value ) ;
                info.writeCacheInfo(pnfsFile);
            }
        }else if( operation.equals( "get" ) ){
            String v = flags.get( flagName ) ;
            say( "flags ls "+pnfsId+" "+flagName+" -> "+v ) ;
            return v ;
        }else if( operation.equals( "remove" ) ){
            say( "flags remove "+pnfsId+" "+flagName ) ;
            flags.remove( flagName ) ;
            info.writeCacheInfo(pnfsFile);
        }
        return null ;

    }
    private CacheInfo getCacheInfo( PnfsId pnfsId )throws Exception {
        say("get cache info for "+pnfsId ) ;
        PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
        return new CacheInfo( pf ) ;
    }
    private void putCacheInfo( PnfsId pnfsId , CacheInfo cacheInfo )throws Exception {
        say("store cache info for "+pnfsId ) ;
        PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
        cacheInfo.writeCacheInfo( pf ) ;
    }
    private void addCacheLocation(PnfsAddCacheLocationMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("add cache location "+pnfsMessage.getPoolName()+" for "+pnfsId);
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            ci.addCacheLocation(pnfsMessage.getPoolName());
            ci.writeCacheInfo(pf);
        } catch (Exception e){
            esay("Exception in addCacheLocation "+e);
            esay(e) ;
            pnfsMessage.setFailed(4,"Exception in addCacheLocation");
            _xaddCacheLocation.failed() ;
            //no reply to this message
        }
    }

    private void clearCacheLocation(PnfsClearCacheLocationMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        String cacheLocation = pnfsMessage.getPoolName();
        say("clearCacheLocation : "+ cacheLocation +" for "+pnfsId);
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            if( pf == null ){
                esay( "Can't get PnfsFile of : "+pnfsId ) ;
                return ;
            }
            CacheInfo ci = new CacheInfo(pf);

            if( cacheLocation.equals("*") ) {

        		List<String> cacheLocations = ci.getCacheLocations();

        		if(!cacheLocations.isEmpty()) {
	        		for( String location: cacheLocations) {
	                    ci.clearCacheLocation( location );
	                }
                    ci.writeCacheInfo(pf);
                }

            }else{
                if( ci.clearCacheLocation( cacheLocation ) )
                       ci.writeCacheInfo(pf);
            }

            if( ci.getCacheLocations().isEmpty() ){
                //
                // no copy in cache any more ...
                //
                CacheInfo.CacheFlags flags = ci.getFlags() ;
                String deletable = (String)flags.get("d") ;
                if( ( pnfsMessage.removeIfLast()   ) ||
                ( ( deletable != null ) && deletable.startsWith("t") ) ){

                    say("clearCacheLocation : deleting "+pnfsId+" from filesystem");
                    removeByPnfsId( pnfsId ) ;

                }
            }
        } catch (Exception e){
            _xclearCacheLocation.failed() ;
            esay("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            esay(e) ;
            //no reply to this message
        }
    }

    private void getCacheLocations(PnfsGetCacheLocationsMessage pnfsMessage){
        try {
            PnfsId pnfsId = pnfsMessage.getPnfsId();
            PnfsFile pf = null;
            if( pnfsId == null ) {
                say("get cache locations for "+ pnfsMessage.getPnfsPath());
                pf = new PnfsFile( _pathManager.globalToLocal( pnfsMessage.getPnfsPath() ) );
            }else{
                say("get cache locations for "+pnfsId);
                pf = _pathManager.getFileByPnfsId( pnfsId );
            }
            say("pnfs file = "+pf);
            CacheInfo ci = new CacheInfo(pf);
            say("cache info = "+ci);

            pnfsMessage.setCacheLocations(ci.getCacheLocations());
            pnfsMessage.setSucceeded();
        } catch (Exception exc){
            _xgetCacheLocations.failed() ;
            esay("Exception in getCacheLocations "+exc);
            esay(exc) ;
            pnfsMessage.setFailed(4,"Pnfs lookup failed");
        }
    }

    private void createDirectory(PnfsCreateDirectoryMessage pnfsMessage){
        String globalPath = pnfsMessage.getPath() ;
        say("create PNFS directory for "+globalPath);

        PnfsFile pf        = null ;
        String   localPath = null ;
        try{
            localPath = _pathManager.globalToLocal(globalPath) ;
            pf = new PnfsFile( localPath ) ;
        }catch(Exception ie ){
            pnfsMessage.setFailed(5,"g2l match failed : "+ie.getMessage());
            return ;
        }
        try{
            if( ! pf.mkdir())
                throw new
                Exception("File exists") ;
        }catch(Exception e){
            _xcreateEntry.failed() ;
            pnfsMessage.setFailed(2, e.getMessage());
            return;
        }

        if( ! pf.isPnfs() ){
            _xcreateEntry.failed() ;
            pnfsMessage.setFailed(3,"not pnfs file system");
            pf.delete();
            return;
        }
        PnfsId pnfsId = pf.getPnfsId() ;
        try{
            setFileMetaData( _pathManager.getMountPointByPnfsId(pnfsId),
            pnfsId , 0 ,
            pnfsMessage.getUid() ,
            pnfsMessage.getGid() ,
            pnfsMessage.getMode(),true  ) ;
        }catch(Exception ee ){
            esay("Can't set meta data for "+pnfsId+" : "+ee ) ;
        }
        pnfsMessage.setPnfsId(pnfsId);
        pnfsMessage.setSucceeded();
        //
        // now we try to get the storageInfo out of the
        // parent directory. If it failes, we don't care.
        // We declare the request to be successful because
        // the createEntry seem to be ok.
        //
        try{
            say( "Trying to get storageInfo for "+pnfsId) ;
            File  mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            StorageInfo info = _extractor.getStorageInfo(
            mountpoint.getAbsolutePath(),
            pnfsId
            )  ;

            if( info instanceof EnstoreStorageInfo )
                ((EnstoreStorageInfo)info).setPath( localPath ) ;

            pnfsMessage.setStorageInfo( info ) ;
            pnfsMessage.setMetaData( getFileMetaData( mountpoint , pnfsId ) ) ;

        }catch(Exception eeee){
            esay( "Can't determine storageInfo : "+eeee ) ;
        }
        return;
    }
    private void createEntry(PnfsCreateEntryMessage pnfsMessage){
        String globalPath = pnfsMessage.getPath() ;
        say("create PNFS entry for "+globalPath);

        PnfsFile pf        = null ;
        String   localPath = null ;
        try{
            localPath = _pathManager.globalToLocal(globalPath) ;
            pf = new PnfsFile( localPath ) ;
        }catch(Exception ie ){
            pnfsMessage.setFailed(5,"g2l match failed : "+ie.getMessage());
            return ;
        }
        try{
            if( ! pf.createNewFile() )
                throw new
                Exception("File exists") ;
        }catch(Exception e){
            _xcreateEntry.failed() ;
            pnfsMessage.setFailed(2, e.getMessage());
            return;
        }

        if( ! pf.isPnfs() ){
            _xcreateEntry.failed() ;
            pnfsMessage.setFailed(3,"not pnfs file system");
            pf.delete();
            return;
        }
        PnfsId pnfsId = pf.getPnfsId() ;
        try{
            for( int level = 0 ; level < 2 ; level ++ ){
                setFileMetaData( _pathManager.getMountPointByPnfsId(pnfsId),
                pnfsId , level ,
                pnfsMessage.getUid() ,
                pnfsMessage.getGid() ,
                pnfsMessage.getMode() ,false ) ;
            }
        }catch(Exception ee ){
            esay("Can't set meta data for "+pnfsId+" : "+ee ) ;
        }
        pnfsMessage.setPnfsId(pnfsId);
        pnfsMessage.setSucceeded();
        //
        // now we try to get the storageInfo out of the
        // parent directory. If it failes, we don't care.
        // We declare the request to be successful because
        // the createEntry seem to be ok.
        //
        try{
            say( "Trying to get storageInfo for "+pnfsId) ;
            File  mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            StorageInfo info = _extractor.getStorageInfo(
            mountpoint.getAbsolutePath(),
            pnfsId
            )  ;

            if( info instanceof EnstoreStorageInfo )
                ((EnstoreStorageInfo)info).setPath( localPath ) ;
            info.setKey("path", pnfsMessage.getPath() );
            pnfsMessage.setStorageInfo( info ) ;
            pnfsMessage.setMetaData( getFileMetaData( mountpoint , pnfsId ) ) ;

        }catch(Exception eeee){
            esay( "Can't determine storageInfo : "+eeee ) ;
        }
        return;
    }
    private void setStorageInfo( PnfsSetStorageInfoMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String  pnfsPath    = pnfsMessage.getPnfsPath() ;
                say("setStorageInfo (by path) global : "+pnfsPath  ) ;
                String  globalPath  = _pathManager.globalToLocal(pnfsPath);
                say("setStorageInfo (by path) local : "+globalPath  ) ;
                String resolvedPath = new File(globalPath).getCanonicalPath() ;
                say("setStorageInfo (by path) resolved : "+resolvedPath  ) ;
                PnfsFile   pnfsFile =  new PnfsFile( resolvedPath ) ;
                PnfsId     id       = pnfsFile.getPnfsId() ;
                if( id == null )
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                pnfsId = id ;
            }
            say( "setStorageInfo : "+pnfsId ) ;
            File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            _extractor.setStorageInfo(
            mountpoint.getAbsolutePath() ,
            pnfsId ,
            pnfsMessage.getStorageInfo() ,
            pnfsMessage.getAccessMode()    ) ;

        }catch(CacheException ee ){
            _xsetStorageInfo.failed() ;
            esay( "Failed : "+ee ) ;
            esay(ee);
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            _xsetStorageInfo.failed() ;
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( 7 , iee.getMessage() ) ;
        }
        return ;

    }
    private long getSimulatedFilesize( PnfsId pnfsId ){
        try{
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo cinfo  = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = cinfo.getFlags() ;

            Iterator i = flags.entrySet().iterator() ;
            while( i.hasNext() ){
                Map.Entry entry = (Map.Entry)i.next() ;
                if( entry.getKey().equals("l") ){
                    try{
                        return Long.parseLong(entry.getValue().toString());
                    }catch(Exception ee){
                        return -1 ;
                    }
                }
            }
            return -1 ;
        }catch(Exception eee ){
            esay( "Error obtaining 'l' flag for getSimulatedFilesize : "+eee ) ;
            return -1 ;
        }

    }
    private void getStorageInfo( PnfsGetStorageInfoMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String  pnfsPath    = pnfsMessage.getPnfsPath() ;
                say("getStorageInfo (by path) global : "+pnfsPath  ) ;
                String  globalPath  = _pathManager.globalToLocal(pnfsPath);
                say("getStorageInfo (by path) local : "+globalPath  ) ;
                String resolvedPath = new File(globalPath).getCanonicalPath() ;
                say("getStorageInfo (by path) resolved : "+resolvedPath  ) ;
                PnfsFile   pnfsFile =  new PnfsFile( resolvedPath ) ;
                PnfsId     id       = pnfsFile.getPnfsId() ;
                if( id == null )
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                pnfsId = id ;
            }
            say( "getStorageInfo : "+pnfsId ) ;
            File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            StorageInfo info = _extractor.getStorageInfo(
            mountpoint.getAbsolutePath() ,
            pnfsId ) ;
            pnfsMessage.setStorageInfo(info ) ;
            pnfsMessage.setPnfsId( pnfsId ) ;
            pnfsMessage.setSucceeded() ;
            say( "Storage info "+info ) ;
            pnfsMessage.setMetaData( getFileMetaData( mountpoint , pnfsId ) ) ;
            try{
                PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
                CacheInfo cinfo  = new CacheInfo( pf ) ;
                CacheInfo.CacheFlags flags = cinfo.getFlags() ;

                Iterator i = flags.entrySet().iterator() ;
                while( i.hasNext() ){
                    Map.Entry entry = (Map.Entry)i.next() ;
                    info.setKey( "flag-"+entry.getKey() , entry.getValue().toString() ) ;
                }
            }catch(Exception eee ){
                esay( "Error adding bits (stickybit) to storageinfo : "+eee ) ;
            }
            //
            // simulate large files
            //
            long largeFilesize       = -1 ;
            try{
                largeFilesize = Long.parseLong(info.getKey("flag-l"));
            }catch(Exception ee){}
            if( ( info.getFileSize() == 1L ) && ( largeFilesize > 0L ) )
                info.setFileSize(largeFilesize) ;

        }catch(CacheException ee ){
            _xgetStorageInfo.failed() ;
            esay( "Failed : "+ee ) ;
            esay(ee);
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            _xgetStorageInfo.failed() ;
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( 7 , iee.getMessage() ) ;
        }
        return ;

    }
    private void getFileMetaData( PnfsGetFileMetaDataMessage pnfsMessage ){
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        boolean resolve = pnfsMessage.resolve() ;
        try{
            if( pnfsId == null ){
                //
                // if the pnfsid is not defined they want 'StorageInfo by path'
                // let's get the pnfsId
                //
                String   pnfsPath   = pnfsMessage.getPnfsPath() ;
                String   globalPath = _pathManager.globalToLocal(pnfsPath) ;
                String   resolved   = resolve ?
                new File(globalPath).getCanonicalPath() :
                    globalPath ;
                PnfsFile pnfsFile   = new PnfsFile( resolved ) ;
                say("getFileMetaData (by path) : "+pnfsPath+" ("+pnfsFile+")" ) ;
                PnfsId   id         = pnfsFile.getPnfsId() ;
                if( id == null )
                    throw new
                    CacheException( "can't get pnfsId (not a pnfsfile)" ) ;
                pnfsId = id ;
            }
            say( "getFileMetaData : "+pnfsId ) ;
            File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
            pnfsMessage.setPnfsId( pnfsId ) ;
            pnfsMessage.setSucceeded() ;
            pnfsMessage.setMetaData( getFileMetaData( mountpoint , pnfsId ) ) ;

        }catch(CacheException ee ){
            _xgetStorageInfo.failed() ;
            esay( "Failed : "+ee ) ;
            esay(ee);
            pnfsMessage.setFailed( ee.getRc() , ee.getMessage() ) ;
        }catch(Exception iee ){
            _xgetStorageInfo.failed() ;
            esay( "Failed : "+iee ) ;
            esay(iee);
            pnfsMessage.setFailed( 7 , iee.getMessage() ) ;
        }
        return ;

    }

    private void setFileMetaData( PnfsSetFileMetaDataMessage pnfsMessage ) {
        PnfsId pnfsId   = pnfsMessage.getPnfsId() ;
        say( "setFileMetaData : "+pnfsId ) ;
        boolean resolve = pnfsMessage.resolve() ;

        int mode = 0;

        File mp = _pathManager.getMountPointByPnfsId(pnfsId) ;
        FileMetaData meta = pnfsMessage.getMetaData() ;

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

        for( int level = 0 ; level < 2 ; level ++ ){
            try {
                this.setFileMetaData( mp, pnfsId, level, meta.getUid(), meta.getGid(), mode, meta.isDirectory() );
            }catch( Exception e) {
                esay(e);
            }
        }
        return ;

    }

    private void setFileMetaData( File mp , PnfsId pnfsId ,
    int level ,
    int uid , int gid , int mode,boolean isDir)
    throws Exception {

        String hexTime = Long.toHexString( System.currentTimeMillis() / 1000L ) ;
        int l = hexTime.length() ;
        if( l > 8 )hexTime = hexTime.substring(l-8) ;
        StringBuffer sb = new StringBuffer(128);
        sb.append(".(pset)(").append(pnfsId.getId()).append(")(attr)(").
        append(level).append(")(").
        append(Integer.toOctalString(0100000|mode)).append(":").
        append(uid).append(":").
        append(gid).append(":").
        append(hexTime).append(":").
        append(hexTime).append(":").
        append(hexTime).append(")") ;

        File metaFile = new File( mp , sb.toString() ) ;
        try{
            say("touch "+metaFile);
            metaFile.createNewFile() ;
        }catch(Exception ee ){
            esay("Ignored Problem with "+metaFile+" : "+ee ) ;
        }
    }
    //
    // taken from linux stat man page
    //
    private static final int ST_FILE_FMT  = 0170000 ;
    private static final int ST_REGULAR   = 0100000 ;
    private static final int ST_DIRECTORY = 0040000 ;
    private static final int ST_SYMLINK   = 0120000 ;

    private FileMetaData getFileMetaData( File mp , PnfsId pnfsId )throws Exception{
        File metafile = new File( mp , ".(getattr)("+pnfsId.getId()+")" ) ;
        File orgfile  = new File( mp , ".(access)("+pnfsId.getId()+")" ) ;

        long filesize = orgfile.length() ;

        BufferedReader br = new BufferedReader(
        new FileReader( metafile ) ) ;
        try{
            String line = br.readLine() ;
            if( line == null )
                throw new
                IOException("Can't read meta : "+pnfsId )  ;
            StringTokenizer st = new StringTokenizer( line , ":" ) ;
            try{
                int perm = Integer.parseInt( st.nextToken() , 8 ) ;
                int uid  = Integer.parseInt( st.nextToken() ) ;
                int gid  = Integer.parseInt( st.nextToken() ) ;

                long aTime = Long.parseLong( st.nextToken() , 16 ) ;
                long mTime = Long.parseLong( st.nextToken() , 16 ) ;
                long cTime = Long.parseLong( st.nextToken() , 16 ) ;

                FileMetaData meta = new FileMetaData( uid , gid , perm ) ;

                long simFilesize = getSimulatedFilesize( pnfsId ) ;

                meta.setSize( ( simFilesize < 0L ) || ( filesize > 1L ) ? filesize : simFilesize ) ;

                int filetype = perm & ST_FILE_FMT ;

                meta.setFileType( filetype == ST_REGULAR ,
                filetype == ST_DIRECTORY ,
                filetype == ST_SYMLINK    ) ;

                meta.setTimes( aTime * 1000, mTime * 1000, cTime *1000) ;

                say( "getFileMetaData of "+pnfsId+" -> "+meta ) ;
                return meta ;
            }catch(Exception eee ){
                throw new
                IOException("Illegal meta data format : "+pnfsId+" ("+line+")" ) ;
            }
        }finally{
            try{ br.close() ; }catch(Exception ee){}
        }
    }
    private void deleteEntry(PnfsDeleteEntryMessage pnfsMessage){
        String path = pnfsMessage.getPath();
        boolean rc = false;
        PnfsFile pf = null;
        if( path == null) {
            try {
                // FIXME: not as elegant as I would like to see it
                path = this.pathfinder(pnfsMessage.getPnfsId());

                String domain = pnfsMessage.getPnfsId().getDomain() ;
                PnfsFile.VirtualMountPoint vmp = _pathManager.getVmpByDomain(domain);
                if( vmp == null )
                    throw new
                    IllegalArgumentException("Can't find default VMP");



                String pvm = vmp.getVirtualPnfsPath() ;
                if( ! path.startsWith(pvm) )
                    throw new
                    IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);
                path = vmp.getVirtualGlobalPath()+path.substring(pvm.length());


            }catch(Exception e) {
                esay("delete failed (pathfinder) "+e);
                esay(e) ;
                _xdeleteEntry.failed() ;
                pnfsMessage.setFailed(7, e);
                return;
            }
        }

        path = _pathManager.globalToLocal( path ) ;


        say("delete PNFS entry for "+path);

        pf = new PnfsFile(path);



        if (! pf.exists()){
            _xdeleteEntry.failed() ;
            say(path+": no such file");
            pnfsMessage.setFailed(4,"No such file");
            return;
        }

        try {
            rc = pf.delete();
        } catch (Exception e) {
            esay("delete failed "+e);
            esay(e) ;
            _xdeleteEntry.failed() ;
            pnfsMessage.setFailed(5, e);
            return;
        }


        if( ! rc ) {
            if( pf.isDirectory() && ( pf.list().length != 0 ) ) {
                esay(path + ": is not empty");
                pnfsMessage.setFailed(7, "directory is not empty");
            }else{
                esay(path + ": unknown reason");
                pnfsMessage.setFailed(8, "unknown reason");
            }
            _xdeleteEntry.failed() ;
            return ;
        }

        pnfsMessage.setSucceeded();
        return;
    }

    private void setLength(PnfsSetLengthMessage pnfsMessage){

        PnfsId pnfsId = pnfsMessage.getPnfsId();
        long   length = pnfsMessage.getLength();

        say("Set length of "+pnfsId+" to "+length+" Simulate Large = "+_simulateLargeFiles);

        try{

            if( _simulateLargeFiles ){

                updateFlag( pnfsId , "put" , "l" , ""+length ) ;

                setFileSize( pnfsId , length > 0x7fffffffL ? 1L : length ) ;

            }else{
                setFileSize( pnfsId , length ) ;
            }
        }catch(Exception exc){
            esay("Exception in setLength"+exc);
            esay(exc);
            _xsetLength.failed() ;
            pnfsMessage.setFailed(4,"Pnfs lookup failed");
        }
    }
    private void setFileSize( PnfsId pnfsId , long length )throws Exception {
        PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
        pf.setLength(length);
        for( int i = 0 ; i < 10 ; i++ ){
            long size =  pf.length() ;
            if( size == length )break ;
            say( "setLength : not yet ... " ) ;
            Thread.currentThread().sleep(1000);
        }
    }
    private void removeByPnfsId( PnfsId pnfsId ) throws Exception {

        say("removeByPnfsId : "+pnfsId );
        String pnfsPath = pathfinder( pnfsId ) ;
        say("removeByPnfsId : "+pnfsId+" -> pnfsPath="+pnfsPath) ;

        String domain = pnfsId.getDomain() ;
        PnfsFile.VirtualMountPoint vmp = _pathManager.getVmpByDomain(domain);
        if( vmp == null )
            throw new
            IllegalArgumentException("Can't find default VMP for "+pnfsId);



        String pvm = vmp.getVirtualPnfsPath() ;
        if( ! pnfsPath.startsWith(pvm) )
            throw new
            IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);

        say("removeByPnfsId : "+pnfsId+" -> virtualPnfsPath="+pvm) ;

        String localPath = vmp.getVirtualLocalPath()+pnfsPath.substring(pvm.length());

        say("removeByPnfsId : "+pnfsId+" -> localPath="+localPath) ;

        if( ! new File(localPath).delete() )
            throw new
            IOException("Couldn't delete file "+pnfsPath);

        return ;
    }

    public String pathfinderGlobal( PnfsId pnfsId ) throws Exception {

        String pnfsPath = pathfinder( pnfsId ) ;
        String domain = pnfsId.getDomain() ;
        PnfsFile.VirtualMountPoint vmp = _pathManager.getVmpByDomain(domain);
        if( vmp == null )
            throw new
            IllegalArgumentException("Can't find default VMP");

            String pvm = vmp.getVirtualPnfsPath() ;
            if( ! pnfsPath.startsWith(pvm) )
                throw new
                IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);
            return vmp.getVirtualGlobalPath()+pnfsPath.substring(pvm.length());
    }


    public String pathfinderLocal( PnfsId pnfsId ) throws Exception {

        String pnfsPath = pathfinder( pnfsId ) ;
        String domain = pnfsId.getDomain() ;
        PnfsFile.VirtualMountPoint vmp = _pathManager.getVmpByDomain(domain);
        if( vmp == null )
            throw new
            IllegalArgumentException("Can't find default VMP");

            String pvm = vmp.getVirtualPnfsPath() ;
            if( ! pnfsPath.startsWith(pvm) )
                throw new
                IllegalArgumentException("PnfsId not in scope of vmp : "+pvm);
            return vmp.getVirtualLocalPath()+pnfsPath.substring(pvm.length());
    }


    private String pathfinder( PnfsId pnfsId ) throws Exception {
        ArrayList list = new ArrayList() ;
        String    pnfs = pnfsId.getId() ;
        File mp = _pathManager.getMountPointByPnfsId( pnfsId ) ;
        String name ;
        while( true ){
            try{
                if( ( name = nameOf( mp , pnfs ) ) == null )break ;
                list.add( name ) ;
                if( ( pnfs = parentOf( mp , pnfs ) ) == null )break ;
            }catch(Exception ee ){
                break ;
            }
        }
        if( list.size() == 0 )
            throw new
            IllegalArgumentException( "PnfsId not found" ) ;

        StringBuffer sb = new StringBuffer() ;
        for( int i = list.size() - 1 ; i >= 0 ; i-- ){
            sb.append("/").append(list.get(i)) ;
        }
        return sb.toString() ;
    }
    private String nameOf( File mp , String pnfsId ) throws Exception {
        File file = new File( mp , ".(nameof)("+pnfsId+")" ) ;
        BufferedReader br = new BufferedReader(new FileReader( file ) ) ;
        try{
            return br.readLine() ;
        }finally{
            try{ br.close() ; }catch(Exception ee){}
        }
    }
    private String parentOf( File mp , String pnfsId ) throws Exception {
        File file = new File( mp , ".(parent)("+pnfsId+")" ) ;
        BufferedReader br = new BufferedReader(new FileReader( file ) ) ;
        try{
            return br.readLine() ;
        }finally{
            try{ br.close() ; }catch(Exception ee){}
        }
    }
    private void mapPath( PnfsMapPathMessage pnfsMessage ){
        PnfsId pnfsId     = pnfsMessage.getPnfsId() ;
        String globalPath = pnfsMessage.getGlobalPath() ;
        say("map path for "+pnfsId+" globalPath : "+globalPath);
        if( ( pnfsId == null ) && ( globalPath == null ) ){
            pnfsMessage.setFailed( 5 , "Illegal Arguments : need path or pnfsid" ) ;
            return ;
        }

        if( globalPath == null ){
            try{
                pnfsMessage.setGlobalPath( pathfinder( pnfsId ) ) ;
            }catch(Exception eee ){
                esay("Exception in mapPath (pathfinder) "+eee);
                esay(eee);
                pnfsMessage.setFailed(6,eee);
            }
            return ;
        }
        try{
            String   localPath = _pathManager.globalToLocal( globalPath ) ;
            PnfsFile pnfsFile  = new PnfsFile( localPath ) ;
            pnfsMessage.setPnfsId( pnfsFile.getPnfsId() ) ;
        }catch(Exception exc){
            esay("Exception in mapPath"+exc);
            esay(exc);
            pnfsMessage.setFailed(4,exc);
        }

    }

    private void getCacheStatistics(PnfsGetCacheStatisticsMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("get cache statistics for "+pnfsId);
        try {
            PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            CacheStatistics cs = ci.getCacheStatistics();
            pnfsMessage.setCacheStatistics(cs);
            pnfsMessage.setSucceeded();
        } catch (Exception exc){
            esay("Exception in getCacheStatistics"+exc);
            esay(exc);
            _xgetCacheStatistics.failed() ;
            pnfsMessage.setFailed(4,"Pnfs lookup failed");
        }
    }

    private void updateCacheStatistics(PnfsUpdateCacheStatisticsMessage pnfsMessage){
        PnfsId pnfsId = pnfsMessage.getPnfsId();
        say("update cache statistics for "+pnfsId);
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            CacheStatistics cs = ci.getCacheStatistics();
            long accessTime = pnfsMessage.getAccessTime();
            cs.markAccessed(accessTime);
            ci.setCacheStatistics(cs); //XXX is this necessary?
            ci.writeCacheInfo(pf);
            //XXX tell all the other pools about this update
        } catch (Exception e){
            _xupdateCacheStatistics.failed() ;
            esay("Exception in updateCacheStatistics "+e);
            esay(e);
        }
    }

    private void rename(PnfsRenameMessage pnfsMessage) {

        PnfsId pnfsId = pnfsMessage.getPnfsId();
        String newName = pnfsMessage.newName();
        say(" rename "+pnfsId+" into "+newName);
        try {
            this.rename(pnfsId, newName, pnfsMessage.isUnique() );
        }catch( Exception exc){
            esay("Exception in rename "+exc);
            esay(exc);
            _xrename.failed() ;
            pnfsMessage.setFailed(4,"Pnfs rename failed");
        }

    }


    private void rename(PnfsId pnfsId, String newName, boolean makeUnique) throws Exception {

        File src = new File( this.pathfinderLocal(pnfsId)  );

        if( (newName == null) && makeUnique) {

            StringBuffer uniqueName = new StringBuffer(src.getAbsolutePath());
            uniqueName.append(";").append( uniqueName.hashCode() ).append("_").append( new Date( System.currentTimeMillis() ) );
            newName = uniqueName.toString();
        }

        say("renameing " + pnfsId +"("+ this.pathfinder(pnfsId) + ")" + " to " + newName );

        String localPath = _pathManager.globalToLocal(newName);

        if( !src.renameTo( new File(localPath) ) ) {
            throw new CacheException(2817, "Failed to rename " + pnfsId + " (" + src.getAbsolutePath() + ") " +" to " + newName + " (" + localPath + ") ");
        }
    }


    private class ProcessThread implements Runnable {
        private SyncFifo2 _fifo = null ;
        private ProcessThread( SyncFifo2 fifo ){ _fifo = fifo ; }
        public void run(){
            CellMessage message = null ;
            say("Thread <"+Thread.currentThread().getName()+"> started");
            while( ( message = (CellMessage)_fifo.pop() ) != null ){
                PnfsMessage pnfsMessage = (PnfsMessage)message.getMessageObject() ;
                try{
                    processPnfsMessage( message , pnfsMessage );
                }catch(Throwable processException ){
                    esay( "processPnfsMessage : "+
                    Thread.currentThread().getName()+" : "+
                    processException );
                }
            }
            say("Thread <"+Thread.currentThread().getName()+"> finished");
        }
    }

    public void messageArrived( CellMessage message ){

        Object pnfsMessage  = message.getMessageObject();
        if (! (pnfsMessage instanceof Message) ){
            say("Unexpected message class "+pnfsMessage.getClass());
            say("source = "+message.getSourceAddress());
            return;
        }
        PnfsMessage pnfs   = (PnfsMessage)pnfsMessage ;
        PnfsId      pnfsId = pnfs.getPnfsId() ;

        if( ( _cacheModificationRelay != null ) &&
        ( ( pnfs instanceof PnfsAddCacheLocationMessage   ) ||
        ( pnfs instanceof PnfsClearCacheLocationMessage )    ) ){


            forwardModifyCacheLocationMessage( pnfs ) ;

        }
        int hashIndex = 0 ;
        if( pnfsId == null ){
            hashIndex = _random.nextInt(_threads) ;
        }else{
            hashIndex = pnfsId.hashCode() ;
            hashIndex = hashIndex == Integer.MIN_VALUE ? 0 : ( Math.abs(hashIndex) % _threads ) ;
        }
        say( "Using thread ["+pnfsId+"] "+hashIndex);
        SyncFifo2 fifo = _fifos[hashIndex] ;

        fifo.push( message ) ;
    }
    private void forwardModifyCacheLocationMessage( PnfsMessage message ){
        try{

            sendMessage( new CellMessage( _cacheModificationRelay , message ) ) ;

        }catch(Exception ee ){
            esay("Problem "+ee.getMessage()+" relaying to "+_cacheModificationRelay+" : "+message ) ;
        }
    }
    public void processPnfsMessage( CellMessage message , PnfsMessage pnfsMessage ){

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
            _xgetStorageInfo.request() ;
            getFileMetaData((PnfsGetFileMetaDataMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsSetFileMetaDataMessage){
            _xsetStorageInfo.request() ;
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
            _xmapPath.request() ;
            mapPath((PnfsMapPathMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsFlagMessage){
            _xupdateFlag.request() ;
            updateFlag((PnfsFlagMessage)pnfsMessage);
        }
        else if (pnfsMessage instanceof PnfsRenameMessage){
            rename((PnfsRenameMessage)pnfsMessage);
        }
        else {
            say("Unexpected message class "+pnfsMessage.getClass());
            say("source = "+message.getSourceAddress());
            return;
        }
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
}
