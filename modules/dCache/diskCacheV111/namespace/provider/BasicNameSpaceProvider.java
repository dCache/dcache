/*
 * $Id: BasicNameSpaceProvider.java,v 1.12.2.8 2007-08-16 13:46:09 tigran Exp $
 */

package diskCacheV111.namespace.provider;

import dmg.util.Args;
import dmg.cells.nucleus.CellNucleus;
import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.namespace.*;


import java.io.*;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.Vector;
import java.util.Enumeration;
import java.util.Set;

public class BasicNameSpaceProvider implements NameSpaceProvider, StorageInfoProvider, CacheLocationProvider {


    private String            _mountPoint = null;
    private CellNucleus       _nucleus = null;
    private List              _virtualMountPoints     = null ;
    private PathManager       _pathManager = null;
    private Args        _args      = null ;
    private StorageInfoExtractable _extractor = null ;


    /** Creates a new instance of BasicNameSpaceProvider */
    public BasicNameSpaceProvider(Args args, CellNucleus nucleus) throws Exception {


        _nucleus = nucleus;

        _args = args;

        //
        // get the extractor
        //
        Class exClass = Class.forName( _args.argv(0)) ;
        _extractor = (StorageInfoExtractable)exClass.newInstance() ;

        _mountPoint = _args.getOpt("pnfs")  ;


        if( ( _mountPoint != null ) && ( ! _mountPoint.equals("") ) ){
            say( "PnfsFilesystem enforced : "+_mountPoint) ;
            PnfsFile pf = new PnfsFile(_mountPoint);
            if( ! (pf.isDirectory() && pf.isPnfs() ) )
                throw new
                IllegalArgumentException(
                "not a pnfs directory: "+_mountPoint);
            _virtualMountPoints = PnfsFile.getVirtualMountPoints(new File(_mountPoint));

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
    }

    public void addCacheLocation(PnfsId pnfsId, String cacheLocation) {

        say("add cache location "+ cacheLocation +" for "+pnfsId);
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            ci.addCacheLocation( cacheLocation);
            ci.writeCacheInfo(pf);
        } catch (Exception e){
            esay("Exception in addCacheLocation "+e);
            //no reply to this message
        }
    }

    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception {

        say("clearCacheLocation : "+cacheLocation+" for "+pnfsId);
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            if( pf == null ){
                esay( "Can't get PnfsFile of : "+pnfsId ) ;
                return ;
            }
            CacheInfo ci = new CacheInfo(pf);

            if( cacheLocation.equals("*") ) {

                Map cacheLocations = ci.getCacheLocations();
                Set locations = cacheLocations.keySet();

                if( !locations.isEmpty() ) {
                    Iterator li = locations.iterator() ;
                    while( li.hasNext() ) {
                        String location = (String)cacheLocations.get( li.next() );
                        ci.clearCacheLocation( location );
                    }

                    ci.writeCacheInfo(pf);
                }
            }else{
                if( ci.clearCacheLocation( cacheLocation ) )
                       ci.writeCacheInfo(pf);
            }

            if( ci.getCacheLocations().size() == 0 ){
                //
                // no copy in cache any more ...
                //
                CacheInfo.CacheFlags flags = ci.getFlags() ;
                String deletable = (String)flags.get("d") ;
                if( ( removeIfLast  ) ||
                ( ( deletable != null ) && deletable.startsWith("t") ) ){

                    say("clearCacheLocation : deleting "+pnfsId+" from filesystem");
                    this.deleteEntry( pnfsId ) ;

                }
            }
        } catch (Exception e){
            esay("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            //no reply to this message
        }
    }

    public PnfsId createEntry(String name, boolean isDirectory ) throws Exception {


        String globalPath = name;
        say("create PNFS entry for "+globalPath);

        PnfsFile pf        = null ;
        String   localPath = null ;
        try{
            localPath = _pathManager.globalToLocal(globalPath) ;
            pf = new PnfsFile( localPath ) ;
        }catch(Exception ie ){
            throw new IllegalArgumentException( "g2l match failed : "+ie.getMessage());
        }

        boolean rc;

        try{

            if( isDirectory ) {
                rc = pf.mkdir();
            }else{
                rc = pf.createNewFile();
            }

        }catch(Exception e){
            throw new IllegalArgumentException( "failed : " + e.getMessage());
        }

        if( ! rc ) {
            throw new
            FileExistsCacheException("File exists") ;
        }

        if( ! pf.isPnfs() ){
            pf.delete();
            throw new IllegalArgumentException( "Not a pnfs file system");
        }

        return pf.getPnfsId() ;
    }

    public void deleteEntry(PnfsId pnfsId) throws Exception {

        boolean rc = false;
        String  pnfsIdPath = this.pnfsidToPath(pnfsId) ;
        say("delete PNFS entry for " + pnfsId + " path " + pnfsIdPath);

        PnfsFile pf =  new PnfsFile( this.pnfsidToPath(pnfsId) );

        if (! pf.exists()){
            say(pnfsId+": no such file");
            throw new IllegalArgumentException( "No such file or directory");
        }

        try {
            rc = pf.delete();
        } catch (Exception e) {
            esay("delete failed "+e);
            throw new IllegalArgumentException( "Failed to remove entry " + pnfsId + " : " + e);
        }


        if( ! rc ) {
            if( pf.isDirectory() && ( pf.list().length != 0 ) ) {
                esay(pnfsId + ": is not empty");
                throw new IllegalArgumentException( "Directory  " + pnfsId + " not empty");
            }else{
                esay(pnfsId+ ": unknown reason");
                throw new IllegalArgumentException( "Failed to remove entry " + pnfsId + " : Unknown reason.");
            }
        }

        return;
    }

    public List getCacheLocation(PnfsId pnfsId) throws Exception{



        PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
        if(  pf == null || ! pf.exists() ) {
        	throw new FileNotFoundCacheException("no such file or directory" + pnfsId.toString() );
        }
        say("pnfs file = "+pf);
        CacheInfo ci = new CacheInfo(pf);
        say("cache info = "+ci);

        List locations = new Vector();
        Enumeration e = ci.getCacheLocations().keys();
        while (e.hasMoreElements()){
            locations.add((String)e.nextElement());
        }

        return locations;

    }


    public FileMetaData getFileMetaData(PnfsId pnfsId) throws Exception {

        FileMetaData fileMetaData;

        fileMetaData = getFileMetaData( _pathManager.getMountPointByPnfsId(pnfsId) , pnfsId ) ;

        /*
         *  we do not catch any exception here, while we can not react on it 
         *  ( FileNotFoundException )
         *  The caller will do it
         */
        
        return fileMetaData;
    }

    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) {

        int mode = 0;

        //FIXME: to be done more elegant

        // user
        if( metaData.getUserPermissions().canRead() ) {
            mode |= 0400;
        }

        if( metaData.getUserPermissions().canWrite() ) {
            mode |= 0200;
        }
        if( metaData.getUserPermissions().canExecute() ) {
            mode |= 0100;
        }


        // group
        if( metaData.getGroupPermissions().canRead() ) {
            mode |= 0040;
        }

        if( metaData.getGroupPermissions().canWrite() ) {
            mode |= 0020;
        }
        if( metaData.getGroupPermissions().canExecute() ) {
            mode |= 0010;
        }

        // world
        if( metaData.getWorldPermissions().canRead() ) {
            mode |= 0004;
        }

        if( metaData.getWorldPermissions().canWrite() ) {
            mode |= 0002;
        }
        if( metaData.getWorldPermissions().canExecute() ) {
            mode |= 0001;
        }

        for( int level = 0 ; level < 2 ; level ++ ){
            try {
                this.setFileMetaData( _pathManager.getMountPointByPnfsId(pnfsId), pnfsId, level, metaData.getUid(), metaData.getGid(), mode, metaData.isDirectory() );
            }catch( Exception e) {
                esay(e.getMessage());
            }
        }
        if( ! metaData.isDirectory() ){
            try {
               setFileSize(pnfsId, metaData.getFileSize());
            }catch(Exception e){}
        }

        return ;
    }

    public String pnfsidToPath(PnfsId pnfsId) throws CacheException {

     PnfsFile pnfsFile = _pathManager.getFileByPnfsId(pnfsId);
     if(pnfsFile == null) {
         throw new FileNotFoundCacheException(pnfsId.toString() + " not found");
     }

     try {
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

     }catch ( Exception e) {
        esay("!! Problem determining path of "+pnfsId);
        esay(e);
     }
        return pnfsFile.getPath();

    }

    public PnfsId pathToPnfsid(String path, boolean followLinks) throws Exception {

        PnfsFile pnfsFile = null;
        try {
	        String localPath = _pathManager.globalToLocal( path );

	        if( followLinks ) {
	            File localFile = new File( localPath );
	            pnfsFile = new PnfsFile(localFile.getAbsolutePath());
	        }else {
	            pnfsFile = new PnfsFile(localPath);
	        }
        }catch(NoSuchElementException nse) {
        	throw new FileNotFoundCacheException("path " +path+" not found");
        }

        if(pnfsFile == null || !pnfsFile.exists() ) {
            throw new FileNotFoundCacheException("path " +path+" not found");
        }
        return pnfsFile.getPnfsId();
    }


    public void setStorageInfo(PnfsId pnfsId, StorageInfo storageInfo, int mode) throws Exception {


        say( "setStorageInfo : "+pnfsId ) ;
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
        _extractor.setStorageInfo(
        mountpoint.getAbsolutePath() ,
        pnfsId ,
        storageInfo,
        mode ) ;

        return ;
    }

    public StorageInfo getStorageInfo(PnfsId pnfsId) throws Exception {

        say( "getStorageInfo : "+pnfsId ) ;
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
        StorageInfo info = _extractor.getStorageInfo(
        mountpoint.getAbsolutePath() ,
        pnfsId ) ;

        say( "Storage info "+info ) ;

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
        if( info.getFileSize() == 1L ) {

	        long largeFilesize       = -1L ;
	        try{
	        	String sizeString = info.getKey("flag-l");
	        	if(sizeString != null ) {
		            largeFilesize = Long.parseLong(sizeString);
			        if(  largeFilesize > 0L  ) {
			            info.setFileSize(largeFilesize) ;
			        }
	        	}
	        }catch(NumberFormatException nfe){}
        }
        return info ;

    }

    public String[] getFileAttributeList(PnfsId pnfsId) {

        String[] keys = null;

        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo info   = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = info.getFlags() ;
            Set s = flags.entrySet();

            keys = new String[s.size()];
            Iterator it = s.iterator();

            for( int i = 0; i < keys.length ; i++) {
                Map.Entry entry = (Map.Entry)it.next() ;
                keys[i] = (String)entry.getKey() ;
            }
        }catch(Exception e){
            esay(e.getMessage());
        }

        return keys;

    }

    public Object getFileAttribute(PnfsId pnfsId, String attribute) {

        Object attr = null;
        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo info   = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = info.getFlags() ;

            attr =  flags.get(attribute);

        }catch( Exception e){
            esay(e.getMessage());
        }

        return attr;
    }

    public void setFileAttribute(PnfsId pnfsId, String attribute, Object data) {

        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo info   = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = info.getFlags() ;

            flags.put( attribute ,  data.toString()) ;

            info.writeCacheInfo( pf ) ;
        }catch( Exception e){
            esay(e.getMessage());
        }

    }

    public void removeFileAttribute(PnfsId pnfsId, String attribute) {
        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo info   = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = info.getFlags() ;

            flags.remove( attribute ) ;
            info.writeCacheInfo( pf ) ;
        }catch( Exception e){
            esay(e.getMessage());
        }

    }


    public void renameEntry(PnfsId pnfsId, String newName) throws Exception {

        File src = new File( _pathManager.globalToLocal( this.pnfsidToPath(pnfsId) ) );
        String localPath = _pathManager.globalToLocal(newName);

        if( !src.renameTo( new File(localPath) ) ) {
            throw new CacheException(2817, "Failed to rename " + pnfsId + " (" + src.getAbsolutePath() + ") " +" to " + newName + " (" + localPath + ") ");
        }
    }


    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////
    private class PathManager {
        private PathMap _globalPathMap = new PathMap() ;
        private Map     _servers       = new HashMap() ;
        private File    _defaultMountPoint = null ;
        private String  _defaultServerName = null ;
        private String  _defaultServerId = null ;

        private PathManager( List virtualMountPoints ){
            Iterator mp = _virtualMountPoints.iterator() ;
            while( mp.hasNext() ){
                PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)mp.next() ;
                _globalPathMap.add( vmp.getVirtualGlobalPath() , vmp ) ;
                _servers.put( vmp.getServerId()  , vmp ) ;
            }
            if( _servers.size() == 0 ) {
                throw new
                NoSuchElementException("Emtpy server list" ) ;
            }

            PnfsFile.VirtualMountPoint vmp =
            (PnfsFile.VirtualMountPoint)virtualMountPoints.get(0);

            _defaultServerName = vmp.getServerName() ;
            _defaultServerId = vmp.getServerId() ;
            _defaultMountPoint = getVmpByDomain( _defaultServerId ).getRealMountPoint()  ;


        }
        private PnfsFile.VirtualMountPoint getVmpByDomain( String domain ){
            domain = ( (domain == null ) ||  domain.equals("") ) ? _defaultServerId : domain ;
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
        public PnfsFile getFileByPnfsId( PnfsId pnfsId ){
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



    ///// CELL hack


    void say( String s ){
        _nucleus.say(s);
    }

    void esay( String s ){
        _nucleus.say(s);
    }

    void esay( Exception e ){
        _nucleus.esay(e);
    }

    ////////////////////    Internal Part     /////////////////////////////



    //
    // taken from linux stat man page
    //
    private static final int ST_FILE_FMT  = 0170000 ;
    private static final int ST_REGULAR   = 0100000 ;
    private static final int ST_DIRECTORY = 0040000 ;
    private static final int ST_SYMLINK   = 0120000 ;




    private long getSimulatedFilesize( PnfsId pnfsId ){

    	long simulatedFileSize = -1;

        try{

            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo cinfo  = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = cinfo.getFlags() ;

            String simulatedFileSizeString = flags.get("l");

            if( simulatedFileSizeString != null ) {
	            try{
	            	simulatedFileSize =  Long.parseLong(simulatedFileSizeString);
	            }catch(NumberFormatException ignored){}
            }

        }catch(Exception eee ){
            esay( "Error obtaining 'l' flag for getSimulatedFilesize : "+eee ) ;
            simulatedFileSize =  -1 ;
        }

        return  simulatedFileSize;
    }



    private FileMetaData getFileMetaData( File mp , PnfsId pnfsId )throws Exception{
        File metafile = new File( mp , ".(getattr)("+pnfsId.getId()+")" ) ;
        File orgfile  = new File( mp , ".(access)("+pnfsId.getId()+")" ) ;

        long filesize = orgfile.length() ;

        BufferedReader br = null;
        try{

        	br = new BufferedReader(
        	        new FileReader( metafile ) ) ;

            String line = br.readLine() ;
            if( line == null ) {
                throw new
                IOException("Can't read meta : "+pnfsId )  ;
            }

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

                meta.setTimes( aTime , mTime , cTime ) ;

                say( "getFileMetaData of "+pnfsId+" -> "+meta ) ;
                return meta ;
            }catch(Exception eee ){
                throw new
                IOException("Illegal meta data format : "+pnfsId+" ("+line+")" ) ;
            }
        }catch(FileNotFoundException fnf ) {
        	throw new FileNotFoundCacheException("no such file or directory" + pnfsId.getId() );
        }finally{
            try{ if(br != null) br.close() ; }catch(Exception ee){}
        }
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


    private void setFileSize( PnfsId pnfsId , long length )throws Exception {
        PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
        pf.setLength(length);
        for( int i = 0 ; i < 10 ; i++ ){
            long size =  pf.length() ;
            if( size == length )break ;
            say( "setLength : not yet ... " ) ;
            Thread.sleep(1000);
        }
    }



    public String toString() {
        Iterator mp = _virtualMountPoints.iterator() ;
        StringBuffer sb = new StringBuffer();


        sb.append("$Id: BasicNameSpaceProvider.java,v 1.12.2.8 2007-08-16 13:46:09 tigran Exp $").append("\n");
        while( mp.hasNext() ){
            PnfsFile.VirtualMountPoint vmp = (PnfsFile.VirtualMountPoint)mp.next() ;

            sb.append( " Server         : "+vmp.getServerId()+"("+vmp.getServerName()+")" ).append("\n") ;
            sb.append( " RealMountPoint : "+vmp.getRealMountId()+" "+vmp.getRealMountPoint() ).append("\n") ;
            sb.append( "       VirtualMountId : "+vmp.getVirtualMountId() ).append("\n") ;
            sb.append( "      VirtualPnfsPath : "+vmp.getVirtualPnfsPath() ).append("\n") ;
            sb.append( "     VirtualLocalPath : "+vmp.getVirtualLocalPath() ).append("\n") ;
            sb.append( "    VirtualGlobalPath : "+vmp.getVirtualGlobalPath() ).append("\n") ;
        }


        return sb.toString();
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

}
