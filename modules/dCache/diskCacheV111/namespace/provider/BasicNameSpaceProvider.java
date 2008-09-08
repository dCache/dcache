/*
 * $Id: BasicNameSpaceProvider.java,v 1.24 2007-09-24 07:01:38 tigran Exp $
 */

package diskCacheV111.namespace.provider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;

import org.apache.log4j.Logger;

import diskCacheV111.namespace.CacheLocationProvider;
import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.namespace.StorageInfoProvider;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PathMap;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.StorageInfoExtractable;
import diskCacheV111.util.PnfsFile.VirtualMountPoint;
import diskCacheV111.vehicles.CacheInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;

public class BasicNameSpaceProvider implements NameSpaceProvider, StorageInfoProvider, CacheLocationProvider {


    private final String            _mountPoint ;
    private final CellNucleus       _nucleus;
    private final List<VirtualMountPoint>              _virtualMountPoints ;
    private final PathManager       _pathManager ;
    private final Args        _args ;
    private final StorageInfoExtractable _extractor;
    private final AttributeChecksumBridge _attChecksumImpl;

    private static final Logger _logNameSpace =  Logger.getLogger("logger.org.dcache.namespace." + BasicNameSpaceProvider.class.getName());

    /** Creates a new instance of BasicNameSpaceProvider */
    public BasicNameSpaceProvider(Args args, CellNucleus nucleus) throws Exception {


        _nucleus = nucleus;

        _args = args;

        _attChecksumImpl = new AttributeChecksumBridge(this);

        //
        // get the extractor
        //
        Class<?> exClass = Class.forName( _args.argv(0)) ;
        _extractor = (StorageInfoExtractable)exClass.newInstance() ;

        _mountPoint = _args.getOpt("pnfs")  ;


        if( ( _mountPoint != null ) && ( ! _mountPoint.equals("") ) ){
            _logNameSpace.debug( "PnfsFilesystem enforced : "+_mountPoint) ;
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
            _logNameSpace.debug( "Starting PNFS autodetect" ) ;
            _virtualMountPoints = PnfsFile.getVirtualMountPoints();
        }
        if( _virtualMountPoints.isEmpty() )
            throw new
            Exception("No mountpoints left ... ");

        for( PnfsFile.VirtualMountPoint vmp: _virtualMountPoints ){

            if(_logNameSpace.isDebugEnabled()) {

                _logNameSpace.debug( " Server         : "+vmp.getServerId()+"("+vmp.getServerName()+")" ) ;
                _logNameSpace.debug( " RealMountPoint : "+vmp.getRealMountId()+" "+vmp.getRealMountPoint() ) ;
                _logNameSpace.debug( "       VirtualMountId : "+vmp.getVirtualMountId() ) ;
                _logNameSpace.debug( "      VirtualPnfsPath : "+vmp.getVirtualPnfsPath() ) ;
                _logNameSpace.debug( "     VirtualLocalPath : "+vmp.getVirtualLocalPath() ) ;
                _logNameSpace.debug( "    VirtualGlobalPath : "+vmp.getVirtualGlobalPath() ) ;

            }
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

        _logNameSpace.debug("Using default pnfs server : "+_pathManager.getDefaultServerName());
    }

    public void addCacheLocation(PnfsId pnfsId, String cacheLocation) {

        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("add cache location " + cacheLocation + " for "
                    + pnfsId);
        }
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            ci.addCacheLocation( cacheLocation);
            ci.writeCacheInfo(pf);
        } catch (Exception e){
            _logNameSpace.error("Exception in addCacheLocation "+e);
            //no reply to this message
        }
    }

    public void clearCacheLocation(PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws Exception {

        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("clearCacheLocation : " + cacheLocation
                    + " for " + pnfsId);
        }
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            if( pf == null ){
                _logNameSpace.error( "Can't get PnfsFile of : "+pnfsId ) ;
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
                String deletable = flags.get("d") ;
                if( ( removeIfLast  ) ||
                ( ( deletable != null ) && deletable.startsWith("t") ) ){

                    if (_logNameSpace.isDebugEnabled()) {
                        _logNameSpace.debug("clearCacheLocation : deleting "
                                + pnfsId + " from filesystem");
                    }
                    this.deleteEntry( pnfsId ) ;

                }
            }
        } catch (Exception e){
            _logNameSpace.error("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            //no reply to this message
        }
    }

    public PnfsId createEntry(String name, FileMetaData metaData, boolean isDirectory ) throws Exception {


        String globalPath = name;
        if(_logNameSpace.isDebugEnabled() ) {
        	_logNameSpace.debug("create PNFS entry for "+globalPath);
        }

        PnfsFile pf        = null ;
        String   localPath = null ;
        PnfsId pnfsId = null;

        try{
            localPath = _pathManager.globalToLocal(globalPath) ;
            pf = new PnfsFile( localPath ) ;
        }catch(Exception ie ){
        	_logNameSpace.error("failed to map global path to local: " + globalPath, ie);
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
        	_logNameSpace.error("failed to create a new entry: " + globalPath, e);
            throw new IllegalArgumentException( "failed : " + e.getMessage());
        }

        if( ! rc ) {
            throw new
            FileExistsCacheException("File exists") ;
        }

        if( ! pf.isPnfs() ){
            _logNameSpace.warn("requested path ["+globalPath+"], is not an pnfs path");
            pf.delete();
            throw new IllegalArgumentException( "Not a pnfs file system");
        }


        pnfsId = pf.getPnfsId();
        try {
        	this.setFileMetaData(pnfsId, metaData);
        }catch(Exception e) {
        	_logNameSpace.error("failed to set permissions for: " + globalPath, e);
        	pf.delete();
        	throw new IllegalArgumentException( "failed to set permissions for: " + globalPath + " : " + e.getMessage());
        }

        if(_logNameSpace.isDebugEnabled() ) {
        	_logNameSpace.debug("Created new entry ["+globalPath+"], id: " + pnfsId.toString());
        }
        return pf.getPnfsId() ;
    }

    public void deleteEntry(PnfsId pnfsId) throws Exception {

        String  pnfsIdPath = this.pnfsidToPath(pnfsId) ;
        _logNameSpace.debug("delete PNFS entry for " + pnfsId );

        deleteEntry(pnfsIdPath);
    }

    public void deleteEntry(String path) throws Exception {

        boolean rc;
        
        _logNameSpace.debug("delete PNFS entry for  path " + path);

        PnfsFile pf =  new PnfsFile(path);

        if (! pf.exists()){
            _logNameSpace.debug(path+": no such file");
            throw new FileNotFoundCacheException( "No such file or directory");
        }

        try {
            rc = pf.delete();
        } catch (Exception e) {
            _logNameSpace.error("delete failed "+e);
            throw new IllegalArgumentException( "Failed to remove entry " + path + " : " + e);
        }


        if( ! rc ) {
            if( pf.isDirectory() && ( pf.list().length != 0 ) ) {
                _logNameSpace.error(path + ": is not empty");
                throw new IllegalArgumentException( "Directory  " + path + " not empty");
            } else{
                _logNameSpace.error(path+ ": unknown reason");
                throw new IllegalArgumentException( "Failed to remove entry " + path + " : Unknown reason.");
            }
        }
    }

    public List<String> getCacheLocation(PnfsId pnfsId) throws Exception{

        PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
        if( pf == null || ! pf.exists() ) {
        	throw new FileNotFoundCacheException("no such file or directory" + pnfsId.toString() );
        }

        CacheInfo ci = new CacheInfo(pf);
        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("pnfs file = " + pf + " cache info = " + ci);
        }
        return new ArrayList<String>(ci.getCacheLocations());

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

    public void setFileMetaData(PnfsId pnfsId, FileMetaData metaData) throws Exception {

        if (metaData.isUserPermissionsSet()) {

            File mountPoint = _pathManager.getMountPointByPnfsId(pnfsId);
            for (int level = 0; level < 2; level++) {
                this.setFileMetaData(mountPoint, pnfsId, level, metaData);
            }

        }

        if (!metaData.isDirectory() && metaData.isSizeSet()) {
            setFileSize(pnfsId, metaData.getFileSize());
        }

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
        _logNameSpace.error("!! Problem determining path of "+pnfsId);
        _logNameSpace.error(e);
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

        if( !pnfsFile.exists() ) {
            throw new FileNotFoundCacheException("path " +path+" not found");
        }
        return pnfsFile.getPnfsId();
    }


    public void setStorageInfo(PnfsId pnfsId, StorageInfo storageInfo, int mode) throws Exception {


        _logNameSpace.debug( "setStorageInfo : "+pnfsId ) ;
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
        _extractor.setStorageInfo(
        mountpoint.getAbsolutePath() ,
        pnfsId ,
        storageInfo,
        mode ) ;

        return ;
    }

    public StorageInfo getStorageInfo(PnfsId pnfsId) throws Exception {

        _logNameSpace.debug("getStorageInfo : " + pnfsId);
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId);
        StorageInfo info = _extractor.getStorageInfo(mountpoint.getAbsolutePath(), pnfsId);

        _logNameSpace.debug("Storage info " + info);

        PnfsFile pf = _pathManager.getFileByPnfsId(pnfsId);
        if (pf.isFile()) {

            try {

                CacheInfo cinfo = new CacheInfo(pf);
                CacheInfo.CacheFlags flags = cinfo.getFlags();

                for (Map.Entry<String, String> entry : flags.entrySet()) {
                    info.setKey("flag-" + entry.getKey(), entry.getValue());
                }

            } catch (Exception eee) {
                _logNameSpace.error("Error adding bits (stickybit) to storageinfo : " + eee);
            }

            //
            // simulate large files
            //
            if (info.getFileSize() == 1L) {

                long largeFilesize = -1L;
                try {
                    String sizeString = info.getKey("flag-l");
                    if (sizeString != null) {
                        largeFilesize = Long.parseLong(sizeString);
                        if (largeFilesize > 0L) {
                            info.setFileSize(largeFilesize);
                        }
                    }
                } catch (NumberFormatException nfe) {
                    /* ignore bad values */
                }
            }

        }
        return info;

    }

    public String[] getFileAttributeList(PnfsId pnfsId) {

        String[] keys = null;

        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo info   = new CacheInfo( pf ) ;
            CacheInfo.CacheFlags flags = info.getFlags() ;
            Set<Map.Entry<String, String>> s = flags.entrySet();

            keys = new String[s.size()];

            int i = 0;
            for( Map.Entry<String, String> entry: s) {
                keys[i++] = entry.getKey() ;
            }
        }catch(Exception e){
            _logNameSpace.error(e.getMessage());
        }

        return keys;

    }

    public Object getFileAttribute(PnfsId pnfsId, String attribute) {

        Object attr = null;
        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            if(pf.isFile()) {
                CacheInfo info   = new CacheInfo( pf ) ;
                CacheInfo.CacheFlags flags = info.getFlags() ;

                attr =  flags.get(attribute);
            }else{

                _logNameSpace.debug("getFileAttribute on non file object");
            }
        }catch( Exception e){
            _logNameSpace.error(e.getMessage());
        }

        return attr;
    }

    public void setFileAttribute(PnfsId pnfsId, String attribute, Object data) {

        try {
            PnfsFile  pf     = _pathManager.getFileByPnfsId( pnfsId );
            if(pf.isFile() ) {
                CacheInfo info   = new CacheInfo( pf ) ;
                CacheInfo.CacheFlags flags = info.getFlags() ;

                flags.put( attribute ,  data.toString()) ;

                info.writeCacheInfo( pf ) ;
            }else{
                _logNameSpace.warn("setFileAttribute on non file object");
            }
        }catch( Exception e){
            _logNameSpace.error(e.getMessage());
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
            _logNameSpace.error(e.getMessage());
        }

    }


    public void renameEntry(PnfsId pnfsId, String newName) throws Exception {

        File src = new File( _pathManager.globalToLocal( this.pnfsidToPath(pnfsId) ) );
        String localPath = _pathManager.globalToLocal(newName);

        if( !src.renameTo( new File(localPath) ) ) {
            throw new CacheException(2817, "Failed to rename " + pnfsId + " (" + src.getAbsolutePath() + ") " +" to " + newName + " (" + localPath + ") ");
        }
    }

    public void addChecksum(PnfsId pnfsId, int type, String value) throws Exception
    {
        _attChecksumImpl.setChecksum(pnfsId,value,type);
    }

    public String getChecksum(PnfsId pnfsId, int type) throws Exception
    {
        return _attChecksumImpl.getChecksum(pnfsId,type);
    }
    public void removeChecksum(PnfsId pnfsId, int type) throws Exception
    {
        _attChecksumImpl.removeChecksum(pnfsId,type);
    }

    public int[] listChecksumTypes(PnfsId pnfsId ) throws Exception
    {
        return _attChecksumImpl.types(pnfsId);
    }


    ////////////////////////////////////////////////////
    ////////////////////////////////////////////////////
    private static class PathManager {
        private final PathMap _globalPathMap = new PathMap() ;
        private final Map<String, PnfsFile.VirtualMountPoint>     _servers       = new HashMap<String, PnfsFile.VirtualMountPoint> () ;
        private File    _defaultMountPoint = null;
        private String  _defaultServerName = null;
        private String  _defaultServerId   = null;

        private PathManager( List<VirtualMountPoint> virtualMountPoints ){

            for( PnfsFile.VirtualMountPoint vmp: virtualMountPoints ){
                _globalPathMap.add( vmp.getVirtualGlobalPath() , vmp ) ;
                _servers.put( vmp.getServerId()  , vmp ) ;
            }

            if( _servers.isEmpty() ) {
                throw new
                NoSuchElementException("Emtpy server list" ) ;
            }

            PnfsFile.VirtualMountPoint vmp = virtualMountPoints.get(0);

            _defaultServerName = vmp.getServerName() ;
            _defaultServerId = vmp.getServerId() ;
            _defaultMountPoint = getVmpByDomain( _defaultServerId ).getRealMountPoint()  ;


        }
        private PnfsFile.VirtualMountPoint getVmpByDomain( String domain ){

            String resolvedDomain = ( (domain == null ) ||  domain.length() == 0 ) ? _defaultServerId : domain ;
            PnfsFile.VirtualMountPoint vmp = _servers.get( resolvedDomain ) ;

            if( vmp == null ) {
                throw new
                NoSuchElementException("No server found for : "+domain ) ;
            }

            return vmp ;
        }
        public File getMountPointByPnfsId( PnfsId pnfsId ){
            String domain = pnfsId.getDomain() ;
            if( ( domain == null ) || ( domain.equals("") ) )
                return _defaultMountPoint ;

            PnfsFile.VirtualMountPoint vmp = getVmpByDomain( domain ) ;

            return vmp.getRealMountPoint() ;
        }
        public PnfsFile getFileByPnfsId( PnfsId pnfsId ) throws CacheException {
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

            /*
             * there is no simulated file size for directories
             */
            if( pf.isFile() ) {
                CacheInfo cinfo  = new CacheInfo( pf ) ;
                CacheInfo.CacheFlags flags = cinfo.getFlags() ;



                String simulatedFileSizeString = flags.get("l");

                if( simulatedFileSizeString != null ) {
    	            try{
    	            	simulatedFileSize =  Long.parseLong(simulatedFileSizeString);
    	            }catch(NumberFormatException ignored){/* bad values ignored */}
                }
            }
        // TODO: handle file not found
        }catch(Exception eee ){
            _logNameSpace.error( "Error obtaining 'l' flag for getSimulatedFilesize : "+eee ) ;
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

                meta.setTimes( aTime *1000, mTime *1000, cTime *1000) ;

                if (_logNameSpace.isDebugEnabled()) {
                    _logNameSpace.debug("getFileMetaData of " + pnfsId + " -> "
                            + meta);
                }

                return meta ;

            }catch(NoSuchElementException nse) {
                throw new
                IOException("Illegal meta data format : "+pnfsId+" ("+line+")" ) ;
            }catch(NumberFormatException eee ){
                throw new
                IOException("Illegal meta data format : "+pnfsId+" ("+line+")" ) ;
            }
        }catch(FileNotFoundException fnf ) {
        	throw new FileNotFoundCacheException("no such file or directory " + pnfsId.getId() );
        }finally{
            if(br != null)try{ br.close() ; }catch(IOException ee){/* too late to react */}
        }

    }

    private void setFileMetaData( File mp , PnfsId pnfsId , int level,  FileMetaData newMetaData)
    throws Exception {

        String hexTime = Long.toHexString( System.currentTimeMillis() / 1000L ) ;
        int l = hexTime.length() ;
        if( l > 8 )hexTime = hexTime.substring(l-8) ;
        StringBuilder sb = new StringBuilder(128);
        sb.append(".(pset)(").append(pnfsId.getId()).append(")(attr)(").
        append(level).append(")(").
        append(Integer.toOctalString(0100000|toUnixMode(newMetaData))).append(":").
        append(newMetaData.getUid()).append(":").
        append(newMetaData.getGid()).append(":").
        append(hexTime).append(":").
        append(hexTime).append(":").
        append(hexTime).append(")") ;

        File metaFile = new File( mp , sb.toString() ) ;

        /*
         * due to bug in pnfsd, which copies directory inode into virtual '.(pset)' file inode and
         * keeps IS_DIR mask and due to fact, that SUN JVM checks mode after open/create ( fstat() ) and
         * throws 'Is a Directory' IOException, wrap create operation with try-catch block.
         *
         * JVM code:
         *
         *   int open64_w(const char *path, int oflag, int mode)
         *   {
         *       int result = open64(path, oflag, mode);
         *       if (result != -1) {
         *           // If the open succeeded, the file might still be a directory
         *           int st_mode;
         *           if (sysFfileMode(result, &st_mode) != -1) {
         *               if ((st_mode & S_IFMT) == S_IFDIR) {
         *                   errno = EISDIR;
         *                   close(result);
         *                   return -1;
         *               }
         *           } else {
         *               close(result);
         *               return -1;
         *           }
         *       }
         *       return result;
         *   }
         *
         */

        try {
            metaFile.createNewFile() ;
        }catch(IOException ioe) {
            /*
             *  check for new permissions and re-throw exception
             *        if it not set.
             */

            FileMetaData actualMetaData = getFileMetaData(mp, pnfsId);
            if( ! (
                    actualMetaData.getGroupPermissions().equals( newMetaData.getGroupPermissions() )
                    && actualMetaData.getUserPermissions().equals( newMetaData.getUserPermissions() )
                    && actualMetaData.getWorldPermissions().equals(newMetaData.getWorldPermissions() )
                                                                          ) ) {
                    _logNameSpace.error("failed to apply new attributes to " + pnfsId);
                    _logNameSpace.error("    expected: " + newMetaData);
                    _logNameSpace.error("    actual  : " + actualMetaData);
                throw ioe;
           }
        }

    }

    /*
     * TODO: shell we move this method into FileMetaData class?
     */
    private static int toUnixMode(FileMetaData metaData) {

        int mode = 0;

        // TODO: to be done more elegant

        // user
        if (metaData.getUserPermissions().canRead()) {
            mode |= 0400;
        }

        if (metaData.getUserPermissions().canWrite()) {
            mode |= 0200;
        }
        if (metaData.getUserPermissions().canExecute()) {
            mode |= 0100;
        }

        // group
        if (metaData.getGroupPermissions().canRead()) {
            mode |= 0040;
        }

        if (metaData.getGroupPermissions().canWrite()) {
            mode |= 0020;
        }
        if (metaData.getGroupPermissions().canExecute()) {
            mode |= 0010;
        }

        // world
        if (metaData.getWorldPermissions().canRead()) {
            mode |= 0004;
        }

        if (metaData.getWorldPermissions().canWrite()) {
            mode |= 0002;
        }
        if (metaData.getWorldPermissions().canExecute()) {
            mode |= 0001;
        }

        return mode;
    }

    private void setFileSize( PnfsId pnfsId , long length )throws Exception {
        PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
        pf.setLength(length);
        for( int i = 0 ; i < 10 ; i++ ){
            long size =  pf.length() ;
            if( size == length )break ;
            _logNameSpace.debug( "setLength : not yet ... " ) ;
            Thread.sleep(1000);
        }
    }



    @Override
    public String toString() {

        StringBuffer sb = new StringBuffer();


        sb.append("$Revision$").append("\n");
        for( PnfsFile.VirtualMountPoint vmp: _virtualMountPoints ){

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
       List<String> list = new ArrayList<String>() ;
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
         FileNotFoundCacheException( pnfsId.toString() + " not found" ) ;

       StringBuilder sb = new StringBuilder() ;
       for( int i = list.size() - 1 ; i >= 0 ; i-- ){
          sb.append("/").append(list.get(i)) ;
       }
       return sb.toString() ;
    }
    private String nameOf( File mp , String pnfsId ) throws Exception {

        _logNameSpace.info("nameof for pnfsid" + pnfsId);

       File file = new File( mp , ".(nameof)("+pnfsId+")" ) ;
       BufferedReader br = null;
       try{
    	   br = new BufferedReader(new FileReader( file ) ) ;
          return br.readLine() ;
       }finally{
          if( br != null) try{ br.close() ; }catch(IOException ee){ /* to late to react */}
       }
    }
    private String parentOf( File mp , String pnfsId ) throws Exception {

        _logNameSpace.info("parentof for pnfsid" + pnfsId);
       File file = new File( mp , ".(parent)("+pnfsId+")" ) ;
       BufferedReader br = null;
       try{
    	   br = new BufferedReader(new FileReader( file ) ) ;
          return br.readLine() ;
       }finally{
          if( br != null) try{ br.close() ; }catch(IOException ee){ /* to late to react */}
       }
    }

}
