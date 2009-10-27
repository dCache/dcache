/*
 * $Id$
 */

package diskCacheV111.namespace.provider;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.FileFilter;
import java.io.RandomAccessFile;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.dcache.util.Checksum;
import org.dcache.util.Glob;
import org.dcache.util.Interval;

import diskCacheV111.namespace.NameSpaceProvider;
import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.FileExistsCacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileNotFoundCacheException;
import diskCacheV111.util.PathMap;
import diskCacheV111.util.PnfsFile;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.util.StorageInfoExtractable;
import diskCacheV111.util.PnfsFile.VirtualMountPoint;
import diskCacheV111.vehicles.CacheInfo;
import diskCacheV111.vehicles.StorageInfo;
import dmg.cells.nucleus.CellNucleus;
import dmg.util.Args;
import dmg.util.CollectionFactory;
import org.dcache.namespace.FileAttribute;
import org.dcache.namespace.FileType;
import org.dcache.namespace.ListHandler;
import org.dcache.util.ChecksumType;
import org.dcache.vehicles.FileAttributes;
import org.dcache.acl.ACLException;
import org.dcache.acl.handler.singleton.AclHandler;

import javax.security.auth.Subject;

import static org.dcache.auth.Subjects.ROOT;

public class BasicNameSpaceProvider
    implements NameSpaceProvider
{
    private final String            _mountPoint ;
    private final CellNucleus       _nucleus;
    private final List<VirtualMountPoint>              _virtualMountPoints ;
    private final PathManager       _pathManager ;
    private final Args        _args ;
    private final StorageInfoExtractable _extractor;
    private final AttributeChecksumBridge _attChecksumImpl;

    private static final Logger _logNameSpace =  Logger.getLogger("logger.org.dcache.namespace." + BasicNameSpaceProvider.class.getName());
    private final NameSpaceProvider _cacheLocationProvider;

    private final AccessLatency _defaultAccessLatency;
    private final RetentionPolicy _defaultRetentionPolicy;

    private final static long FILE_SIZE_2GB = 0x7FFFFFFFL;

    private final static String ACCESS_LATENCY_FLAG = "al";
    private final static String RETENTION_POLICY_FLAG = "rp";

    /** Creates a new instance of BasicNameSpaceProvider */
    public BasicNameSpaceProvider(Args args, CellNucleus nucleus) throws Exception {


        _nucleus = nucleus;

        _args = args;

        _attChecksumImpl = new AttributeChecksumBridge(this);

        String accessLatensyOption = args.getOpt("DefaultAccessLatency");
            if( accessLatensyOption != null && accessLatensyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultAccessLatency = AccessLatency.getAccessLatency(accessLatensyOption);
            }else{
                _defaultAccessLatency = StorageInfo.DEFAULT_ACCESS_LATENCY;
            }

            String retentionPolicyOption = args.getOpt("DefaultRetentionPolicy");
            if( retentionPolicyOption != null && retentionPolicyOption.length() > 0) {
                /*
                 * IllegalArgumentException thrown if option is invalid
                 */
                _defaultRetentionPolicy = RetentionPolicy.getRetentionPolicy(retentionPolicyOption);
            }else{
                _defaultRetentionPolicy = StorageInfo.DEFAULT_RETENTION_POLICY;
            }

        //
        // get the extractor
        //
        Class<StorageInfoExtractable> exClass = (Class<StorageInfoExtractable>) Class.forName( _args.argv(0)) ;
        Constructor<StorageInfoExtractable>  extractorInit =
            exClass.getConstructor();
        _extractor =  extractorInit.newInstance();


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

        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("Using default pnfs server : "+_pathManager.getDefaultServerName());
        }
        // Process 'delete-registration' arguments
        String location, driverName, userName, passwd;
        if (((location = _args.getOpt("delete-registration")) == null) || (location.equals(""))) {
            _logNameSpace.warn("'delete-registration' is not defined.");
            location = null;
        }
        if (((driverName = _args.getOpt("delete-registration-jdbcDrv")) == null) || (driverName.equals(""))) {
            _logNameSpace.warn("'delete-registration-jdbcDrv' is not defined.");
            driverName = null;
        }
        if (((userName = _args.getOpt("delete-registration-dbUser")) == null) || (userName.equals(""))) {
            _logNameSpace.warn("'delete-registration-dbUser' is not defined.");
            userName = null;
        }
        if (((passwd = _args.getOpt("delete-registration-dbPass")) == null) || (passwd.equals(""))) {
            _logNameSpace.warn("'delete-registration-dbPass' is not defined.");
            passwd = null;
        }
        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("'delete-registration' set to " + location);
        }
        if ((location != null) && location.startsWith("/")) {
            FsTrash trash = new FsTrash(location);
            PnfsFile.setTrash(trash);
        } else if ((location != null) && location.startsWith("jdbc:")) {
            DbTrash trash = new DbTrash(location, driverName, userName, passwd);
            PnfsFile.setTrash(trash);
        } else if ((location != null) && location.startsWith("pnfs:")) {
            NfsTrash trash = new NfsTrash(_mountPoint);
            PnfsFile.setTrash(trash);
        }
        else {
            _logNameSpace.info("Empty trash is selected");
        }

        /*
         * private reference of cache info provider.
         * The DcacheNameSpaceProviderFactorys will take care that we have only
         * one instance of each type of provider.
         */
        String cacheLocation_provider = _args.getOpt("cachelocation-provider");
        if (cacheLocation_provider != null) {
            _logNameSpace.debug("CacheLocation provider: " + cacheLocation_provider);
            DcacheNameSpaceProviderFactory cacheLocationProviderFactory = (DcacheNameSpaceProviderFactory) Class.forName(cacheLocation_provider).newInstance();
            _cacheLocationProvider = (NameSpaceProvider)cacheLocationProviderFactory.getProvider(_args, _nucleus);
        }else{
            _cacheLocationProvider = this;
        }
    }

    public void addCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation)
        throws CacheException
    {
        if (_logNameSpace.isDebugEnabled()) {
            _logNameSpace.debug("add cache location " + cacheLocation + " for "
                    + pnfsId);
        }
        try {
            PnfsFile  pf = _pathManager.getFileByPnfsId( pnfsId );
            CacheInfo ci = new CacheInfo(pf);
            ci.addCacheLocation( cacheLocation);
            ci.writeCacheInfo(pf);
        } catch (IOException e){
            _logNameSpace.error("Exception in addCacheLocation "+e);
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    public void clearCacheLocation(Subject subject, PnfsId pnfsId, String cacheLocation, boolean removeIfLast) throws CacheException {

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
                    this.deleteEntry(subject, pnfsId);

                }
            }
        } catch (Exception e){
            _logNameSpace.error("Exception in clearCacheLocation for : "+pnfsId+" -> "+e);
            //no reply to this message
        }
    }

    public PnfsId createEntry(Subject subject, String name, FileMetaData metaData, boolean isDirectory ) throws CacheException {


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

        File parent = pf.getParentFile();

        boolean rc;
        try {
            if (isDirectory) {
                rc = pf.mkdir();
            } else {
                rc = pf.createNewFile();
            }
        } catch (IOException e) {
            if (parent.isDirectory()) {
                _logNameSpace.error("Failed to create " + globalPath + ": "
                                    + e.getMessage());
                throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                         "IO Error creating " + name
                                         + ": " + e.getMessage());
            } else {
                rc = false;
            }
        }

        if (!rc) {
            if (!parent.exists()) {
                throw new FileNotFoundCacheException("No such file or directory: " + name);
            } else if (!parent.isDirectory()) {
                throw new NotDirCacheException("Not a directory: " + name);
            } else {
                throw new FileExistsCacheException("File exists: " + name);
            }
        }

        if( ! pf.isPnfs() ){
            _logNameSpace.warn("requested path ["+globalPath+"], is not an pnfs path");
            pf.delete();
            throw new IllegalArgumentException( "Not a pnfs file system");
        }


        pnfsId = pf.getPnfsId();
        try {
            this.setFileMetaData(subject, pnfsId, metaData);
        } catch (RuntimeException e) {
            pf.delete();
            throw e;
        } catch (CacheException e) {
            pf.delete();
            throw e;
        }

        if(_logNameSpace.isDebugEnabled() ) {
        	_logNameSpace.debug("Created new entry ["+globalPath+"], id: " + pnfsId.toString());
        }
        return pf.getPnfsId() ;
    }

    public void deleteEntry(Subject subject, PnfsId pnfsId) throws CacheException {

        String  pnfsIdPath = this.pnfsidToPath(subject, pnfsId) ;
        _logNameSpace.debug("delete PNFS entry for " + pnfsId );

        deleteEntry(subject, pnfsIdPath);
    }

    public void deleteEntry(Subject subject, String path) throws CacheException {

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

    public List<String> getCacheLocation(Subject subject, PnfsId pnfsId) throws CacheException
    {
        try {
            PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
            if( pf == null || ! pf.exists() ) {
        	throw new FileNotFoundCacheException("no such file or directory" + pnfsId.toString() );
            }

            CacheInfo ci = new CacheInfo(pf);
            if (_logNameSpace.isDebugEnabled()) {
                _logNameSpace.debug("pnfs file = " + pf + " cache info = " + ci);
            }
            return new ArrayList<String>(ci.getCacheLocations());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }


    public FileMetaData getFileMetaData(Subject subject, PnfsId pnfsId) throws CacheException {

        FileMetaData fileMetaData;

        fileMetaData = getFileMetaData( _pathManager.getMountPointByPnfsId(pnfsId) , pnfsId ) ;

        /*
         *  we do not catch any exception here, while we can not react on it
         *  ( FileNotFoundException )
         *  The caller will do it
         */

        return fileMetaData;
    }

    public void setFileMetaData(Subject subject, PnfsId pnfsId, FileMetaData metaData) throws CacheException {

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

    public String pnfsidToPath(Subject subject, PnfsId pnfsId) throws CacheException {

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

    public PnfsId pathToPnfsid(Subject subject, String path, boolean followLinks) throws CacheException {

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

        return pnfsFile.getPnfsId();
    }

    /**
     * HACK -  AccessLatency and RetentionPolicy stored as a level 2 flags
     * FIXME: this information shouldn't be stored here.
     * @param storageInfo
     * @param pnfsFile
     * @throws CacheException
     */
    private void storeAlRpInLevel2(StorageInfo storageInfo, PnfsFile pnfsFile)
        throws IOException
    {
        if (storageInfo.isSetAccessLatency() || storageInfo.isSetRetentionPolicy()) {
            CacheInfo info = new CacheInfo(pnfsFile);
            CacheInfo.CacheFlags flags = info.getFlags();

            if (storageInfo.isSetAccessLatency()) {
                flags.put(ACCESS_LATENCY_FLAG,
                          storageInfo.getAccessLatency().toString());
            }

            if (storageInfo.isSetRetentionPolicy()) {
                flags.put(RETENTION_POLICY_FLAG,
                          storageInfo.getRetentionPolicy().toString());
            }

            info.writeCacheInfo(pnfsFile);
        }
    }

    public void setStorageInfo(Subject subject, PnfsId pnfsId, StorageInfo storageInfo, int mode)
        throws CacheException
    {
        _logNameSpace.debug( "setStorageInfo : "+pnfsId ) ;
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId) ;
        _extractor.setStorageInfo(mountpoint.getAbsolutePath() ,
                                  pnfsId ,
                                  storageInfo,
                                  mode ) ;

        PnfsFile pnfsFile =
            PnfsFile.getFileByPnfsId(mountpoint, pnfsId);
        try {
            storeAlRpInLevel2(storageInfo, pnfsFile);
        } catch (IOException e) {
            throw new CacheException("IO error while updating level 2 of " + pnfsId + ": " + e.getMessage());
        }
    }

    /**
     * Returns the default access latency for a directory.
     *
     * The access latency for a directory is defined through the
     * AccessLatency tag of the directory. If not defined or if set to
     * an invalid value, then the system wide default access latency
     * is returned.
     */
    private AccessLatency getAccessLatencyForDirectory(PnfsFile dir)
    {
        String[] s = dir.getTag("AccessLatency");
        if (s != null) {
            try {
                return AccessLatency.getAccessLatency(s[0].trim());
            } catch (IllegalArgumentException e) {
                _logNameSpace.error("Invalid AccessLatency tag in " + dir);
            }
        }
        return _defaultAccessLatency;
    }


    /**
     * Returns the default retention policy for a directory.
     *
     * The retention policy for a directory is defined through the
     * AccessLatency tag of the directory. If not defined or if set to
     * an invalid value, then the system wide default retention policy
     * is returned.
     */
    private RetentionPolicy getRetentionPolicyForDirectory(PnfsFile dir)
    {
        String[] s = dir.getTag("RetentionPolicy");
        if (s != null) {
            try {
                return RetentionPolicy.getRetentionPolicy(s[0].trim());
            } catch (IllegalArgumentException e) {
                _logNameSpace.error("Invalid RetentionPolicy tag in " + dir);
            }
        }
        return _defaultRetentionPolicy;
    }

    public StorageInfo getStorageInfo(Subject subject, PnfsId pnfsId)
        throws CacheException
    {
        _logNameSpace.debug("getStorageInfo : " + pnfsId);
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId);
        StorageInfo info =
            _extractor.getStorageInfo(mountpoint.getAbsolutePath(), pnfsId);

        _logNameSpace.debug("Storage info " + info);

        PnfsFile pf = _pathManager.getFileByPnfsId(pnfsId);
        if (pf.isDirectory()) {
            info.setAccessLatency(getAccessLatencyForDirectory(pf));
            info.setRetentionPolicy(getRetentionPolicyForDirectory(pf));
        } else if (pf.isFile() && info.isCreatedOnly()) {
            /* Does not contain level 2 yet. AL and RP is defined by
             * directory tags.
             */
            PnfsFile dir = getParentPnfsFile(mountpoint, pf, null);
            info.setAccessLatency(getAccessLatencyForDirectory(dir));
            info.setRetentionPolicy(getRetentionPolicyForDirectory(dir));
        } else if (pf.isFile()) {
            try {
                PnfsFile dir = null;
                CacheInfo cinfo = new CacheInfo(pf);
                CacheInfo.CacheFlags flags = cinfo.getFlags();

                /* Add all level 2 flags to storage info.
                 */
                for (Map.Entry<String, String> entry: flags.entrySet()) {
                    info.setKey("flag-" + entry.getKey(), entry.getValue());
                }

                /* Set access latency from al flag if
                 * defined. Otherwise use directory tags.
                 */
                String al = flags.get(ACCESS_LATENCY_FLAG);
                if (al != null) {
                    try {
                        info.setAccessLatency(AccessLatency.getAccessLatency(al));
                    } catch (IllegalArgumentException e) {
                        throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR, "Invalid access latency in level 2 of " + pnfsId);
                    }
                } else {
                    dir = getParentPnfsFile(mountpoint, pf, dir);
                    info.setAccessLatency(getAccessLatencyForDirectory(dir));
                }

                /* Set access latency from rp flag if
                 * defined. Otherwise use directory tags.
                 */
                String rp = flags.get(RETENTION_POLICY_FLAG);
                if (rp != null) {
                    try {
                        info.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(rp));
                    } catch (IllegalArgumentException e) {
                        throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR, "Invalid retention policy in level 2 of " + pnfsId);
                    }
                } else {
                    dir = getParentPnfsFile(mountpoint, pf, dir);
                    info.setRetentionPolicy(getRetentionPolicyForDirectory(dir));
                }

                /* Simulate large files
                 */
                if (info.getFileSize() == 1L) {
                    try {
                        String sizeString = flags.get("l");
                        if (sizeString != null) {
                            long largeFilesize = Long.parseLong(sizeString);
                            if (largeFilesize > 0L) {
                                info.setFileSize(largeFilesize);
                            }
                        }
                    } catch (NumberFormatException nfe) {
                        throw new CacheException(CacheException.ATTRIBUTE_FORMAT_ERROR, "Invalid length in level 2 of " + pnfsId);
                    }
                }
            } catch (IOException e) {
                throw new CacheException("IO error while reading level 2 of " + pnfsId);
            }
        }
        return info;
    }

    public String[] getFileAttributeList(Subject subject, PnfsId pnfsId) {

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

    public Object getFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {

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

    public void setFileAttribute(Subject subject, PnfsId pnfsId, String attribute, Object data) {

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

    public void removeFileAttribute(Subject subject, PnfsId pnfsId, String attribute) {
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

    public void renameEntry(Subject subject, PnfsId pnfsId,
                            String newName, boolean overwrite)
        throws CacheException
    {
        File src =
            new File(_pathManager.globalToLocal(this.pnfsidToPath(subject,
                                                                  pnfsId)));
        File dest = new File(_pathManager.globalToLocal(newName));

        if (!overwrite && dest.exists()) {
            throw new FileExistsCacheException("File exists: " + dest);
        }

        if (!src.renameTo(dest)) {
            if (!src.exists()) {
                throw new FileNotFoundCacheException("No such file or directory: " + pnfsId);
            }
            if (!dest.getParentFile().isDirectory()) {
                throw new NotDirCacheException("No such directory: " + dest.getParent());
            }

            throw new CacheException("Failed to rename " + pnfsId
                                     + " to " + newName);
        }
    }

    public void addChecksum(Subject subject, PnfsId pnfsId, int type, String value) throws CacheException
    {
        _attChecksumImpl.setChecksum(subject, pnfsId,value,type);
    }

    public String getChecksum(Subject subject, PnfsId pnfsId, int type) throws CacheException
    {
        return _attChecksumImpl.getChecksum(subject, pnfsId,type);
    }

    public Set<Checksum> getChecksums(Subject subject, PnfsId pnfsId) throws CacheException {
        return _attChecksumImpl.getChecksums(subject, pnfsId);
    }

    public void removeChecksum(Subject subject, PnfsId pnfsId, int type) throws CacheException
    {
        _attChecksumImpl.removeChecksum(subject, pnfsId,type);
    }

    public int[] listChecksumTypes(Subject subject, PnfsId pnfsId ) throws CacheException
    {
        return _attChecksumImpl.types(subject, pnfsId);
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
        public File getMountPointByPnfsId(PnfsId pnfsId){
            String domain = pnfsId.getDomain() ;
            if( ( domain == null ) || ( domain.equals("") ) )
                return _defaultMountPoint ;

            PnfsFile.VirtualMountPoint vmp = getVmpByDomain( domain ) ;

            return vmp.getRealMountPoint() ;
        }
        public PnfsFile getFileByPnfsId(PnfsId pnfsId) throws CacheException {
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



    private FileMetaData getFileMetaData( File mp , PnfsId pnfsId )throws CacheException{
        BufferedReader br = null;

        try{

            File metafile = new File( mp , ".(getattr)("+pnfsId.getId()+")" ) ;
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

                File orgfile  = new File( mp , ".(access)("+pnfsId.getId()+")" ) ;
                long filesize = orgfile.length() ;

                /**
                 *  1 is the magic number indicating that a file is >= 2GiB, so we need
                 *  to look up the filesize in level-2 metadata.   NB. We also lookup when
                 *  filesize is zero. This is deliberate.  It is needed to work around
                 *  potential failure to write filesize in PNFS.
                 */
                if( filesize <= 1L) {
                    long simFilesize = getSimulatedFilesize( pnfsId ) ;
                    filesize = (simFilesize < 0L) ? filesize : simFilesize;
                }

                meta.setSize(  filesize);

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
//        	throw new FileNotFoundCacheException("no such file or directory " + pnfsId.getId() );
            boolean deleted = PnfsFile.isDeleted(pnfsId);
            if (deleted)
                throw new FileNotFoundCacheException("no such file or directory " + pnfsId.getId() );
            else
                throw new CacheException(CacheException.NOT_IN_TRASH, "Not in trash: " + pnfsId.toString());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }finally{
            if(br != null)try{ br.close() ; }catch(IOException ee){/* too late to react */}
        }

    }

    private void setFileMetaData( File mp , PnfsId pnfsId , int level,  FileMetaData newMetaData)
    throws CacheException {

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
            long delay = 1;
            while (true) {
                /* Synchronously create the pset file.
                 */
                try {
                    RandomAccessFile raf =
                        new RandomAccessFile(metaFile, "rws");
                    raf.close();
                    return;
                } catch (IOException e) {
                    if (getFileMetaData(mp, pnfsId).equalsPermissions(newMetaData)) {
                        return;
                    }
                    _logNameSpace.warn("Failed to set permissions: " +
                                       e.getMessage());
                }

                /* Don't retry forever.
                 */
                if (delay > 1024) {
                    throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                             "Failed to set permissions");
                }

                /* Exponential back-off.
                 */
                Thread.sleep(delay);
                delay = delay * 2;

                /* Clear any cached result.
                 */
                metaFile.delete();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "Failed to set permissions: Operation was interrupted");
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

    private void setFileSize( PnfsId pnfsId , long length )throws CacheException {
        try {
            PnfsFile pf = _pathManager.getFileByPnfsId( pnfsId );
            pf.setLength(length);
            for( int i = 0 ; i < 10 ; i++ ){
                long size =  pf.length() ;
                if( size == length )break ;
                _logNameSpace.debug( "setLength : not yet ... " ) ;
                Thread.sleep(1000);
            }
        } catch (InterruptedException e) {
            throw new CacheException("Operation interrupted");
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
   private String pathfinder( PnfsId pnfsId ) throws CacheException {
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

    private String nameOf( File mp , String pnfsId )
        throws IOException
    {
       File file = new File( mp , ".(nameof)("+pnfsId+")" ) ;

       if (_logNameSpace.isInfoEnabled() ) {
           _logNameSpace.info("nameof for pnfsid " + pnfsId);
       }

       BufferedReader br = null;
       try{
    	   br = new BufferedReader(new FileReader( file ) ) ;
          return br.readLine() ;
       }finally{
          if( br != null) try{ br.close() ; }catch(IOException ee){ /* to late to react */}
       }
    }
    private String parentOf( File mp , String pnfsId ) throws IOException {

        if (_logNameSpace.isInfoEnabled() ) {
            _logNameSpace.info("parent for pnfsid " + pnfsId);
        }

       File file = new File( mp , ".(parent)("+pnfsId+")" ) ;
       BufferedReader br = null;
       try{
    	   br = new BufferedReader(new FileReader( file ) ) ;
          return br.readLine() ;
       }finally{
          if( br != null) try{ br.close() ; }catch(IOException ee){ /* to late to react */}
       }
    }

    public PnfsId getParentOf(Subject subject, PnfsId pnfsId) throws CacheException
    {
        try {
            File mp = _pathManager.getMountPointByPnfsId(pnfsId);
            return new PnfsId(parentOf(mp, pnfsId.toString()));
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
    public FileAttributes getFileAttributes(Subject subject, PnfsId pnfsId,
                                            Set<FileAttribute> attr)
        throws CacheException
    {
        PnfsFile pf = _pathManager.getFileByPnfsId(pnfsId);
        CacheInfo cacheInfo = null;
        FileMetaData meta = null;
        FileAttributes attributes = new FileAttributes();

        try {
            for (FileAttribute attribute: attr) {
                switch (attribute) {
                case ACL:
                    if (AclHandler.getAclConfig().isAclEnabled()) {
                        attributes.setAcl(AclHandler.getACL(pnfsId.toString()));
                    } else {
                        attributes.setAcl(null);
                    }
                    break;
                case ACCESS_LATENCY:
                    cacheInfo = getCacheInfo(pf, cacheInfo);
                    attributes.setAccessLatency(AccessLatency.getAccessLatency(cacheInfo.getFlags().get(ACCESS_LATENCY_FLAG)));
                    break;
                case RETENTION_POLICY:
                    cacheInfo = getCacheInfo(pf, cacheInfo);
                    attributes.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(cacheInfo.getFlags().get(ACCESS_LATENCY_FLAG)));
                    break;
                case SIZE:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    attributes.setSize(meta.getFileSize());
                    break;
                case MODIFICATION_TIME:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    attributes.setModificationTime(meta.getLastModifiedTime());
                    break;
                case OWNER:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    attributes.setOwner(meta.getUid());
                    break;
                case OWNER_GROUP:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    attributes.setGroup(meta.getGid());
                    break;
                case CHECKSUM:
                    attributes.setChecksums(_attChecksumImpl.getChecksums(subject, pnfsId));
                    break;
                case LOCATIONS:
                    if (_cacheLocationProvider != this) {
                        attributes.setLocations(_cacheLocationProvider.getCacheLocation(subject, pnfsId));
                    } else {
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        attributes.setLocations(cacheInfo.getCacheLocations());
                    }
                    break;
                case FLAGS:
                    cacheInfo = getCacheInfo(pf, cacheInfo);
                    Map<String,String> flags = CollectionFactory.newHashMap();
                    for (Map.Entry<String,String> e: cacheInfo.getFlags().entrySet()) {
                        flags.put(e.getKey(), e.getValue());
                    }
                    attributes.setFlags(flags);
                    break;
                case TYPE:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    if (meta.isRegularFile()) {
                        attributes.setFileType(FileType.REGULAR);
                    } else if (meta.isDirectory()) {
                        attributes.setFileType(FileType.DIR);
                    } else if (meta.isSymbolicLink()) {
                        attributes.setFileType(FileType.LINK);
                    } else {
                        attributes.setFileType(FileType.SPECIAL);
                    }
                    break;
                case MODE:
                    meta = getFileMetaData(subject, pnfsId, meta);
                    attributes.setMode(meta.getMode());
                    break;
                case PNFSID:
                    attributes.setPnfsId(pnfsId);
                    break;
                default:
                    throw new UnsupportedOperationException("Attribute " + attribute + " not supported yet.");
                }
            }
            return attributes;
        } catch (ACLException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        } catch (IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     e.getMessage());
        }
    }

    @Override
    public void setFileAttributes(Subject subject, PnfsId pnfsId, FileAttributes attr) throws CacheException {

        PnfsFile pf = _pathManager.getFileByPnfsId(pnfsId);
        File mountpoint = _pathManager.getMountPointByPnfsId(pnfsId);
        PnfsFile dir = null;
        CacheInfo cacheInfo = null;

        try {
            for (FileAttribute attribute : attr.getDefinedAttributes()) {
                switch (attribute) {
                    case ACCESS_LATENCY:
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        cacheInfo.getFlags().put(ACCESS_LATENCY_FLAG,
                                attr.getAccessLatency().toString());
                        break;
                    case RETENTION_POLICY:
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        cacheInfo.getFlags().put(RETENTION_POLICY_FLAG,
                                attr.getRetentionPolicy().toString());
                        break;
                    case DEFAULT_ACCESS_LATENCY:
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        /* update the value only if someone did not
                         * set it yet
                         */
                        if (cacheInfo.getFlags().get(ACCESS_LATENCY_FLAG) == null) {
                            dir = getParentPnfsFile(mountpoint, pf, dir);
                            cacheInfo.getFlags().put(ACCESS_LATENCY_FLAG,
                                                     getAccessLatencyForDirectory(dir).toString());
                        }
                        break;
                    case DEFAULT_RETENTION_POLICY:
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        /* update the value only if someone did not
                         * set it yet
                         */
                        if (cacheInfo.getFlags().get(RETENTION_POLICY_FLAG) == null) {
                            dir = getParentPnfsFile(mountpoint, pf, dir);
                            cacheInfo.getFlags().put(RETENTION_POLICY_FLAG,
                                                     getRetentionPolicyForDirectory(dir).toString());
                        }
                        break;
                    case SIZE:
                        long size = attr.getSize();
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        cacheInfo.getFlags().put("l", Long.toString(size));
                        if( size > FILE_SIZE_2GB) {
                            pf.setLength(1);
                        }else{
                            pf.setLength(size);
                        }
                        break;
                    case CHECKSUM:
                        cacheInfo = getCacheInfo(pf, cacheInfo);

                        for(Checksum sum: attr.getChecksums() ) {
                            String flagName;
                            if( sum.getType() == ChecksumType.ADLER32 ) {
                                flagName = "c";
                            } else {
                                flagName = "uc";
                            }
                            ChecksumCollection collection = new ChecksumCollection(cacheInfo.getFlags().get(flagName));

                            String currentValue = collection.get(sum.getType().getType());
                            if( currentValue != null && sum.getValue() != null) {
                                if( !currentValue.equals(sum.getValue())) {
                                    throw new CacheException(CacheException.INVALID_ARGS,
                                           "Checksum mismatch");
                                }
                                continue;
                            }
                            collection.put(sum.getType().getType(), sum.getValue());
                            cacheInfo.getFlags().put(flagName, collection.serialize());
                        }
                        break;
                    case LOCATIONS:
                        for (String location: attr.getLocations()) {
                            if (_cacheLocationProvider != this) {
                                _cacheLocationProvider.addCacheLocation(subject, pnfsId, location);
                            } else {
                                cacheInfo = getCacheInfo(pf, cacheInfo);
                                cacheInfo.addCacheLocation(location);
                            }
                        }
                        break;
                    case FLAGS:
                        cacheInfo = getCacheInfo(pf, cacheInfo);
                        for(Map.Entry<String, String> flag: attr.getFlags().entrySet()) {
                            cacheInfo.getFlags().put(flag.getKey(), flag.getValue());
                        }
                        break;
                   default:
                       throw new UnsupportedOperationException("Attribute " + attribute + " not supported yet.");
                }
            }

            if( cacheInfo != null ) {
                cacheInfo.writeCacheInfo(pf);
            }
        }catch(IOException e) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, e.getMessage());
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
     *
     * The filter always returns false, thus ensuring that we do not
     * construct the full array of all files in the directory.
     */
    private class ListFilter implements FileFilter
    {
        private final Pattern _pattern;
        private final ListHandler _handler;
        private final Set<FileAttribute> _attributes;
        private final Interval _range;
        private long _counter = 0;

        public ListFilter(Pattern pattern,
                          Interval range,
                          Set<FileAttribute> attributes,
                          ListHandler handler)
        {
            _pattern = pattern;
            _range = range;
            _handler = handler;
            _attributes = attributes;
        }

        public boolean accept(File file)
        {
            String name = file.getName();
            if ((_pattern == null || _pattern.matcher(name).matches()) &&
                (_range == null || _range.contains(_counter++))) {
                try {
                    if (_attributes.isEmpty()) {
                        _handler.addEntry(name, null);
                    } else {
                        PnfsId id =
                            pathToPnfsid(ROOT, file.toString(), true);
                        FileAttributes attr =
                            getFileAttributes(ROOT, id, _attributes);
                        _handler.addEntry(name, attr);
                    }
                } catch (CacheException e) {
                    /* Deleting a file during a list operation is not
                     * an error.
                     */
                    if (e.getRc() != CacheException.FILE_NOT_FOUND &&
                        e.getRc() != CacheException.NOT_IN_TRASH) {
                        /* We cannot abort, so we log instead.
                         */
                        _logNameSpace.warn("Lookup failed: " + file);
                    }
                }
            }
            return false;
        }
    }

    @Override
    public void list(Subject subject, String path, Glob glob, Interval range,
                     Set<FileAttribute> attrs, ListHandler handler)
        throws CacheException
    {
        File file = new File(path);
        if (!file.exists()) {
            throw new FileNotFoundCacheException("Directory does not exist");
        }

        if (!file.isDirectory()) {
            throw new NotDirCacheException("Not a directory");
        }

        Pattern pattern = (glob == null) ? null : glob.toPattern();
        File[] list = file.listFiles(new ListFilter(pattern, range, attrs, handler));
        if (list == null) {
            throw new CacheException(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                                     "IO Error");
        }
    }

    /*
     * poor man caching
     */
    private static CacheInfo getCacheInfo(PnfsFile pnfsFile, CacheInfo cacheInfo) throws IOException {
        return (cacheInfo != null) ? cacheInfo : new CacheInfo(pnfsFile);
    }

    /**
     * Returns the parent directory of file.
     *
     * @param mountpoint Mount point of the PNFS file system
     * @param file The for which to return the parent
     * @param parent The parent of file; if non-null this value is returned.
     */
    private static PnfsFile getParentPnfsFile(File mountpoint, PnfsFile file, PnfsFile parent)
        throws CacheException
    {
        if (parent != null) {
            return parent;
        }

        PnfsId id = file.getParentId();
        if (id == null)
            throw new CacheException(36, "Couldn't determine parent ID");
        return PnfsFile.getFileByPnfsId(mountpoint, id);
    }

    /**
     * Returns the meta data of a file.
     *
     * @param subject The subject who performs the operation
     * @param pnfsId The PNFS ID of the file
     * @param meta The meta data of the file; if non-null this value
     *             is returned.
     */
    private FileMetaData getFileMetaData(Subject subject, PnfsId pnfsId,
                                         FileMetaData meta)
        throws CacheException
    {
        return (meta != null) ? meta : getFileMetaData(subject, pnfsId);
    }
}
