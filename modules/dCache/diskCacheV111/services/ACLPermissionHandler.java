package diskCacheV111.services;

import java.net.InetAddress;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACL;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Permission;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.AuthType;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.enums.InetAddressType;
import org.dcache.chimera.acl.handler.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetFileMetaDataMessage;
import dmg.cells.nucleus.CellAdapter;


public class ACLPermissionHandler implements PermissionHandlerInterface  {

    private final CellAdapter _cell;
    /**
     * if there is no ACL defined for a resource,
     * then we will ask some one else
     */
    private static Origin _origin;
    

    
    private final FileMetaDataSource _metaDataSource;
    private final AclHandler _aclHandler;
     
    private static PnfsHandler _pnfs; //Stub object for talking to the PNFS manager.

    private final static Logger _logPermisions = Logger
            .getLogger("logger.org.dcache.authorization."
                    + ACLPermissionHandler.class.getName());

    public ACLPermissionHandler(CellAdapter cell,
            FileMetaDataSource metaDataSource, String aclProperties) {
        _cell = cell;
       _metaDataSource = metaDataSource;
       _aclHandler = new AclHandler(aclProperties);
    //   _fallBackHandler = new ACLPermissionHandler(_cell, new PnfsManagerFileMetaDataSource(_cell));
        
    }
    
  
/////////////////new   canReadFile   READY///////////////////////////
    public boolean canReadFile(Subject subject, String pnfsPath, Origin userOrigin) throws  CacheException, Exception {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canRead(" + subject.getUid()+ "," + Arrays.toString( subject.getGids() ) + ","
                    + pnfsPath + ")");
        }
        
        FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);
         
        PnfsId pnfsID = fileMetaData.getPnfsId();
        
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
    
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
     
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        _origin = new Origin(authenticationType, inetAddressType, inetAddress);
        
       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getFileMetaData().getUid();
        int gidOwner = fileMetaData.getFileMetaData().getGid();
      
        	
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + _origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, _origin, owner, acl);
        
        Action actionREAD=Action.READ;
        Boolean permissionToRead = AclNFSv4Matcher.isAllowed(permission, actionREAD);
        return permissionToRead != null && permissionToRead == Boolean.TRUE; 
        
    }
    
    
    
  ///////////////////  new canWriteFile , former canWrite(..)  READY////////////////////////////////  
    public boolean canWriteFile(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException{
    	 //IN CASE    pnfsPath   refers to a FILE  a/b/c/filename,
        //means "ask permission to write file filename".
        //find ACL of this file a/b/c/filename, action WRITE     
    	
    	if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canWrite(" + subject.getUid() + "," + Arrays.toString( subject.getGids() )+ ","
                + pnfsPath + ")");
        }
        
        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPath);
        FileMetaData fileMetaData = metadataMessage.getMetaData();

     
     if (!fileMetaData.isRegularFile()) { 
    	 _logPermisions.error(
            " Object is not a file, can not write "
             + pnfsPath);
    	 	return false;
     		}       
    
     PnfsId pnfsID = metadataMessage.getPnfsId();
     
     ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
     //public Subject(int uid, int[] gids)
  
     //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
     //for now take default origin _origin
     if (userOrigin ==null) userOrigin=_origin;  //take Default 
     AuthType authenticationType = userOrigin.getAuthType();
     InetAddressType inetAddressType = userOrigin.getAddressType();
     InetAddress inetAddress = userOrigin.getAddress();
     Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

    
     //Get Owner of this resource :
     int uidOwner = fileMetaData.getUid();
     int gidOwner = fileMetaData.getGid();
     Owner owner = new Owner(uidOwner, gidOwner);

     if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
     
     Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
  
        Action actionWRITE=Action.WRITE;  
        Boolean permissionToWriteFile = AclNFSv4Matcher.isAllowed(permission, actionWRITE);
        return permissionToWriteFile != null && permissionToWriteFile == Boolean.TRUE; 
    }     
    
    ///////////  new canCreateFile, former canWrite(..) READY///////////////
    public boolean canCreateFile(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException{
    	 //IN CASE    pnfsPath   refers to a DIRECTORY  a/b/c,
        //means "ask permission to create file in this directory a/b/c".
        //find ACL of this directory a/b/c, check action CREATE (file)    
   	
   	if(_logPermisions.isDebugEnabled() ) {
           _logPermisions.debug("canWrite(" + subject.getUid() + "," + Arrays.toString( subject.getGids() )+ ","
               + pnfsPath + ")");
       }
   	
    //get pnfsPath of parent directory 
    int last_slash_pos = pnfsPath.lastIndexOf('/');
    String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);
     
       PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPathParent);
       FileMetaData fileMetaData = metadataMessage.getMetaData();

    
    if (!fileMetaData.isDirectory()) { 
   	 _logPermisions.error(
           " Object is not a directory, can not create directory here "
            + pnfsPath);
   	 	return false;
    		}       
   
    PnfsId pnfsID = metadataMessage.getPnfsId();
    
    ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
    //public Subject(int uid, int[] gids)
 
 
    //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
    //for now take default origin _origin
    if (userOrigin ==null) userOrigin=_origin;  //take Default 
    AuthType authenticationType = userOrigin.getAuthType();
    InetAddressType inetAddressType = userOrigin.getAddressType();
    InetAddress inetAddress = userOrigin.getAddress();
    Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

   
    //Get Owner of this resource :
    int uidOwner = fileMetaData.getUid();
    int gidOwner = fileMetaData.getGid();
    Owner owner = new Owner(uidOwner, gidOwner);

    if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
    
    Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
 
       Action actionCREATE=Action.CREATE;  
       Boolean permissionToCreateFile = AclNFSv4Matcher.isAllowed(permission, actionCREATE, Boolean.FALSE);
       //return permissionToCreateFile; //can be 'null'
       return permissionToCreateFile != null && permissionToCreateFile == Boolean.TRUE; 
   }
  
   /////////////////////new canCreateDir  READY///////////////////////////////////////////
    public boolean canCreateDir(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canCreateDir(" + subject.getUid() + "," + Arrays.toString( subject.getGids() ) + ","
                + pnfsPath + ")");
        }
        
        //***********old -->
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _metaDataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return false;
        }
        // <---- old  **************
        
        
        //get pnfsPath of parent directory 
        int last_slash_pos = pnfsPath.lastIndexOf('/');
        String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);
        
        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPathParent);
        FileMetaData fileMetaData = metadataMessage.getMetaData();
       
        PnfsId pnfsID = metadataMessage.getPnfsId();
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
        //public Subject(int uid, int[] gids)
     
     
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
        if (userOrigin ==null) userOrigin=_origin;  //take Default 
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        
        Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getUid();
        int gidOwner = fileMetaData.getGid();
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
        
        Action actionCREATE=Action.CREATE;
        Boolean permissionToCreateDir = AclNFSv4Matcher.isAllowed(permission, actionCREATE, Boolean.TRUE);
        // in case of 'undefined', that is null:  "false" will be returned (that is "deny") 
        return permissionToCreateDir != null && permissionToCreateDir == Boolean.TRUE; 
        
        //or maybe:
        //       return permissionToCreateDir;
        //in this case 'null' can be returned if permission to perform this action is 'undefined'
    }
////////////////// new  canDeleteDir ()  READY////////////////////////
    public boolean canDeleteDir(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDeleteDir(" + subject.getUid() + "," + Arrays.toString( subject.getGids() ) + ","
                + pnfsPath + ")");
        }

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath + " is not a directory");
            throw new CacheException("path is not a directory");
        }

        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPath);
        FileMetaData fileMetaData = metadataMessage.getMetaData();
         
        PnfsId pnfsID = metadataMessage.getPnfsId();
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
        //public Subject(int uid, int[] gids)

     
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
        if (userOrigin ==null) userOrigin=_origin;  //take Default 
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getUid();
        int gidOwner = fileMetaData.getGid();
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
        
        Action actionREMOVEdir=Action.REMOVE;
        Boolean permissionToRemoveDir = AclNFSv4Matcher.isAllowed(permission, actionREMOVEdir, Boolean.TRUE);
        return permissionToRemoveDir != null && permissionToRemoveDir == Boolean.TRUE; 
       
    }
    
//////////////////new  canListDir ()  READY////////////////////////
    public boolean canListDir(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canListDir(" + subject.getUid() + "," + Arrays.toString( subject.getGids() ) + ","
                + pnfsPath + ")");
        }

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath + " is not a directory");
            throw new CacheException("path is not a directory");
        }

        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPath);
        FileMetaData fileMetaData = metadataMessage.getMetaData();
         
        PnfsId pnfsID = metadataMessage.getPnfsId();
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
        //public Subject(int uid, int[] gids)
    
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
        if (userOrigin ==null) userOrigin=_origin;  //take Default 
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getUid();
        int gidOwner = fileMetaData.getGid();
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
        
        Action actionREADDIR=Action.READDIR;
        Boolean permissionToListDir = AclNFSv4Matcher.isAllowed(permission, actionREADDIR);
        return permissionToListDir != null && permissionToListDir == Boolean.TRUE; 
       
    }
    
//////////////////new  canDeleteFile ()  READY////////////////////////
    public boolean canDeleteFile(Subject subject, String pnfsPath, Origin userOrigin) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDeleteFile(" + subject.getUid() + "," + Arrays.toString( subject.getGids() ) + ","
                + pnfsPath + ")");
        }

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isRegularFile()) {
            _logPermisions.error(pnfsPath + " is not a regular file");
            throw new CacheException("path is not a file");
        }

        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPath);
        FileMetaData fileMetaData = metadataMessage.getMetaData();
         
        PnfsId pnfsID = metadataMessage.getPnfsId();
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
     
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
        if (userOrigin ==null) userOrigin=_origin;  //take Default 
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getUid();
        int gidOwner = fileMetaData.getGid();
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
        
        Action actionREMOVEfile=Action.REMOVE;
        Boolean permissionToRemoveFile = AclNFSv4Matcher.isAllowed(permission, actionREMOVEfile, Boolean.FALSE);
        return permissionToRemoveFile != null && permissionToRemoveFile == Boolean.TRUE; 
       
    }
    
//////////////////new   canSetAttributes (..)  READY////////////////////////
    public boolean canSetAttributes(Subject subject, String pnfsPath, Origin userOrigin, FileAttribute attributes) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canSetAttributes(" + subject.getUid() + "," + Arrays.toString( subject.getGids() ) + ","
                + pnfsPath + " attribute: " + attributes.toString() + " ) ");
        }

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isRegularFile()&&!meta.isDirectory()) {
            _logPermisions.error(pnfsPath + " is not a regular file and not a directory");
            throw new CacheException("path is not a file and not a directory");
        }

        PnfsGetFileMetaDataMessage metadataMessage = _pnfs.getFileMetaDataByPath(pnfsPath);
        FileMetaData fileMetaData = metadataMessage.getMetaData();
         
        PnfsId pnfsID = metadataMessage.getPnfsId();
        
        ACL acl = _aclHandler.getACL(pnfsID.toString());
	 
        //public Subject(int uid, int[] gids)

     
        //User's Origin : userOrigin.getAuthType(), userOrigin.getAddressType();userOrigin.getAddress();
        //for now take default origin _origin
        if (userOrigin ==null) userOrigin=_origin;  //take Default 
        AuthType authenticationType = userOrigin.getAuthType();
        InetAddressType inetAddressType = userOrigin.getAddressType();
        InetAddress inetAddress = userOrigin.getAddress();
        Origin origin = new Origin(authenticationType, inetAddressType, inetAddress);

       
        //Get Owner of this resource :
        int uidOwner = fileMetaData.getUid();
        int gidOwner = fileMetaData.getGid();
        Owner owner = new Owner(uidOwner, gidOwner);

        if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + origin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}
        
        Permission permission=AclMapper.getPermission(subject, origin, owner, acl);
        
        Action actionSETATTR=Action.SETATTR;
        //USE: Boolean isAllowed(Permission perm, Action action, FileAttribute attribute)
        Boolean permissionToSetAttributes = AclNFSv4Matcher.isAllowed(permission, actionSETATTR, attributes);
        return permissionToSetAttributes != null && permissionToSetAttributes == Boolean.TRUE; 
       
    }       

}
