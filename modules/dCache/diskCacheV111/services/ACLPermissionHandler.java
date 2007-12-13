package diskCacheV111.services;

import java.util.Arrays;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACL;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Permission;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.handler.AclHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAdapter;

public class ACLPermissionHandler implements PermissionHandlerInterface {

	private final CellAdapter _cell;
	/**
	 * if there is no ACL defined for a resource, then we will ask some one else
	 */
	private FileMetaDataSource _metaDataSource;
	private AclHandler _aclHandler;


	private final static Logger _logPermisions = Logger
			.getLogger("logger.org.dcache.authorization."
					+ ACLPermissionHandler.class.getName());

	public ACLPermissionHandler(CellAdapter cell) throws 
            IllegalArgumentException, 
            InstantiationException, 
            IllegalAccessException, 
            InvocationTargetException, ClassNotFoundException, NoSuchMethodException {
		
		_cell = cell;
	
		String aclProperties = 
			parseOption("acl-permission-handler-config", null);
			
		if(aclProperties == null) {
			throw new IllegalArgumentException("acl-permission-handler-config option not defined");
		}

		_aclHandler = new AclHandler(aclProperties);
		
        String metaDataProvider =
            parseOption("meta-data-provider",
                      "diskCacheV111.services.PnfsManagerFileMetaDataSource");
        _logPermisions.debug("Loading metaDataProvider :" + metaDataProvider);
        Class<?> [] argClass = { dmg.cells.nucleus.CellAdapter.class };
        Class<?> fileMetaDataSourceClass;
	
		fileMetaDataSourceClass = Class.forName(metaDataProvider);
		Constructor<?> fileMetaDataSourceCon = fileMetaDataSourceClass.getConstructor( argClass ) ;
				
		Object[] initargs = { _cell };
		_metaDataSource = (FileMetaDataSource)fileMetaDataSourceCon.newInstance(initargs);
				
				
	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can read
	 * file with pnfs-path 'pnfsPath'
	 */
	public boolean canReadFile(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canRead(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + ")");
		}

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);

		if (!fileMetaData.getFileMetaData().isRegularFile()) {
			_logPermisions.error(" Object is not a file, can not read "
					+ pnfsPath);
			return false;
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionREAD = Action.READ;
		Boolean permissionToRead = AclNFSv4Matcher.isAllowed(permission,
				actionREAD);
		return permissionToRead != null && permissionToRead.equals( Boolean.TRUE );

	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can
	 * write into a file with pnfs-path 'pnfsPath'
	 */
	public boolean canWriteFile(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {
		// IN CASE pnfsPath refers to a FILE a/b/c/filename,
		// means "ask permission to write file filename".
		// find ACL of this file a/b/c/filename, action WRITE

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canWriteFile(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + ")");
		}

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);

		if (!fileMetaData.getFileMetaData().isRegularFile()) {
			_logPermisions.error(" Object is not a file, can not write "
					+ pnfsPath);
			return false;
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionWRITE = Action.WRITE;
		Boolean permissionToWriteFile = AclNFSv4Matcher.isAllowed(permission,
				actionWRITE);
		return permissionToWriteFile != null
				&& permissionToWriteFile.equals( Boolean.TRUE );
	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can
	 * create a file with pnfs-path 'pnfsPath' (sample pnfsPath
	 * "/pnfs/desy.de/data/newfilename")
	 */
	public boolean canCreateFile(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {
		// IN CASE pnfsPath refers to a DIRECTORY a/b/c,
		// means "ask permission to create file in this directory a/b/c".
		// find ACL of this directory a/b/c, check action CREATE (file)

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canCreateFile(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + ")");
		}

		// get pnfsPath of parent directory
		int last_slash_pos = pnfsPath.lastIndexOf('/');
		String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);

		FileMetaDataX fileMetaData = _metaDataSource
				.getXMetaData(pnfsPathParent);

		if (!fileMetaData.getFileMetaData().isDirectory()) {
			_logPermisions
					.error(" Object is not a directory, can not create directory in "
							+ pnfsPath);
			return false;
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionCREATE = Action.CREATE;
		Boolean permissionToCreateFile = AclNFSv4Matcher.isAllowed(permission,
				actionCREATE, Boolean.FALSE);
		// in case 'null' is returned, action CREATE is denied:
		return permissionToCreateFile != null
				&& permissionToCreateFile.equals( Boolean.TRUE );
	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can
	 * create a directory with pnfs-path 'pnfsPath' (sample pnfsPath
	 * "/pnfs/desy.de/data/newdirname")
	 */
	public boolean canCreateDir(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canCreateDir(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + ")");
		}

		// get pnfsPath of parent directory
		int last_slash_pos = pnfsPath.lastIndexOf('/');
		String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);

		FileMetaDataX fileMetaData = _metaDataSource
				.getXMetaData(pnfsPathParent);

		if (!fileMetaData.getFileMetaData().isDirectory()) {
			_logPermisions.error(pnfsPathParent
					+ " exists and is not a directory, can not create "
					+ pnfsPath);
			return false;
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionCREATE = Action.CREATE;
		Boolean permissionToCreateDir = AclNFSv4Matcher.isAllowed(permission,
				actionCREATE, Boolean.TRUE);
		// in case of 'undefined', that is null: action CREATE will be denied:
		return permissionToCreateDir != null
				&& permissionToCreateDir.equals( Boolean.TRUE );

	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can
	 * delete Directory with pnfs-path 'pnfsPath' (sample pnfsPath  "/pnfs/desy.de/data/dir1").
	 * For this: ask for permission to perform action REMOVE for this directory
	 *          (that is, bit DELETE for /pnfs/desy.de/data/dir1 will be checked)
	 *           and
	 *           ask for permission to perform action REMOVE for parent directory /pnfs/desy.de/data
	 *           (that is, bit DELETE_CHILD for /pnfs/desy.de/data will be checked)
	 *           If both are allowed, then the original action REMOVE directory is allowed.
	 */
	public boolean canDeleteDir(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {

        boolean isAllowed = false; 		

		FileMetaDataX dirMetaData = _metaDataSource.getXMetaData(pnfsPath);

		if (!dirMetaData.getFileMetaData().isDirectory()) {
			_logPermisions.error(pnfsPath + " is not a directory");
			throw new CacheException("path is not a directory");
		}

		// get pnfsPath of parent directory
		int last_slash_pos = pnfsPath.lastIndexOf('/');
		String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);

		FileMetaDataX parentDirMetaData = _metaDataSource.getXMetaData(pnfsPathParent);

		if (!parentDirMetaData.getFileMetaData().isDirectory()) {
			_logPermisions.error(pnfsPathParent + " (parent) is not a directory");
			throw new CacheException("parent path is not a directory");
		}

        /////////////////////////////////////////////////////////////////
		//Ask for permission to perform action REMOVE for the given directory
		PnfsId dirPnfsID = dirMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(dirPnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = dirMetaData.getFileMetaData().getUid();
		int gidOwner = dirMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		// Explanation. We use the following method (from ACLNFSv4Matcher) here:
		// Boolean isAllowed(Permission perm, Action action, Boolean isDir)
		// isDir in this case will be always Boolean.FALSE, as we are talking 
		//about the object to be deleted (in our case it is actually a directory), 
		// and for this object bit DELETE has to be checked. According to the implementation
		// of isAlowed(..), bit DELETE is checked only in case isDir is set to FALSE. 
		Action actionREMOVEdir = Action.REMOVE;
		Boolean permissionToRemoveDir = AclNFSv4Matcher.isAllowed(permission,
				actionREMOVEdir, Boolean.FALSE);

		Boolean decision1 = permissionToRemoveDir != null
				&& permissionToRemoveDir.equals( Boolean.TRUE );

        ///////////////////////////////////////////////////////////////////
		//Ask for permission to perform action REMOVE for parent directory

		PnfsId pnfsIDparent = parentDirMetaData.getPnfsId();

		ACL acl2 = _aclHandler.getACL(pnfsIDparent.toString());

		// Get Owner of this directory :
		int uidOwner2 = parentDirMetaData.getFileMetaData().getUid();
		int gidOwner2 = parentDirMetaData.getFileMetaData().getGid();

		Owner owner2 = new Owner(uidOwner2, gidOwner2);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Owner : " + owner2.toString());
			_logPermisions.debug("ACL : " + acl2.toString());
		}

		Permission permission2 = AclMapper.getPermission(subject, userOrigin,
				owner2, acl2);

		Action actionREMOVEchild = Action.REMOVE;
		Boolean permissionToRemoveChild = AclNFSv4Matcher.isAllowed(permission2,
				actionREMOVEchild, Boolean.TRUE);

		Boolean decision2 = permissionToRemoveChild != null
		&& permissionToRemoveChild.equals( Boolean.TRUE );
		
		//Decision
		isAllowed = decision1 && decision2;
        
		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canDeleteDir(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + "):"+isAllowed);
		}
		
		return isAllowed;

	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can list
	 * a directory with pnfs-path 'pnfsPath' (sample pnfsPath
	 * "/pnfs/desy.de/data/dir1/dir2")
	 */
	public boolean canListDir(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canListDir(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + ")");
		}

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);

		if (!fileMetaData.getFileMetaData().isDirectory()) {
			_logPermisions.error(pnfsPath + " is not a directory");
			throw new CacheException("path is not a directory");
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionREADDIR = Action.READDIR;
		Boolean permissionToListDir = AclNFSv4Matcher.isAllowed(permission,
				actionREADDIR);
		return permissionToListDir != null
				&& permissionToListDir.equals( Boolean.TRUE );

	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can
	 * delete file with pnfs-path 'pnfsPath' (sample pnfsPath
	 * "/pnfs/desy.de/data/dir1/filename").
	 * For this: ask for permission to perform action REMOVE for file filename
	 *          (that is, bit DELETE for /pnfs/desy.de/data/dir1/filename will be checked)
	 *           and
	 *           ask for permission to perform action REMOVE for parent directory /pnfs/desy.de/data/dir1
	 *           (that is, bit DELETE_CHILD for /pnfs/desy.de/data/dir1 will be checked)
	 *           If both are allowed, then the original action REMOVE is allowed.
	 */
	public boolean canDeleteFile(Subject subject, String pnfsPath,
			Origin userOrigin) throws CacheException {

		boolean isAllowed = false;

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);

		if (!fileMetaData.getFileMetaData().isRegularFile()) {
			_logPermisions.error(pnfsPath + " is not a regular file");
			throw new CacheException("path is not a file");
		}

		// get pnfsPath of parent directory
		int last_slash_pos = pnfsPath.lastIndexOf('/');
		String pnfsPathParent = pnfsPath.substring(0, last_slash_pos);

		FileMetaDataX fileMetaDataParent = _metaDataSource.getXMetaData(pnfsPathParent);

		if (!fileMetaDataParent.getFileMetaData().isDirectory()) {
			_logPermisions.error(pnfsPathParent + " is not a directory");
			throw new CacheException("path is not a directory");
		}

        ///////////////////////////////////////////////////////////////////
		//Ask for permission to perform action REMOVE for this file

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this file:
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission1 = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionREMOVEfile = Action.REMOVE;
		Boolean permissionToRemoveFile = AclNFSv4Matcher.isAllowed(permission1,
				actionREMOVEfile, Boolean.FALSE);
		Boolean decision1 = permissionToRemoveFile != null
		&& permissionToRemoveFile.equals( Boolean.TRUE );

		///////////////////////////////////////////////////////////////////
		//Ask for permission to perform action REMOVE for parent directory

		PnfsId pnfsIDparent = fileMetaDataParent.getPnfsId();

		ACL acl2 = _aclHandler.getACL(pnfsIDparent.toString());

		// Get Owner of this directory :
		int uidOwner2 = fileMetaDataParent.getFileMetaData().getUid();
		int gidOwner2 = fileMetaDataParent.getFileMetaData().getGid();

		Owner owner2 = new Owner(uidOwner2, gidOwner2);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Owner : " + owner2.toString());
			_logPermisions.debug("ACL : " + acl2.toString());
		}

		Permission permission2 = AclMapper.getPermission(subject, userOrigin,
				owner2, acl2);

		Action actionREMOVEchild = Action.REMOVE;
		Boolean permissionToRemoveChild = AclNFSv4Matcher.isAllowed(permission2,
				actionREMOVEchild, Boolean.TRUE);

		Boolean decision2 = permissionToRemoveChild != null
		&& permissionToRemoveChild.equals( Boolean.TRUE );
		
        //Decision
		isAllowed = decision1 && decision2;
		
		if (_logPermisions.isDebugEnabled()) {
			_logPermisions
					.debug("canDeleteFile(" + subject.getUid() + ","
							+ Arrays.toString(subject.getGids()) + ","
							+ pnfsPath + "):" + isAllowed);
		}
		
		return isAllowed;
	}

	/**
	 * checks whether the user (defined as 'subject') with 'userOrigin' can set
	 * attributes to the object defined with pnfs-path 'pnfsPath' (sample
	 * pnfsPath "/pnfs/desy.de/data/dir1" or "/pnfs/desy.de/data/file1")
	 */
	public boolean canSetAttributes(Subject subject, String pnfsPath,
			Origin userOrigin, FileAttribute attributes) throws CacheException {

		if (_logPermisions.isDebugEnabled()) {
			_logPermisions.debug("canSetAttributes(" + subject.getUid() + ","
					+ Arrays.toString(subject.getGids()) + "," + pnfsPath
					+ " attribute: " + attributes.toString() + " ) ");
		}

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);
		FileMetaData infoMeta = fileMetaData.getFileMetaData();

		if (!infoMeta.isRegularFile() && !infoMeta.isDirectory()) {
			_logPermisions.error(pnfsPath
					+ " is not a regular file and not a directory");
			throw new CacheException("path is not a file and not a directory");
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionSETATTR = Action.SETATTR;
		// USE: Boolean isAllowed(Permission perm, Action action, FileAttribute attribute)
		Boolean permissionToSetAttributes = AclNFSv4Matcher.isAllowed(
				permission, actionSETATTR, attributes);
		return permissionToSetAttributes != null
				&& permissionToSetAttributes.equals(  Boolean.TRUE );

	}
	
	/**
	 * checks whether the user can get
	 * attributes to the object defined with pnfs-path 'pnfsPath' (sample
	 * pnfsPath "/pnfs/desy.de/data/dir1" or "/pnfs/desy.de/data/file1")
	 */
	public boolean canGetAttributes(Subject subject, String pnfsPath,
			Origin userOrigin, FileAttribute attributes) throws CacheException {
		
		if (_logPermisions.isDebugEnabled()) {
			_logPermisions.debug("canGetAttributes(" + subject.getUid() + ","
					+ Arrays.toString(subject.getGids()) + "," + pnfsPath
					+ " attribute: " + attributes.toString() + " ) ");
		}

		FileMetaDataX fileMetaData = _metaDataSource.getXMetaData(pnfsPath);
		FileMetaData infoMeta = fileMetaData.getFileMetaData();

		if (!infoMeta.isRegularFile() && !infoMeta.isDirectory()) {
			_logPermisions.error(pnfsPath
					+ " is not a regular file and not a directory");
			throw new CacheException("path is not a file and not a directory");
		}

		PnfsId pnfsID = fileMetaData.getPnfsId();

		ACL acl = _aclHandler.getACL(pnfsID.toString());

		// Get Owner of this resource :
		int uidOwner = fileMetaData.getFileMetaData().getUid();
		int gidOwner = fileMetaData.getFileMetaData().getGid();

		Owner owner = new Owner(uidOwner, gidOwner);

		if (_logPermisions.isDebugEnabled()) {

			_logPermisions.debug("Subject : " + subject.toString());
			_logPermisions.debug("Origin : " + userOrigin.toString());
			_logPermisions.debug("Owner : " + owner.toString());
			_logPermisions.debug("ACL : " + acl.toString());
		}

		Permission permission = AclMapper.getPermission(subject, userOrigin,
				owner, acl);

		Action actionGETATTR = Action.GETATTR;
		// USE: Boolean isAllowed(Permission perm, Action action, FileAttribute attribute)
		Boolean permissionToGetAttributes = AclNFSv4Matcher.isAllowed(
				permission, actionGETATTR, attributes);
		return permissionToGetAttributes != null
				&& permissionToGetAttributes.equals(  Boolean.TRUE );

	}
	 /**
     * Returns the value of a named cell argument.
     *
     * @param name the name of the cell argument to return
     * @param def the value to return when <code>name</code> is
     *            not defined or cannot be parsed
     */
    private String parseOption(String name, String def)
    {
        String value;
        String tmp = _cell.getArgs().getOpt(name);
        if (tmp != null && tmp.length() > 0) {
            value = tmp;
        } else {
            value = def;
        }

        if (value != null) {
        	_logPermisions.debug(name + "=" + value);
        }

        return value;
    }
    
}
