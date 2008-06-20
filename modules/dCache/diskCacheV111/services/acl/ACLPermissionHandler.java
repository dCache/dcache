package diskCacheV111.services.acl;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Owner;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.Action;
import org.dcache.chimera.acl.enums.FileAttribute;
import org.dcache.chimera.acl.handler.AclFsHandler;
import org.dcache.chimera.acl.mapper.AclMapper;
import org.dcache.chimera.acl.matcher.AclNFSv4Matcher;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FileMetaDataX;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAdapter;

public class ACLPermissionHandler extends AbstractPermissionHandler {

	private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + ACLPermissionHandler.class.getName());
	private static final boolean IS_DEBUG_ENABLED = logger.isDebugEnabled();

	private final AclFsHandler aclHandler;

	public ACLPermissionHandler(CellAdapter cell) throws ACLException {
		super(cell);

	      String acl_props = cell.getArgs().getOpt("acl-permission-handler-config");
	        if ( acl_props == null || acl_props.length() == 0 )
	            throw new IllegalArgumentException("acl-permission-handler-config option not defined");

	        aclHandler = new AclFsHandler(acl_props);
	}

	public boolean canReadFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canReadWriteFile(pnfsPath, subject, origin, false);
	}
	public boolean canWriteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canReadWriteFile(pnfsPath, subject, origin, true);
	}
	public boolean canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canReadWriteFile(pnfsId, subject, origin, false);
	}
	public boolean canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canReadWriteFile(pnfsId, subject, origin, true);
	}

	public boolean canCreateDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canCreate(pnfsPath, subject, origin, Boolean.TRUE);
	}
	public boolean canCreateFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canCreate(pnfsPath, subject, origin, Boolean.FALSE);
	}
	public boolean canCreateDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canCreate(pnfsId, subject, origin, Boolean.TRUE);
	}
	public boolean canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canCreate(pnfsId, subject, origin, Boolean.FALSE);
	}

	public boolean canDeleteDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canDelete(pnfsPath, subject, origin, Boolean.TRUE);
	}
	public boolean canDeleteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		return canDelete(pnfsPath, subject, origin, Boolean.FALSE);
	}
	public boolean canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canDelete(pnfsId, subject, origin, Boolean.TRUE);
	}
	public boolean canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		return canDelete(pnfsId, subject, origin, Boolean.FALSE);
	}

	public boolean canListDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canListDir ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

		final FileMetaDataX metadataX = metadataSource.getXMetaData(pnfsPath);
		final FileMetaData metadata = metadataX.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(pnfsPath);

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(metadataX.getPnfsId().toString())),
						Action.READDIR);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}
	public boolean canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canListDir ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId, subject, origin));

		final FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("Directory Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(pnfsId.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(pnfsId.toString())),
						Action.READDIR);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}

	public boolean canGetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
		return canGetSetAttributes(pnfsPath, subject, origin, attribute, true);
	}
	public boolean canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
		return canGetSetAttributes(pnfsPath, subject, origin, attribute, false);
	}

	public boolean canGetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
		return canGetSetAttributes(pnfsId, subject, origin, attribute, true);
	}
	public boolean canSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException, ACLException {
		return canGetSetAttributes(pnfsId, subject, origin, attribute, false);
	}

	public boolean setDefaultPermissions(String pnfsPath) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.setDefaultPermissions ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath));

		String pnfsParentPath = getParentPath(pnfsPath);
		final FileMetaDataX metadataXParent = metadataSource.getXMetaData(pnfsParentPath);
		final FileMetaData metadataParent = metadataXParent.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent File Metadata: " + metadataParent.toString());
		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentPath);

		final FileMetaDataX metadataX = metadataSource.getXMetaData(pnfsPath);
		final FileMetaData metadata = metadataX.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		final String parentID = metadataXParent.getPnfsId().toString();
		boolean res = aclHandler.inheritACL(
				aclHandler.getACL(parentID),
				metadataX.getPnfsId().toString(),
				metadata.isDirectory());

		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- SUCCEED" : "- FAILED"));

		return res;
	}

	public boolean setDefaultPermissions(PnfsId pnfsId) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.setDefaultPermissions ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId));

		PnfsId pnfsParentId = getParentId(pnfsId);
		final FileMetaData metadataParent = metadataSource.getMetaData(pnfsParentId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent Metadata: " + metadataParent.toString());
		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentId.toString());

		final FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		boolean res = aclHandler.inheritACL(aclHandler.getACL(pnfsParentId.toString()), pnfsId.toString(), metadata.isDirectory());
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- SUCCEED" : "- FAILED"));

		return res;
	}

	// Low level checks
	// ///////////////////////////////////////////////////////////////////////////////

	private boolean canReadWriteFile(String pnfsPath, Subject subject, Origin origin, Boolean write) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.can" + (write ? "Write" : "Read") + "File ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

		final FileMetaDataX metadataX = metadataSource.getXMetaData(pnfsPath);
		final FileMetaData metadata = metadataX.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsPath);

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(metadataX.getPnfsId().toString())),
						(write ? Action.WRITE : Action.READ));

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}
	private boolean canReadWriteFile(PnfsId pnfsId, Subject subject, Origin origin, Boolean write) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.can" + (write ? "Write" : "Read") + "File ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId, subject, origin));

		final FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsId.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(pnfsId.toString())),
						(write ? Action.WRITE : Action.READ));

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}

	private boolean canCreate(String pnfsPath, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canCreate" + (isDir ? "Dir " : "File ");
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

		String pnfsParentPath = getParentPath(pnfsPath);
		final FileMetaDataX metadataXParent = metadataSource.getXMetaData(pnfsParentPath);
		final FileMetaData metadataParent = metadataXParent.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent Metadata: " + metadataParent.toString());

		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentPath);

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()),
						aclHandler.getACL(metadataXParent.getPnfsId().toString())),
						Action.CREATE, isDir);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}
	private boolean canCreate(PnfsId pnfsId, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canCreate" + (isDir ? "Dir " : "File ");
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId, subject, origin));

		PnfsId pnfsParentId = getParentId(pnfsId);
		final FileMetaData metadataParent = metadataSource.getMetaData(pnfsParentId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent Metadata: " + metadataParent.toString());

		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentId.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()),
						aclHandler.getACL(pnfsParentId.toString())),
						Action.CREATE, isDir);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}

	private boolean canDelete(String pnfsPath, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canDelete" + (isDir ? "Dir " : "File ");
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath, subject, origin));

		String pnfsParentPath = getParentPath(pnfsPath);
		final FileMetaDataX metadataXParent = metadataSource.getXMetaData(pnfsParentPath);
		final FileMetaData metadataParent = metadataXParent.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent File Metadata: " + metadataParent.toString());
		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentPath);

		final FileMetaDataX metadataX = metadataSource.getXMetaData(pnfsPath);
		final FileMetaData metadata = metadataX.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( isDir ) {
			if ( metadata.isDirectory() == false )
				throw new NotDirCacheException(pnfsPath);

		} else if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsPath);

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()), aclHandler.getACL(metadataXParent.getPnfsId().toString())),
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()), aclHandler.getACL(metadataX.getPnfsId().toString())),
				Action.REMOVE, isDir);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}
	private boolean canDelete(PnfsId pnfsId, Subject subject, Origin origin, Boolean isDir) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.canDelete" + (isDir ? "Dir " : "File ");
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId, subject, origin));

		PnfsId pnfsParentId = getParentId(pnfsId);
		final FileMetaData metadataParent = metadataSource.getMetaData(pnfsParentId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("Parent Metadata: " + metadataParent.toString());
		if ( metadataParent.isDirectory() == false )
			throw new NotDirCacheException(pnfsParentId.toString());

		final FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( isDir ) {
			if ( metadata.isDirectory() == false )
				throw new NotDirCacheException(pnfsId.toString());

		} else if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsId.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadataParent.getUid(), metadataParent.getGid()), aclHandler.getACL(pnfsParentId.toString())),
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()), aclHandler.getACL(pnfsId.toString())),
				Action.REMOVE, isDir);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}

	private boolean canGetSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute, boolean get) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.can" + (get ? "Get" : "Set") + "Attributes ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsPath, subject, origin, attribute));

		final FileMetaDataX metadataX = metadataSource.getXMetaData(pnfsPath);
		final FileMetaData metadata = metadataX.getFileMetaData();
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(metadataX.getPnfsId().toString())),
						(get ? Action.GETATTR : Action.SETATTR), attribute);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}
	private boolean canGetSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute, boolean get) throws CacheException, ACLException {
		final String OPERATION = "ACLPermisionHandler.can" + (get ? "Get" : "Set") + "Attributes ";
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + args2String(pnfsId, subject, origin, attribute));

		final FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		Boolean allowed = AclNFSv4Matcher.isAllowed(
				AclMapper.getPermission(subject, origin, new Owner(metadata.getUid(), metadata.getGid()),
						aclHandler.getACL(pnfsId.toString())),
						(get ? Action.GETATTR : Action.SETATTR), attribute);

		boolean res = allowed != null && allowed.equals(Boolean.TRUE);
		if ( IS_DEBUG_ENABLED )
			logger.debug(OPERATION + (res ? "- ALLOWED" : "- DENIED"));

		return res;
	}

}
