// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services.acl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;

import diskCacheV111.services.FileMetaDataSource;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.ConfigurationException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAdapter;

/**
 * UnixPermissionHandler
 *
 * @author tigran, irinak, optimized by mdavid
 *
 */
public class UnixPermissionHandler extends AbstractPermissionHandler {

	private static final Logger logger = Logger.getLogger("logger.org.dcache.authorization." + UnixPermissionHandler.class.getName());
	private static final boolean IS_DEBUG_ENABLED = logger.isDebugEnabled();

	public UnixPermissionHandler(CellAdapter cell) throws ACLException {
		super(cell);
	}

	public boolean canWriteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canWriteFile " + args2String(pnfsPath, subject, origin));

		boolean allowed = false;
		try {
			allowed = fileCanWrite(subject, metadataSource.getMetaData(pnfsPath));
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canWriteFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		} catch (CacheException Ignore) { /* file do not exist, check directory */
		}

		allowed = dirCanWrite(subject, getParentPath(new FsPath(pnfsPath))); // TODO: replace by getParentPath(pnfsPath)
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canWriteFile:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));

		return allowed;
	}
	public boolean canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canWriteFile " + args2String(pnfsId, subject, origin));

		boolean allowed = false;
		try {
			allowed = fileCanWrite(subject, metadataSource.getMetaData(pnfsId));
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canWriteFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		} catch (CacheException Ignore) { /* file do not exist, check directory */
		}

		allowed = dirCanWrite(subject, getParentId(pnfsId));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canWriteFile:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));

		return allowed;
	}

	public boolean canCreateDir(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateDir " + args2String(pnfsPath, subject, origin));

		boolean allowed = dirCanWrite(subject, getParentPath(new FsPath(pnfsPath)));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateDir:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canCreateDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateDir " + args2String(pnfsId, subject, origin));

		boolean allowed = dirCanWrite(subject, getParentId(pnfsId));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateDir:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canDeleteDir(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteDir " + args2String(pnfsPath, subject, origin));

		FileMetaData metadata = metadataSource.getMetaData(pnfsPath);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(pnfsPath);

		boolean allowed = fileCanWrite(subject, metadata);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteDir:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteDir " + args2String(pnfsId, subject, origin));

		FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(pnfsId.toString());

		boolean allowed = fileCanWrite(subject, metadata);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteDir:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canDeleteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteFile " + args2String(pnfsPath, subject, origin));

		boolean allowed = dirCanDelete(subject, getParentPath(new FsPath(pnfsPath)));
		if ( allowed == false ) {
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canDeleteFile:dirCanDelete - DENIED");
			return false;
		}

		FileMetaData metadata = metadataSource.getMetaData(pnfsPath);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		allowed = fileCanWrite(subject, metadata);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteFile " + args2String(pnfsId, subject, origin));

		boolean allowed = dirCanDelete(subject, getParentId(pnfsId));
		if ( allowed == false ) {
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canDeleteFile:dirCanDelete - DENIED");
			return false;
		}

		FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		allowed = fileCanWrite(subject, metadata);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canDeleteFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canReadFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canReadFile " + args2String(pnfsPath, subject, origin));

		FileMetaData metadata = metadataSource.getMetaData(pnfsPath);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsPath);

		boolean allowed = fileCanRead(subject, metadata);
		if ( allowed == false ) {
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canReadFile:fileCanRead - DENIED");
			return false;
		}

		allowed = dirCanRead(subject, getParentPath(new FsPath(pnfsPath)));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canReadFile:dirCanRead - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canReadFile " + args2String(pnfsId, subject, origin));

		FileMetaData metadata = metadataSource.getMetaData(pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isRegularFile() == false )
			throw new NotFileCacheException(pnfsId.toString());

		boolean allowed = fileCanRead(subject, metadata);
		if ( allowed == false ) {
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canReadFile:fileCanRead - DENIED");
			return false;
		}

		allowed = dirCanRead(subject, getParentId(pnfsId));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canReadFile:dirCanRead - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canCreateFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateFile " + args2String(pnfsPath, subject, origin));

		boolean allowed = false;
		try {
			allowed = fileCanWrite(subject, metadataSource.getMetaData(pnfsPath));
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canCreateFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		} catch (CacheException Ignore) { /* file do not exist, check directory */
		}

		allowed = dirCanWrite(subject, getParentPath(new FsPath(pnfsPath)));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateFile:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateFile " + args2String(pnfsId, subject, origin));

		boolean allowed = false;
		try {
			allowed = fileCanWrite(subject, metadataSource.getMetaData(pnfsId));
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canCreateFile:fileCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		} catch (CacheException Ignore) { /* file do not exist, check directory */
		}

		allowed = dirCanWrite(subject, getParentId(pnfsId));
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canCreateFile:dirCanWrite - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canListDir(String pnfsPath, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canListDir " + args2String(pnfsPath, subject, origin));

		boolean allowed = dirCanRead(subject, pnfsPath);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canListDir:dirCanRead - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canListDir " + args2String(pnfsId, subject, origin));

		boolean allowed = dirCanRead(subject, pnfsId);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canListDir:dirCanRead - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canSetAttributes " + args2String(pnfsPath, subject, origin, attribute));

		boolean allowed = false;
		if ( attribute == FileAttribute.FATTR4_OWNER || attribute == FileAttribute.FATTR4_OWNER_GROUP ) {
			allowed = (metadataSource.getMetaData(pnfsPath).getUid() == subject.getUid() || subject.getUid() == 0);
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canSetAttributes:Owner - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		}

		allowed = canWriteFile(pnfsPath, subject, origin);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canSetAttributes:canWriteFile - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}
	public boolean canSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canSetAttributes " + args2String(pnfsId, subject, origin, attribute));

		boolean allowed = false;
		if ( attribute == FileAttribute.FATTR4_OWNER || attribute == FileAttribute.FATTR4_OWNER_GROUP ) {
			allowed = (metadataSource.getMetaData(pnfsId).getUid() == subject.getUid() || subject.getUid() == 0);
			if ( IS_DEBUG_ENABLED )
				logger.debug("UnixPermisionHandler.canSetAttributes:Owner - " + (allowed ? "ALLOWED" : "DENIED"));
			return allowed;
		}

		allowed = canWriteFile(pnfsId, subject, origin);
		if ( IS_DEBUG_ENABLED )
			logger.debug("UnixPermisionHandler.canSetAttributes:canWriteFile - " + (allowed ? "ALLOWED" : "DENIED"));
		return allowed;
	}

	public boolean canGetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
		if ( IS_DEBUG_ENABLED ) {
			logger.debug("UnixPermisionHandler.canGetAttributes " + args2String(pnfsPath, subject, origin, attribute));
			logger.debug("UnixPermisionHandler.canGetAttributes - ALLOWED");
		}
		return true;
	}
	public boolean canGetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
		if ( IS_DEBUG_ENABLED ) {
			logger.debug("UnixPermisionHandler.canGetAttributes " + args2String(pnfsId, subject, origin, attribute));
			logger.debug("UnixPermisionHandler.canGetAttributes - ALLOWED");
		}
		return true;
	}

	public boolean setDefaultPermissions(String pnfsPath) throws CacheException, ACLException {
		return true;
	}
	public boolean setDefaultPermissions(PnfsId pnfsId) throws CacheException, ACLException {
		return true;
	}

	/**
	 * private method(s)
	 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

//	private boolean dirCanRead(Subject subject, String path) throws CacheException {
//		FileMetaData metadata = metadataSource.getMetaData(path);
//		if ( IS_DEBUG_ENABLED )
//			logger.debug("File Metadata: " + metadata.toString());
//
//		if ( metadata.isDirectory() == false )
//			throw new NotDirCacheException(path);
//
//		if ( fileCanRead(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
//			return false;
//
//		return true;
//	}
	private boolean dirCanRead(Subject subject, Object object) throws CacheException {
		FileMetaData metadata = null;
		if (object instanceof String)
			metadata = metadataSource.getMetaData((String)object);
		else if (object instanceof PnfsId)
			metadata = metadataSource.getMetaData((PnfsId)object);
		else
			throw new IllegalArgumentException("Invalid type: " + object.getClass().getName());

		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(object.toString());

		if ( fileCanRead(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
			return false;

		return true;
	}

//	private boolean dirCanWrite(Subject subject, String path) throws CacheException {
//		FileMetaData metadata = metadataSource.getMetaData(path);
//		if ( IS_DEBUG_ENABLED )
//			logger.debug("File Metadata: " + metadata.toString());
//
//		if ( metadata.isDirectory() == false )
//			throw new NotDirCacheException(path);
//
//		if ( fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
//			return false;
//
//		return true;
//	}
	private boolean dirCanWrite(Subject subject, Object object) throws CacheException {
		FileMetaData metadata = null;
		if (object instanceof String)
			metadata = metadataSource.getMetaData((String)object);
		else if (object instanceof PnfsId)
			metadata = metadataSource.getMetaData((PnfsId)object);
		else
			throw new IllegalArgumentException("Invalid type: " + object.getClass().getName());

		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(object.toString());

		if ( fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
			return false;

		return true;
	}

//	private boolean dirCanDelete(Subject subject, String path) throws CacheException {
//		FileMetaData metadata = metadataSource.getMetaData(path);
//		if ( IS_DEBUG_ENABLED )
//			logger.debug("File Metadata: " + metadata.toString());
//
//		if ( metadata.isDirectory() == false )
//			throw new NotDirCacheException(path);
//
//		if ( fileCanRead(subject, metadata) == false || fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
//			return false;
//
//		return true;
//	}
	private boolean dirCanDelete(Subject subject, Object object) throws CacheException {
		FileMetaData metadata = null;
		if (object instanceof String)
			metadata = metadataSource.getMetaData((String)object);
		else if (object instanceof PnfsId)
			metadata = metadataSource.getMetaData((PnfsId)object);
		else
			throw new IllegalArgumentException("Invalid type: " + object.getClass().getName());

		if ( IS_DEBUG_ENABLED )
			logger.debug("File Metadata: " + metadata.toString());

		if ( metadata.isDirectory() == false )
			throw new NotDirCacheException(object.toString());

		if ( fileCanRead(subject, metadata) == false || fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
			return false;

		return true;
	}


	// Low level checks
	// ///////////////////////////////////////////////////////////////////////////////

	private boolean fileCanWrite(Subject subject, FileMetaData metadata) {
		if ( metadata.getUid() == subject.getUid() )
			return metadata.getUserPermissions().canWrite();

		if ( metadata.getGid() == subject.getGid() )
			return metadata.getGroupPermissions().canWrite();

		return metadata.getWorldPermissions().canWrite();
	}

	private boolean fileCanExecute(Subject subject, FileMetaData metadata) {
		if ( metadata.getUid() == subject.getUid() )
			return metadata.getUserPermissions().canExecute();

		if ( metadata.getGid() == subject.getGid() )
			return metadata.getGroupPermissions().canExecute();

		return metadata.getWorldPermissions().canExecute();
	}

	private boolean fileCanRead(Subject subject, FileMetaData metadata) {
		if ( metadata.getUid() == subject.getUid() )
			return metadata.getUserPermissions().canRead();

		if ( metadata.getGid() == subject.getGid() )
			return metadata.getGroupPermissions().canRead();

		return metadata.getWorldPermissions().canRead();
	}

}
