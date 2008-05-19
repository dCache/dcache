/**
 * 
 */
package diskCacheV111.services.acl;

import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

/**
 * @author irinak, mdavid
 * 
 */
public interface PermissionHandlerInterface {

	/**
	 * checks whether the user can read file
	 * 
	 * @param pnfsPath
	 *            File location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can read file, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canReadFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can write file
	 * 
	 * @param pnfsPath
	 *            File location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can write file, otherwise <code>FALSE</code>
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canWriteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can create directory
	 * 
	 * @param pnfsPath
	 *            Directory location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can create directory, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canCreateDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canCreateDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can create file
	 * 
	 * @param pnfsPath
	 *            File location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can create file, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canCreateFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can delete file
	 * 
	 * @param pnfsPath
	 *            File location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can delete file, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canDeleteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can delete directory
	 * 
	 * @param pnfsPath
	 *            File location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can delete directory, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canDeleteDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can list directory
	 * 
	 * @param pnfsPath
	 *            Directory location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can list directory, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canListDir(String pnfsPath, Subject subject, Origin origin) throws CacheException, ACLException;
	public abstract boolean canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 * checks whether the user can set attributes of a file/directory
	 * 
	 * @param pnfsPath
	 *            File/Directory location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can set attributes of a file/directory, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canSetAttributes(String pnfsPath, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;
	public abstract boolean canSetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;

	/**
	 * checks whether the user can get attributes of a file/directory
	 * 
	 * @param pnfsPath
	 *            File/Directory location
	 * @param subject
	 *            identifies the subject that is trying to access a resource
	 * @param origin
	 *            contains information about the origin of a request
	 * 
	 * @return <code>TRUE</code> if user can get attributes of a file/directory, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean canGetAttributes(String pnfsPath, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;
	public abstract boolean canGetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;

	/**
	 * Sets default permissions for new file/directory
	 * 
	 * @param pnfsPath
	 *            Newly created file/directory location
	 *            
	 * @return <code>TRUE</code> if operation succeed, otherwise <code>FALSE</code>
	 * 
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract boolean setDefaultPermissions(String pnfsPath) throws CacheException, ACLException;
	public abstract boolean setDefaultPermissions(PnfsId pnfsId) throws CacheException, ACLException;

}
