package diskCacheV111.services.acl;

import org.dcache.acl.ACLException;
import org.dcache.acl.Origin;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import javax.security.auth.Subject;

/**
 * This interface describes permissions for access-operations performed on resources.
 *
 * @author Davin Melkumyan, Irina Kozlova
 *
 */
public interface PermissionHandler {

    /**
    * checks whether the user can read file
    *
    * @param pnfsId
    *            File PNFS id
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
    public abstract AccessType canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

	/**
	 *
	 * @param pnfsPath
	 * @param subject
	 * @param origin
	 * @return true if user allowed to read the file
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract AccessType canReadFile(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

    /**
    * checks whether the user can write file
    *
    * @param pnfsId
    *            File PNFS id
    * @param subject
    *            identifies the subject that is trying to access a resource
    * @param origin
    *            contains information about the origin of a request
    *
    * @return <code>TRUE</code> if user can write file, otherwise <code>FALSE</code>
    * @throws CacheException
    * @throws ACLException
    */
    public abstract AccessType canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
     * @param pnfsPath
     * @param subject
     * @param origin
     * @return true if user allowed to write into the file
     * @throws CacheException
     * @throws ACLException
     */
	public abstract AccessType canWriteFile(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

    /**
    * checks whether the user can create SUB-DIRECTORY in the directory with PnfsId pnfsId
    *
    * @param pnfsId
    *            PnfsId of the directory (we check permission to create SUB-DIRECTORY in this directory)
    * @param subject
    *            identifies the subject that is trying to access a resource
    * @param origin
    *            contains information about the origin of a request
    *
    * @return <code>TRUE</code> if user can create (sub-)directory, otherwise <code>FALSE</code>
    *
    * @throws CacheException
    * @throws ACLException
    */
    public abstract AccessType canCreateDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;


    /**
	 *
	 * @param pnfsPath
	 * @param subject
	 * @param userOrigin
	 * @return true if user allowed to create a directory
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract AccessType canCreateDir(String pnfsPath, Subject subject, Origin origin)
	        throws CacheException, ACLException;


    /**
    * checks whether the user can create FILE in the directory with PnfsId pnfsId
    *
    * @param pnfsId
    *            PnfsId of the directory (we check permission to create FILE in this directory)
    * @param subject
    *            identifies the subject that is trying to access a resource
    * @param origin
    *            contains information about the origin of a request
    *
    * @return <code>TRUE</code> if user can create file in this directory, otherwise <code>FALSE</code>
    *
    * @throws CacheException
    * @throws ACLException
    */
    public abstract AccessType canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
	 *
	 * @param pnfsPath
	 * @param subject
	 * @param origin
	 * @return true if user allowed to create a file in the given directory
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract AccessType canCreateFile(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

	/**
    * checks whether the user can delete file
    *
    * @param pnfsId
    *            PnfsId of the file to be deleted
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
    public abstract AccessType canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
     * @param pnfsPath
     * @param subject
     * @param origin
     * @return true if user allowed to delete this file (pnfsPath)
     * @throws CacheException
     * @throws ACLException
     */
	public abstract AccessType canDeleteFile(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

	/**
    * checks whether the user can delete directory
    *
    * @param pnfsId
    *            PnfsId of the directory to be deleted
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
    public abstract AccessType canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
     * @param pnfsPath
     * @param subject
     * @param origin
     * @return true if user allowed to delete this directory (pnfsPath)
     * @throws CacheException
     * @throws ACLException
     */
	public abstract AccessType canDeleteDir(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

    /**
    * checks whether the user can list directory
    *
    * @param pnfsId
    *            PnfsId of the directory to be listed
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
    public abstract AccessType canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
	 *
     * @param pnfsPath
	 * @param subject
	 * @param origin
	 * @return true if user allowed to list the directory
	 * @throws CacheException
	 * @throws ACLException
	 */
	public abstract AccessType canListDir(String pnfsPath, Subject subject, Origin origin)
			throws CacheException, ACLException;

    /**
    * checks whether the user can set attributes of a file/directory
    *
    * @param pnfsId
    *            PnfsId of File/Directory
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
    public abstract AccessType canSetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;

	/**
	 *
	 * @param pnfsPath
	 * @param subject
	 * @param origin
	 * @param attribute
	 * @return true if user allowed to set attributes
	 * @throws CacheException
	 */
	public abstract AccessType canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute)
	       throws CacheException, ACLException;

    /**
    * checks whether the user can get attributes of a file/directory
    *
    * @param pnfsId
    *            PnfsId of file/directory
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
    public abstract AccessType canGetAttributes(PnfsId pnfsId, Subject subject, Origin userOrigin, FileAttribute attribute) throws CacheException, ACLException;

    /**
    * TODO: draft version - unused
    *
    * Returns Unix file mode as String
    *
    * @param pnfsId
    *            File/Directory PNFS id
    *
    * @throws ACLException
    * @throws CacheException
    */
    public abstract String toUnixACL(PnfsId pnfsId) throws ACLException, CacheException;

}
