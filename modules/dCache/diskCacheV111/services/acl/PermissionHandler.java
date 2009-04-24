package diskCacheV111.services.acl;

import org.dcache.acl.ACLException;
import org.dcache.acl.Origin;
import org.dcache.acl.Subject;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

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
    * checks whether the user can create directory
    *
    * @param pnfsId
    *            Parent directory PNFS id
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
    public abstract AccessType canCreateDir(PnfsId parentPnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
    * checks whether the user can create file
    *
    * @param pnfsId
    *            File PNFS id
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
    public abstract AccessType canCreateFile(PnfsId parentPnfsId, Subject subject, Origin origin) throws CacheException, ACLException;

    /**
    * checks whether the user can delete file
    *
    * @param pnfsId
    *            File PNFS id
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
    * checks whether the user can delete directory
    *
    * @param pnfsId
    *            File PNFS id
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
    * checks whether the user can list directory
    *
    * @param pnfsId
    *            Directory PNFS id
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
    * checks whether the user can set attributes of a file/directory
    *
    * @param pnfsId
    *            File/Directory PNFS id
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
    * checks whether the user can get attributes of a file/directory
    *
    * @param pnfsId
    *            File/Directory PNFS id
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
