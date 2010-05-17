// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services.acl;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.acl.ACLException;
import org.dcache.acl.enums.AccessType;
import org.dcache.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import diskCacheV111.util.NotDirCacheException;
import diskCacheV111.util.NotFileCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellEndpoint;

import javax.security.auth.Subject;
import org.dcache.auth.Origin;
import org.dcache.auth.Subjects;

/**
 * UnixPermissionHandler
 *
 * @author tigran, irinak, optimized by mdavid
 *
 */
public class UnixPermissionHandler extends AbstractPermissionHandler {

    private static final Logger _logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + UnixPermissionHandler.class.getName());

    public UnixPermissionHandler(CellEndpoint cell) throws ACLException {
        super(cell);
    }

    public String toUnixACL(PnfsId pnfsId) throws ACLException, CacheException {
        return _metadataSource.getMetaData(pnfsId).getPermissionString();
        }

    public AccessType canWriteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canWriteFile " + args2String(pnfsId, subject, origin));

        AccessType res;
        try {
            res = fileCanWrite(subject, _metadataSource.getMetaData(pnfsId)) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canWriteFile:fileCanWrite - " + res);
            return res;

        } catch (CacheException Ignore) {
            // file do not exist, check directory
        }

        res = dirCanWrite(getParentId(pnfsId), subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canWriteFile:dirCanWrite - " + res);

        return res;
    }

   /**
    * checks whether the user can write file (pnfsPath)
    */
    public AccessType canWriteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canWriteFile " + args2String(pnfsPath, subject, origin));

        AccessType res;
        try {
            res = fileCanWrite(subject, _metadataSource.getMetaData(pnfsPath)) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canWriteFile:fileCanWrite - " + res);
            return res;

        } catch (CacheException Ignore) {
            // file do not exist, check directory
        }

        FsPath parent_path = new FsPath(pnfsPath);
        parent_path.add("..");
        String parentPnfsPath = parent_path.toString();

        res = dirCanWrite(parentPnfsPath, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canWriteFile:dirCanWrite - " + res);

        return res;
    }

    public AccessType canCreateDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateDir " + args2String(pnfsId, subject, origin));

        AccessType res = dirCanWrite(pnfsId, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateDir:dirCanWrite - " + res);
        return res;
    }

   /**
    * checks whether the user can create a sub-directory
    * in this directory (given by its pnfsPath, like /pnfs/sample.com/data/directory)
    */
    public AccessType canCreateDir(String pnfsPath, Subject subject,
            Origin userOrigin) throws CacheException {

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _metadataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logger.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return AccessType.ACCESS_DENIED;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);
        AccessType isAllowed = (parentWriteAllowed && parentExecuteAllowed) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;

        if( _logger.isDebugEnabled() )
            _logger.debug("canCreateDir(" + Arrays.toString(Subjects.getUids(subject)) + ","
                + Arrays.toString(Subjects.getGids(subject)) + "," + pnfsPath + "): " + isAllowed);

        return isAllowed;
    }

    public AccessType canDeleteDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteDir " + args2String(pnfsId, subject, origin));

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsId.toString());

        AccessType res = fileCanWrite(subject, metadata) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteDir:fileCanWrite - " + res);
        return res;
    }

    /**
     * checks whether the user can delete directory given by its pnfsPath
     */
    public AccessType canDeleteDir(String pnfsPath, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteDir " + args2String(pnfsPath, subject, origin));

        FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsPath);

        AccessType res = fileCanWrite(subject, metadata) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteDir:fileCanWrite - " + res);
        return res;
    }

    public AccessType canDeleteFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteFile " + args2String(pnfsId, subject, origin));

        AccessType res;
        if (dirCanDelete(getParentId(pnfsId), subject) == false) {
            res = AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canDeleteFile:dirCanDelete - " + res);
            return res;
        }

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        res = fileCanWrite(subject, metadata) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteFile:fileCanWrite - " + res);
        return res;
    }

   /**
    * checks whether the user can delete file given by its pnfsPath
    */
    public AccessType canDeleteFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteFile " + args2String(pnfsPath, subject, origin));

        AccessType res;
        FsPath parent_path = new FsPath(pnfsPath);
        parent_path.add("..");
        String parentPnfsPath = parent_path.toString();

        if (dirCanDelete(parentPnfsPath, subject) == false) {
            res = AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canDeleteFile:dirCanDelete - " + res);
            return res;
        }

        FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        res = fileCanWrite(subject, metadata) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canDeleteFile:fileCanWrite - " + res);
        return res;
    }

    public AccessType canReadFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canReadFile " + args2String(pnfsId, subject, origin));

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isRegularFile() == false )
            throw new NotFileCacheException(pnfsId.toString());

        AccessType res;
        if (fileCanRead(subject, metadata) == false) {
            res = AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canReadFile:fileCanRead - " + res);
            return res;
        }

        PnfsId parentPnfsId = getParentId(pnfsId);

        res = dirCanRead(parentPnfsId, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canReadFile:dirCanRead - " + res);
        return res;
        }

   /**
    * checks whether the user can read file given by its pnfsPath
    */
    public AccessType canReadFile(String pnfsPath, Subject subject,
            Origin userOrigin) throws CacheException {

        AccessType res;
        FileMetaData meta = _metadataSource.getMetaData(pnfsPath);

        if( !meta.isRegularFile() ) {
            throw new NotFileCacheException(pnfsPath + " exists and not a regular file.");
        }

        if (fileCanRead(subject, meta) == false) {
            res = AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canReadFile:fileCanRead - " + res);
            return res;
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");
        String parent = parent_path.toString();

        res = dirCanRead(subject, parent)? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canReadFile:dirCanRead - " + res);
        return res;

    }

    public AccessType canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateFile " + args2String(pnfsId, subject, origin));

        AccessType res = fileCanWrite(subject, _metadataSource.getMetaData(pnfsId)) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canCreateFile:fileCanWrite - " + res);
            return res;
    }

   /**
    * checks whether the user can create a file
    * in this directory (given by its pnfsPath, like /pnfs/sample.com/data/directory)
    */
    public AccessType canCreateFile(String pnfsPath, Subject subject, Origin origin) throws CacheException {

        try {
            return fileCanWrite(subject, _metadataSource.getMetaData(pnfsPath)) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        } catch (CacheException ce) {
            // file do not exist, check directory
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _metadataSource.getMetaData(parent);
        if (!meta.isDirectory()) {
            _logger.error(parent
                    + " exists and is not a directory, can not create "
                    + pnfsPath);
            return AccessType.ACCESS_DENIED;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);

        AccessType isAllowed = (parentWriteAllowed && parentExecuteAllowed) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;

        if( _logger.isDebugEnabled() )
            _logger.debug("canCreateFile(" + Arrays.toString(Subjects.getUids(subject)) + ","
                + Arrays.toString(Subjects.getGids(subject)) + "," + pnfsPath + ") :" + isAllowed);

        return isAllowed;
    }

    public AccessType canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canListDir " + args2String(pnfsId, subject, origin));

        AccessType res = dirCanRead(pnfsId, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canListDir:dirCanRead - " + res);
        return res;
    }

   /**
    * checks whether the user can list this directory (given by its pnfsPath,
    * like /pnfs/sample.com/data/directory)
    */
    public AccessType canListDir(String pnfsPath, Subject subject, Origin origin) throws CacheException {

        FileMetaData meta = _metadataSource.getMetaData(pnfsPath);
        _logger.debug("pnfsPath() meta = " + meta);

        if (!meta.isDirectory()) {
            _logger.error(pnfsPath
                    + " exists and is not a directory, can not read ");
            return AccessType.ACCESS_DENIED;
        }

        boolean readAllowed = fileCanRead(subject, meta);

        boolean executeAllowed = fileCanExecute(subject, meta);

        if (!(readAllowed && executeAllowed)) {
            _logger.error(" readdir is not allowed ");
            return AccessType.ACCESS_DENIED;
        }

        AccessType isAllowed=(readAllowed && executeAllowed) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;

        _logger.debug("canListDir() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed + ". List directory: "+ isAllowed );

        return isAllowed;
    }

    public AccessType canSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canSetAttributes " + args2String(pnfsId, subject, origin, attribute));

        AccessType res;
        if (attribute == FileAttribute.FATTR4_OWNER ||
                attribute == FileAttribute.FATTR4_OWNER_GROUP ||
                attribute == FileAttribute.FATTR4_MODE) { // mdavid: only owner or root can chgrp/chown/chmod

            res = ( Subjects.hasUid(subject, _metadataSource.getMetaData(pnfsId).getUid()) || Subjects.isRoot(subject)) ? /**/
            AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;

            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canSetAttributes:Owner - " + res);
            return res;
        }

        res = canWriteFile(pnfsId, subject, origin);
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canSetAttributes:canWriteFile - " + res);
        return res;
    }

   //canSetAttributes using String pnfsPath (instead of PnfsId pnfsId)
    public AccessType canSetAttributes(String pnfsPath, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canSetAttributes " + args2String(pnfsPath, subject, origin, attribute));

        AccessType res;
        if (attribute == FileAttribute.FATTR4_OWNER ||
                attribute == FileAttribute.FATTR4_OWNER_GROUP ||
                attribute == FileAttribute.FATTR4_MODE) { // mdavid: only owner or root can chgrp/chown/chmod

            res = ( Subjects.hasUid(subject,_metadataSource.getMetaData(pnfsPath).getUid()) || Subjects.isRoot(subject)) ? /**/
            AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;

            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canSetAttributes:Owner - " + res);
            return res;
        }

        res = canWriteFile(pnfsPath, subject, origin);
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canSetAttributes:canWriteFile - " + res);
        return res;
    }

    public AccessType canGetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
        if ( _logger.isDebugEnabled() ) {
            _logger.debug("UnixPermissionHandler.canGetAttributes " + args2String(pnfsId, subject, origin, attribute));
            _logger.debug("UnixPermissionHandler.canGetAttributes - ALLOWED");
        }
        return AccessType.ACCESS_ALLOWED;
    }

    /**
    * private method(s) * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
    */

    private boolean dirCanRead(PnfsId pnfsId, Subject subject) throws CacheException {
        if (pnfsId == null)
            throw new IllegalArgumentException("pnfsId is null");

        if (subject == null)
            throw new IllegalArgumentException("subject is null");

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsId.toString());

        if ( fileCanRead(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
            return false;

        return true;
    }

    //the following method is used in canReadFile(String pnfsPath, ...)
    private boolean dirCanRead(Subject subject, String path) throws CacheException {

        boolean isAllowed = false;
        FileMetaData meta = _metadataSource.getMetaData(path);
        _logger.debug("dirCanRead() meta = " + meta);
        if (!meta.isDirectory()) {
            _logger.error(path
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(subject, meta);

        boolean executeAllowed = fileCanExecute(subject, meta);

        if (!(readAllowed && executeAllowed)) {
            _logger.error(" read is not allowed ");
            return false;
        }

        isAllowed = readAllowed && executeAllowed;

        _logger.debug("dirCanRead() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed + ", dirCanRead: " + isAllowed);

        return isAllowed;
    }

    private boolean dirCanWrite(PnfsId pnfsId, Subject subject) throws CacheException {
        if (pnfsId == null)
            throw new IllegalArgumentException("pnfsId is null");

        if (subject == null)
            throw new IllegalArgumentException("subject is null");

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsId.toString());

        if ( fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
            return false;

        return true;
    }

    //dirCanWrite using String pnfsPath (instead of pnfsId). call from canWriteFile(String ..)
    private boolean dirCanWrite(String pnfsPath, Subject subject) throws CacheException {
        if (pnfsPath == null)
            throw new IllegalArgumentException("pnfsPath is null");

        if (subject == null)
            throw new IllegalArgumentException("subject is null");

        FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsPath);

        if ( fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
            return false;

        return true;
    }

    private boolean dirCanDelete(PnfsId pnfsId, Subject subject) throws CacheException {
        if (pnfsId == null)
            throw new IllegalArgumentException("pnfsId is null");

        if (subject == null)
            throw new IllegalArgumentException("subject is null");

        FileMetaData metadata = _metadataSource.getMetaData(pnfsId);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsId.toString());

        if ( fileCanRead(subject, metadata) == false || fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
            return false;

        return true;
    }

    //dirCanDelete using String pnfsPath (instead of pnfsId). call from canDeleteFile(String pnfsPath,..)
    private boolean dirCanDelete(String pnfsPath, Subject subject) throws CacheException {
        if (pnfsPath == null)
            throw new IllegalArgumentException("pnfsPath is null");

        if (subject == null)
            throw new IllegalArgumentException("subject is null");

        FileMetaData metadata = _metadataSource.getMetaData(pnfsPath);
        if ( _logger.isDebugEnabled() )
            _logger.debug("File Metadata: " + metadata.toString());

        if ( metadata.isDirectory() == false )
            throw new NotDirCacheException(pnfsPath);

        if ( fileCanRead(subject, metadata) == false || fileCanWrite(subject, metadata) == false || fileCanExecute(subject, metadata) == false )
            return false;

        return true;
    }

    // Low level checks
    // ///////////////////////////////////////////////////////////////////////////////

    private boolean fileCanWrite(Subject subject, FileMetaData metadata) {
        if ( Subjects.hasUid(subject, metadata.getUid()) )
            return metadata.getUserPermissions().canWrite();

        if ( Subjects.hasGid(subject, metadata.getGid()) )
            return  metadata.getGroupPermissions().canWrite();

        return metadata.getWorldPermissions().canWrite();
    }

    private boolean fileCanExecute(Subject subject, FileMetaData metadata) {
        if ( Subjects.hasUid(subject, metadata.getUid()) )
            return metadata.getUserPermissions().canExecute();

        if ( Subjects.hasGid(subject, metadata.getGid()) )
            return  metadata.getGroupPermissions().canExecute();

        return metadata.getWorldPermissions().canExecute();
    }

    private boolean fileCanRead(Subject subject, FileMetaData metadata) {
        if ( Subjects.hasUid(subject, metadata.getUid()) )
            return metadata.getUserPermissions().canRead();

        if ( Subjects.hasGid(subject, metadata.getGid()) )
            return  metadata.getGroupPermissions().canRead();

        return metadata.getWorldPermissions().canRead();
    }

}
