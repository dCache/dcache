// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services.acl;

import org.apache.log4j.Logger;
import org.dcache.chimera.acl.ACLException;
import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.AccessType;
import org.dcache.chimera.acl.enums.FileAttribute;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
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

    private static final Logger _logger = Logger.getLogger("logger.org.dcache.authorization." + UnixPermissionHandler.class.getName());

    public UnixPermissionHandler(CellAdapter cell) throws ACLException {
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

    public AccessType canCreateDir(PnfsId pnfsParentId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateDir " + args2String(pnfsParentId, subject, origin));

        AccessType res = dirCanWrite(pnfsParentId, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateDir:dirCanWrite - " + res);
        return res;
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

    public AccessType canCreateFile(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateFile " + args2String(pnfsId, subject, origin));

        AccessType res;
        try {
            res = fileCanWrite(subject, _metadataSource.getMetaData(pnfsId)) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
            if ( _logger.isDebugEnabled() )
                _logger.debug("UnixPermissionHandler.canCreateFile:fileCanWrite - " + res);
            return res;

        } catch (CacheException Ignore) {
            // file do not exist, check directory
        }

        res = dirCanWrite(getParentId(pnfsId), subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canCreateFile:dirCanWrite - " + res);
        return res;
    }

    public AccessType canListDir(PnfsId pnfsId, Subject subject, Origin origin) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canListDir " + args2String(pnfsId, subject, origin));

        AccessType res = dirCanRead(pnfsId, subject) ? AccessType.ACCESS_ALLOWED : AccessType.ACCESS_DENIED;
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canListDir:dirCanRead - " + res);
        return res;
    }

    public AccessType canSetAttributes(PnfsId pnfsId, Subject subject, Origin origin, FileAttribute attribute) throws CacheException {
        if ( _logger.isDebugEnabled() )
            _logger.debug("UnixPermissionHandler.canSetAttributes " + args2String(pnfsId, subject, origin, attribute));

        AccessType res;
        if (attribute == FileAttribute.FATTR4_OWNER ||
                attribute == FileAttribute.FATTR4_OWNER_GROUP ||
                attribute == FileAttribute.FATTR4_MODE) { // mdavid: only owner or root can chgrp/chown/chmod

            res = (_metadataSource.getMetaData(pnfsId).getUid() == subject.getUid() || subject.getUid() == 0) ? /**/
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
