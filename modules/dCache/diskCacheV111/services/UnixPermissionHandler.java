// $Id: UnixPermissionHandler.java,v 1.2 2007-10-23 15:24:02 tigran Exp $

package diskCacheV111.services;

import org.apache.log4j.Logger;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CellAdapter;

import org.dcache.chimera.acl.Origin;
import org.dcache.chimera.acl.Subject;
import org.dcache.chimera.acl.enums.FileAttribute;

public class UnixPermissionHandler implements PermissionHandlerInterface {

    private CellAdapter _cell;
    private final FileMetaDataSource _metaDataSource;

    private final static Logger _logPermisions = Logger
            .getLogger("logger.org.dcache.authorization."
                    + UnixPermissionHandler.class.getName());

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#say(java.lang.String)
     */
    public UnixPermissionHandler(CellAdapter cell,
            FileMetaDataSource metaDataSource) {
        _cell = cell;
        _metaDataSource = metaDataSource;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canWrite(int, int,
     *      java.lang.String)
     */

    public boolean canWriteFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        _logPermisions.debug("canWrite(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");

        try {
            return fileCanWrite(subject, _metaDataSource.getMetaData(pnfsPath));
        } catch (CacheException ce) {
            // file do not exist, check directory
        }

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

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);

        return parentWriteAllowed && parentExecuteAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canCreateDir(int,
     *      int, java.lang.String)
     */
    public boolean canCreateDir(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        _logPermisions.debug("canCreateDir(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");
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

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);

        return parentWriteAllowed && parentExecuteAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canDeleteDir(int,
     *      int, java.lang.String)
     */
    public boolean canDeleteDir(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        _logPermisions.debug("canDeleteDir(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath + " is not a directory");
            throw new CacheException("path is not a directory");
        }

        return fileCanWrite(subject, meta);
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canDelete(int,
     *      int, java.lang.String)
     */
    public boolean canDeleteFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {
        _logPermisions.debug("canDelete(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _metaDataSource.getMetaData(parent);
        _logPermisions.debug("canWrite() parent meta = " + meta);
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not read "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);
        boolean parentReadAllowed = fileCanRead(subject, meta);

        _logPermisions.debug("canDelete() parent read allowed :"
                + parentReadAllowed + " parent write allowed :"
                + parentReadAllowed + " parent exec allowed :"
                + parentExecuteAllowed);

        if (!parentReadAllowed || !parentExecuteAllowed || !parentWriteAllowed) {
            _logPermisions.error(" parent write is not allowed ");
            return false;
        }

        meta = _metaDataSource.getMetaData(pnfsPath);
        _logPermisions.debug("canDelete() file meta = " + meta);

        boolean deleteAllowed = fileCanWrite(subject, meta);

        if (deleteAllowed) {
            _logPermisions.error("WARNING: canDelete() delete of file "
                    + pnfsPath + " by user uid=" + subject.getUid() + " gid="
                    + subject.getGid() + " is allowed!");
        } else {
            _logPermisions.debug("canDelete() delete of file " + pnfsPath
                    + " by user uid=" + subject.getUid() + " gid="
                    + subject.getGid() + " is not allowed");
        }
        return deleteAllowed;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.services.PermissionHandlerInterface#canRead(int, int,
     *      java.lang.String)
     */
    public boolean canReadFile(Subject subject, String pnfsPath,
            Origin userOrigin) throws CacheException {

        _logPermisions.debug("canRead(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");

        if (!fileCanRead(subject, _metaDataSource.getMetaData(pnfsPath))) {
            return false;
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");
        String parent = parent_path.toString();

        return dirCanRead(subject, parent);
    }

    private boolean dirCanRead(Subject subject, String path) throws CacheException {

        FileMetaData meta = _metaDataSource.getMetaData(path);
        _logPermisions.debug("dirCanRead() meta = " + meta);
        if (!meta.isDirectory()) {
            _logPermisions.error(path
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(subject, meta);
        ;
        boolean executeAllowed = fileCanExecute(subject, meta);
        ;

        _logPermisions.debug("dirCanRead() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed);

        if (!(readAllowed && executeAllowed)) {
            _logPermisions.error(" read is not allowed ");
            return false;
        }

        return readAllowed && executeAllowed;
    }

    public boolean canCreateFile(Subject subject, String pnfsPath, Origin origin) throws CacheException {

        _logPermisions.debug("canCreateFile(" + subject.getUid() + ","
                + subject.getGid() + "," + pnfsPath + ")");

        try {
            return fileCanWrite(subject, _metaDataSource.getMetaData(pnfsPath));
        } catch (CacheException ce) {
            // file do not exist, check directory
        }

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

        boolean parentWriteAllowed = fileCanWrite(subject, meta);
        boolean parentExecuteAllowed = fileCanExecute(subject, meta);

        return parentWriteAllowed && parentExecuteAllowed;
    }

    public boolean canListDir(Subject subject, String pnfsPath, Origin origin) throws CacheException {

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        _logPermisions.debug("pnfsPath() meta = " + meta);
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(subject, meta);
        ;
        boolean executeAllowed = fileCanExecute(subject, meta);
        ;

        _logPermisions.debug("canListDir() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed);

        if (!(readAllowed && executeAllowed)) {
            _logPermisions.error(" readdir is not allowed ");
            return false;
        }

        return readAllowed && executeAllowed;
    }

    public boolean canSetAttributes(Subject subject, String pnfsPath,
            Origin userOrigin, FileAttribute attribute) throws CacheException {
        // TODO Auto-generated method stub
        return false;
    }

    // ////////////////////////////////////////////////////////////////////////////////
    // /
    // / Low level checks
    // /
    // ///////////////////////////////////////////////////////////////////////////////

    private static boolean fileCanWrite(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            writeAllowed = user.canWrite();
        } else if (meta.getGid() == subject.getGids()[0]) {
            writeAllowed = group.canWrite();
        } else {
            // world = all except user and group
            writeAllowed = world.canWrite();
        }

        return writeAllowed;
    }

    private static boolean fileCanExecute(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            writeAllowed = user.canExecute();
        } else if (meta.getGid() == subject.getGids()[0]) {
            writeAllowed = group.canExecute();
        } else {
            // world = all except user and group
            writeAllowed = world.canExecute();
        }

        return writeAllowed;
    }

    private static boolean fileCanRead(Subject subject, FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean readAllowed = false;

        if (meta.getUid() == subject.getUid()) {
            readAllowed = user.canRead();
        } else if (meta.getGid() == subject.getGids()[0]) {
            readAllowed = group.canRead();
        } else {
            // world = all except user and group
            readAllowed = world.canRead();
        }

        return readAllowed;
    }

}
