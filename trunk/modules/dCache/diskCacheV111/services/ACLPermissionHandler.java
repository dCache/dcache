package diskCacheV111.services;

//THIS IS A COPY OF diskCacheV111/services/FsPermissionHandler.java

import java.util.Arrays;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileMetaData;
import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CellAdapter;

import org.apache.log4j.Logger;

public class ACLPermissionHandler implements PermissionHandlerInterface {

    private final CellAdapter _cell;
    /**
     * if there is no ACL defined for a resource,
     * then we will ask some one else
     */
    private final PermissionHandlerInterface _fallBackHandler;
    private final FileMetaDataSource _metaDataSource;

    private final static Logger _logPermisions = Logger
            .getLogger("logger.org.dcache.authorization."
                    + ACLPermissionHandler.class.getName());

    public ACLPermissionHandler(CellAdapter cell,
            FileMetaDataSource metaDataSource) {
        _cell = cell;
        _metaDataSource = metaDataSource;
        _fallBackHandler = new UnixPermissionHandler(_cell,
                new PnfsManagerFileMetaDataSource(_cell));
    }

    public boolean canWrite(int userUid, int[] userGid, String pnfsPath) throws CacheException {
        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canWrite(" + userUid + "," + Arrays.toString( userGid )+ ","
                + pnfsPath + ")");
        }

        try {
            return fileCanWrite(userUid, userGid, _metaDataSource
                    .getMetaData(pnfsPath));
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

        boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
        boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);

        return parentWriteAllowed && parentExecuteAllowed;
    }

    public boolean canCreateDir(int userUid, int[] userGid, String pnfsPath) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canCreateDir(" + userUid + "," + Arrays.toString( userGid ) + ","
                + pnfsPath + ")");
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

        boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
        boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);

        return parentWriteAllowed && parentExecuteAllowed;
    }

    public boolean canDeleteDir(int userUid, int[] userGid, String pnfsPath) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDeleteDir(" + userUid + "," + Arrays.toString( userGid ) + ","
                + pnfsPath + ")");
        }

        FileMetaData meta = _metaDataSource.getMetaData(pnfsPath);
        if (!meta.isDirectory()) {
            _logPermisions.error(pnfsPath + " is not a directory");
            throw new CacheException("path is not a directory");
        }

        return fileCanWrite(userUid, userGid, meta);
    }

    public boolean canDelete(int userUid, int[] userGid, String pnfsPath) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDelete(" + userUid + "," + Arrays.toString( userGid ) + ","
                + pnfsPath + ")");
        }
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();

        FileMetaData meta = _metaDataSource.getMetaData(parent);

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canWrite() parent meta = " + meta);
        }
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not read "
                    + pnfsPath);
            return false;
        }

        boolean parentWriteAllowed = fileCanWrite(userUid, userGid, meta);
        boolean parentExecuteAllowed = fileCanExecute(userUid, userGid, meta);
        boolean parentReadAllowed = fileCanRead(userUid, userGid, meta);

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDelete() parent read allowed :"
                    + parentReadAllowed + " parent write allowed :"
                    + parentReadAllowed + " parent exec allowed :"
                    + parentExecuteAllowed);
        }

        if (!parentReadAllowed || !parentExecuteAllowed || !parentWriteAllowed) {
            _logPermisions.error(" parent write is not allowed ");
            return false;
        }

        meta = _metaDataSource.getMetaData(pnfsPath);
        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canDelete() file meta = " + meta);
        }

        boolean deleteAllowed = fileCanWrite(userUid, userGid, meta);

        if (deleteAllowed) {
            _logPermisions.error("WARNING: canDelete() delete of file "
                    + pnfsPath + " by user uid=" + userUid + " gid=" + Arrays.toString( userGid )
                    + " is allowed!");
        } else {
            if(_logPermisions.isDebugEnabled() ) {
                _logPermisions.debug("canDelete() delete of file " + pnfsPath
                        + " by user uid=" + userUid + " gid=" + Arrays.toString( userGid )
                        + " is not allowed");
            }
        }
        return deleteAllowed;
    }

    public boolean canRead(int userUid, int[] userGid, String pnfsPath) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("canRead(" + userUid + "," + Arrays.toString( userGid ) + ","
                    + pnfsPath + ")");
        }

        if (!fileCanRead(userUid, userGid, _metaDataSource
                .getMetaData(pnfsPath))) {
            return false;
        }

        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");
        String parent = parent_path.toString();

        return dirCanRead(userUid, userGid, parent);
    }

    private boolean dirCanRead(int userUid, int[] userGid, String path) throws CacheException {

        FileMetaData meta = _metaDataSource.getMetaData(path);
        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("dirCanRead() meta = " + meta);
        }
        if (!meta.isDirectory()) {
            _logPermisions.error(path
                    + " exists and is not a directory, can not read ");
            return false;
        }

        boolean readAllowed = fileCanRead(userUid, userGid, meta);

        boolean executeAllowed = fileCanExecute(userUid, userGid, meta);

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("dirCanRead() read allowed :" + readAllowed
                + "  exec allowed :" + executeAllowed);
        }
        if (!(readAllowed && executeAllowed)) {
            _logPermisions.error(" read is not allowed ");
            return false;
        }

        return readAllowed && executeAllowed;
    }

    public boolean worldCanRead(String pnfsPath) throws CacheException {

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("worldCanRead(" + pnfsPath + ")");
        }
        FsPath parent_path = new FsPath(pnfsPath);
        // go one level up
        parent_path.add("..");

        String parent = parent_path.toString();
        FileMetaData meta = _metaDataSource.getMetaData(parent);
        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("worldCanRead() parent meta = " + meta);
        }
        if (!meta.isDirectory()) {
            _logPermisions.error(parent
                    + " exists and is not a directory, can not read "
                    + pnfsPath);
            return false;
        }

        FileMetaData.Permissions world = meta.getWorldPermissions();
        boolean parentReadAllowed = world.canRead();
        boolean parentExecuteAllowed = world.canExecute();

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("worldCanRead() parent read allowed :"
                    + parentReadAllowed + " parent exec allowed :"
                    + parentExecuteAllowed);
        }

        if (!parentReadAllowed || !parentExecuteAllowed) {
            _logPermisions.error(" parent read is not allowed ");
            return false;
        }

        meta = _metaDataSource.getMetaData(pnfsPath);
        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("worldCanRead() file meta = " + meta);
        }
        world = meta.getWorldPermissions();
        boolean readAllowed = world.canRead();

        if(_logPermisions.isDebugEnabled() ) {
            _logPermisions.debug("worldCanRead() file read allowed :" + readAllowed);
        }
        return readAllowed;
    }

    public boolean worldCanWrite(String pnfsPath) throws CacheException {
        // simple and elegant
        return false;
    }

    // ////////////////////////////////////////////////////////////////////////////////
    // /
    // / Low level checks
    // /
    // ///////////////////////////////////////////////////////////////////////////////

    private static boolean fileCanWrite(int userUid, int[] userGid,
            FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == userUid) {
            writeAllowed = user.canWrite();
        } else if (meta.getGid() == userGid[0]) {
            writeAllowed = group.canWrite();
        } else {
            // world = all except user and group
            writeAllowed = world.canWrite();
        }

        return writeAllowed;
    }

    private static boolean fileCanExecute(int userUid, int[] userGid,
            FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean writeAllowed = false;

        if (meta.getUid() == userUid) {
            writeAllowed = user.canExecute();
        } else if (meta.getGid() == userGid[0]) {
            writeAllowed = group.canExecute();
        } else {
            // world = all except user and group
            writeAllowed = world.canExecute();
        }

        return writeAllowed;
    }

    private static boolean fileCanRead(int userUid, int[] userGid,
            FileMetaData meta) {

        FileMetaData.Permissions user = meta.getUserPermissions();
        FileMetaData.Permissions group = meta.getGroupPermissions();
        FileMetaData.Permissions world = meta.getWorldPermissions();

        boolean readAllowed = false;

        if (meta.getUid() == userUid) {
            readAllowed = user.canRead();
        } else if (meta.getGid() == userGid[0]) {
            readAllowed = group.canRead();
        } else {
            // world = all except user and group
            readAllowed = world.canRead();
        }

        return readAllowed;
    }

}
