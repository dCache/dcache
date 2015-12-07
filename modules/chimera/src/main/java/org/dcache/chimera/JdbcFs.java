/*
 * This library is free software; you can redistribute it and/or modify
 * it under the terms of the GNU Library General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Library General Public License for more details.
 *
 * You should have received a copy of the GNU Library General Public
 * License along with this program (see the file COPYING.LIB for more
 * details); if not, write to the Free Software Foundation, Inc.,
 * 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.dcache.chimera;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.NonTransientDataAccessResourceException;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import javax.sql.DataSource;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.acl.enums.AceFlags.*;
import static org.dcache.commons.util.SqlHelper.tryToClose;

/**
 *
 * JDBC-FS is THE building block of Chimera. It's an abstraction layer, which
 * allows to build filesystem on top of a RDBMS.
 *
 *
 * @Immutable
 * @Threadsafe
 */
public class JdbcFs implements FileSystemProvider {
    /**
     * common error message for unimplemented
     */
    private static final String NOT_IMPL =
                    "this operation is unsupported for this "
                                    + "file system; please install a dCache-aware "
                                    + "implementation of the file system interface";

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(JdbcFs.class);
    /**
     * the number of pnfs levels. Level zero associated with file real
     * content, which is not our regular case.
     */
    private static final int LEVELS_NUMBER = 7;
    private final RootInode _rootInode;
    private final String _wormID;

    /**
     * minimal binary handle size which can be processed.
    */
    private final static int MIN_HANDLE_LEN = 4;
    /**
     * SQL query engine
     */
    private final FsSqlDriver _sqlDriver;

    /**
     * Database connection pool
     */
    private final DataSource _dbConnectionsPool;

    private final PlatformTransactionManager _tx;

    private final TransactionDefinition _txDefinition = new DefaultTransactionDefinition();

    /*
     * A dummy constant key force bay cache interface. the value doesn't
     * matter - only that it's the same value every time
     */
    private final Integer DUMMY_KEY = 0;
    /**
     * Cache value of FsStat
     */
    private final Executor _fsStatUpdateExecutor =
            Executors.newSingleThreadExecutor(
                    new ThreadFactoryBuilder()
                            .setNameFormat("fsstat-updater-thread-%d")
                            .build()
            );

    private final LoadingCache<Object, FsStat> _fsStatCache
            = CacheBuilder.newBuilder()
                .refreshAfterWrite(100, TimeUnit.MILLISECONDS)
                .build(
                    CacheLoader.asyncReloading(new CacheLoader<Object, FsStat>() {

                        @Override
                        public FsStat load(Object k) throws Exception {
                            return JdbcFs.this.getFsStat0();
                        }
                    }
            , _fsStatUpdateExecutor));

    /**
     * current fs id
     */
    private final int _fsId;
    /**
     * available space (1 Exabyte)
     */
    static final long AVAILABLE_SPACE = 1152921504606846976L;
    /**
     * total files
     */
    static final long TOTAL_FILES = 62914560L;

    /**
     * maximal length of an object name in a directory.
     */
    private final static int MAX_NAME_LEN = 255;

    public JdbcFs(DataSource dataSource, PlatformTransactionManager txManager, String dialect) {
        this(dataSource, txManager, dialect, 0);
    }

    public JdbcFs(DataSource dataSource, PlatformTransactionManager txManager, String dialect, int id) {
        _dbConnectionsPool = dataSource;
        _fsId = id;

        _tx = txManager;

        // try to get database dialect specific query engine
        _sqlDriver = FsSqlDriver.getDriverInstance(dialect, dataSource);

        _rootInode = new RootInode(this);

        String wormID = null;
        try {
            wormID = getWormID().toString();
        } catch (Exception e) {
        }
        _wormID = wormID;
    }

    private FsInode getWormID() throws ChimeraFsException {

        return this.path2inode("/admin/etc/config");
    }

    private <T> T inTransaction(FallibleTransactionCallback<T> callback)
            throws ChimeraFsException
    {
        TransactionStatus status = _tx.getTransaction(_txDefinition);
        T result;
        try {
            result = callback.doInTransaction(status);
            _tx.commit(status);
        } catch (ChimeraFsException e) {
            rollbackOnException(status, e);
            throw e;
        } catch (NonTransientDataAccessResourceException e) {
            rollbackOnException(status, e);
            throw new BackEndErrorHimeraFsException(e.getMessage(), e);
        } catch (DataAccessException e) {
            rollbackOnException(status, e);
            throw new IOHimeraFsException(e.getMessage(), e);
        } catch (Exception e) {
            rollbackOnException(status, e);
            throw e;
        }
        return result;
    }

    /**
     * Perform a rollback, handling rollback exceptions properly.
     * @param status object representing the transaction
     * @param ex the thrown application exception or error
     * @throws TransactionException in case of a rollback error
     */
    private void rollbackOnException(TransactionStatus status, Throwable ex) throws TransactionException {
        _log.debug("Initiating transaction rollback on application exception", ex);
        try {
            _tx.rollback(status);
        } catch (TransactionSystemException e) {
            _log.error("Application exception overridden by rollback exception", ex);
            e.initApplicationException(ex);
            throw e;
        } catch (RuntimeException e) {
            _log.error("Application exception overridden by rollback exception", ex);
            throw e;
        } catch (Error err) {
            _log.error("Application exception overridden by rollback error", ex);
            throw err;
        }
    }

    //////////////////////////////////////////////////////////
    ////
    ////
    ////      Fs operations
    ////
    /////////////////////////////////////////////////////////
    @Override
    public FsInode createLink(String src, String dest) throws ChimeraFsException {
        File file = new File(src);
        return inTransaction(status -> createLink(path2inode(file.getParent()), file.getName(), dest));
    }

    @Override
    public FsInode createLink(FsInode parent, String name, String dest) throws ChimeraFsException {
        return inTransaction(status -> createLink(parent, name, 0, 0, 0644, dest.getBytes()));
    }

    @Override
    public FsInode createLink(FsInode parent, String name, int uid, int gid, int mode, byte[] dest) throws ChimeraFsException {

        checkNameLength(name);

        return inTransaction(status -> {
            FsInode inode;
            try {
                Stat stat = parent.statCache();
                int group = (stat.getMode() & UnixPermission.S_ISGID) != 0 ? stat.getGid() : gid;
                inode = _sqlDriver.createFile(parent, name, uid, group, mode, UnixPermission.S_IFLNK);
                // link is a regular file where content is a reference
                _sqlDriver.setInodeIo(inode, true);
                _sqlDriver.write(inode, 0, 0, dest, 0, dest.length);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                                   EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                                   EnumSet.of(FILE_INHERIT_ACE));
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return inode;
        });
    }

    /**
     *
     * create a hard link
     *
     * @param parent inode of directory where to create
     * @param inode
     * @param name
     * @return
     * @throws ChimeraFsException
     */
    @Override
    public FsInode createHLink(FsInode parent, FsInode inode, String name) throws ChimeraFsException {

        checkNameLength(name);

        return inTransaction(status -> {
            try {
                _sqlDriver.createEntryInParent(parent, name, inode);
                _sqlDriver.incNlink(inode);
                _sqlDriver.incNlink(parent);
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
            return inode;
        });
    }

    @Override
    public FsInode createFile(String path) throws ChimeraFsException {
        File file = new File(path);
        return inTransaction(status -> createFile(path2inode(file.getParent()), file.getName()));
    }

    @Override
    public FsInode createFile(FsInode parent, String name) throws ChimeraFsException {
        return inTransaction(status -> createFile(parent, name, 0, 0, 0644));
    }

    @Override
    public FsInode createFileLevel(FsInode inode, int level) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.createLevel(inode, 0, 0, 0644 | UnixPermission.S_IFREG, level));
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode) throws ChimeraFsException {
        return createFile(parent, name, owner, group, mode, UnixPermission.S_IFREG);
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode, int type) throws ChimeraFsException {
        if (name.startsWith(".(")) { // special files only
            String[] cmd = PnfsCommandProcessor.process(name);

            if (name.startsWith(".(tag)(") && (cmd.length == 2)) {
                this.createTag(parent, cmd[1], owner, group, 0644);
                return new FsInode_TAG(this, parent.toString(), cmd[1]);
            }

            if (name.startsWith(".(pset)(") || name.startsWith(".(fset)(")) {
                /**
                 * This is not 100% correct, as we throw exist even if
                 * someone tries to set attribute for a file which does not exist.
                 */
                throw new FileExistsChimeraFsException(name);
            }

            if (name.startsWith(".(use)(") && (cmd.length == 3)) {
                int level = Integer.parseInt(cmd[1]);
                return inTransaction(status -> {
                    FsInode useInode = _sqlDriver.inodeOf(parent, cmd[2]);
                    if (useInode == null) {
                        throw new FileNotFoundHimeraFsException(cmd[2]);
                    }
                    try {
                        Stat stat = useInode.statCache();
                        return _sqlDriver.createLevel(useInode, stat.getUid(),
                                                      stat.getGid(),
                                                      stat.getMode(), level);
                    } catch (DuplicateKeyException e) {
                        throw new FileExistsChimeraFsException(name, e);
                    }
                });
            }

            if (name.startsWith(".(access)(") && (cmd.length == 3)) {
                FsInode accessInode = new FsInode(this, cmd[1]);
                int accessLevel = Integer.parseInt(cmd[2]);
                if (accessLevel == 0) {
                    return accessInode;
                }
                return inTransaction(status -> {
                    try {
                        Stat stat = accessInode.stat();
                        return _sqlDriver.createLevel(accessInode,
                                                      stat.getUid(), stat.getGid(),
                                                      stat.getMode(), accessLevel);
                    } catch (DuplicateKeyException e) {
                        throw new FileExistsChimeraFsException(name, e);
                    }
                });
            }

            return null;
        }

        checkNameLength(name);
        checkArgument(UnixPermission.getType(type) != UnixPermission.S_IFDIR);

        return inTransaction(status -> {
            try {
                Stat parentStat = parent.statCache();
                if (parentStat == null) {
                    throw new FileNotFoundHimeraFsException("parent=" + parent.toString());
                }

                if ((parentStat.getMode() & UnixPermission.F_TYPE) != UnixPermission.S_IFDIR) {
                    throw new NotDirChimeraException(parent);
                }

                int gid = (parentStat.getMode() & UnixPermission.S_ISGID) != 0 ? parentStat.getGid() : group;
                FsInode inode = _sqlDriver.createFile(parent, name, owner, gid, mode, type);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                                   EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                                   EnumSet.of(FILE_INHERIT_ACE));
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
        });
    }

    /**
     * Create a new entry with given inode id.
     *
     * @param parent
     * @param inode
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws ChimeraFsException
     */
    @Override
    public void createFileWithId(FsInode parent, FsInode inode, String name, int owner, int group, int mode, int type) throws ChimeraFsException {

        checkNameLength(name);
        checkArgument((type & UnixPermission.S_IFDIR) == 0);

        inTransaction(status -> {
            try {
                if (!parent.exists()) {
                    throw new FileNotFoundHimeraFsException("parent=" + parent.toString());
                }
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }
                Stat stat = parent.statCache();
                int gid = (stat.getMode() & UnixPermission.S_ISGID) != 0 ? stat.getGid() : group;
                _sqlDriver.createFileWithId(parent, inode, name, owner, gid, mode, type);
                _sqlDriver.copyAcl(parent, inode, RsType.FILE,
                                   EnumSet.of(INHERIT_ONLY_ACE, DIRECTORY_INHERIT_ACE, FILE_INHERIT_ACE),
                                   EnumSet.of(FILE_INHERIT_ACE));
                return null;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(e);
            }
        });
    }

    @Override
    public String[] listDir(String dir) {
        try {
            return listDir(path2inode(dir));
        } catch (ChimeraFsException e) {
            return null;
        }
    }

    @Override
    public String[] listDir(FsInode dir) throws ChimeraFsException {
        return _sqlDriver.listDir(dir);
    }

    @Override
    public DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir) throws IOHimeraFsException {
        return _sqlDriver.newDirectoryStream(dir);
    }

    @Override
    public void remove(String path) throws ChimeraFsException {

        File filePath = new File(path);

        String parentPath = filePath.getParent();
        if (parentPath == null) {
            throw new InvalidArgumentChimeraException("Cannot delete file system root.");
        }

        inTransaction(status -> {
            FsInode parent = path2inode(parentPath);
            String name = filePath.getName();
            FsInode inode = _sqlDriver.inodeOf(parent, name);
            if (inode == null || !_sqlDriver.remove(parent, name, inode)) {
                throw new FileNotFoundHimeraFsException(path);
            }
            return null;
        });
    }

    @Override
    public void remove(FsInode directory, String name, FsInode inode) throws ChimeraFsException {
        inTransaction(status -> {
            if (!_sqlDriver.remove(directory, name, inode)) {
                throw new FileNotFoundHimeraFsException(name);
            }
            return null;
        });
    }

    @Override
    public void remove(FsInode inode) throws ChimeraFsException {
        inTransaction(status -> {
            if (inode.type() != FsInodeType.INODE) {
                // now allowed
                throw new InvalidArgumentChimeraException("Not a file.");
            }
            if (inode.equals(_rootInode)) {
                throw new InvalidArgumentChimeraException("Cannot delete file system root.");
            }
            if (!inode.exists()) {
                throw new FileNotFoundHimeraFsException("No such file.");
            }
            if (inode.isDirectory() && inode.statCache().getNlink() > 2) {
                throw new DirNotEmptyHimeraFsException("Directory is not empty");
            }
            _sqlDriver.remove(inode);
            return null;
        });
    }

    @Override
    public Stat stat(String path) throws ChimeraFsException {
        return stat(path2inode(path));
    }

    @Override
    public Stat stat(FsInode inode) throws ChimeraFsException {
        return stat(inode, 0);
    }

    @Override
    public Stat stat(FsInode inode, int level) throws ChimeraFsException {
        Stat stat = _sqlDriver.stat(inode, level);
        if (stat == null) {
            throw new FileNotFoundHimeraFsException(inode.toString());
        }
        return stat;
    }

    @Override
    public FsInode mkdir(String path) throws ChimeraFsException {
        int li = path.lastIndexOf('/');
        String file = path.substring(li + 1);
        String dir = (li > 1) ? path.substring(0, li) : "/";
        return inTransaction(status -> mkdir(path2inode(dir), file));
    }

    @Override
    public FsInode mkdir(FsInode parent, String name) throws ChimeraFsException {
        return mkdir(parent, name, 0, 0, 0755);
    }

    @Override
    public FsInode mkdir(FsInode parent, String name, int owner, int group, int mode) throws ChimeraFsException {
        checkNameLength(name);

        return inTransaction(status -> {
            try {
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }

                Stat stat = parent.statCache();
                int gid, perm;
                if ((stat.getMode() & UnixPermission.S_ISGID) != 0) {
                    gid = stat.getGid();
                    perm = mode | UnixPermission.S_ISGID;
                } else {
                    gid = group;
                    perm = mode;
                }

                FsInode inode = _sqlDriver.mkdir(parent, name, owner, gid, perm);
                _sqlDriver.copyTags(parent, inode);
                _sqlDriver.copyAcl(parent, inode, RsType.DIR, EnumSet.of(INHERIT_ONLY_ACE),
                                   EnumSet.of(FILE_INHERIT_ACE, DIRECTORY_INHERIT_ACE));
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(name, e);
            }
        });
    }

    @Override
    public FsInode mkdir(FsInode parent, String name, int owner, int group, int mode,
                         List<ACE> acl, Map<String, byte[]> tags)
            throws ChimeraFsException
    {
        checkNameLength(name);

        return inTransaction(status -> {
            try {
                if (!parent.isDirectory()) {
                    throw new NotDirChimeraException(parent);
                }
                Stat stat = parent.statCache();
                int gid, perm;
                if ((stat.getMode() & UnixPermission.S_ISGID) != 0) {
                    gid = stat.getGid();
                    perm = mode | UnixPermission.S_ISGID;
                } else {
                    gid = group;
                    perm = mode;
                }
                FsInode inode = _sqlDriver.mkdir(parent, name, owner, gid, perm);
                _sqlDriver.createTags(inode, owner, gid, perm & 0666, tags);
                _sqlDriver.writeAcl(inode, RsType.DIR, acl);
                return inode;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException(name, e);
            }
        });
    }

    @Override
    public FsInode path2inode(String path) throws ChimeraFsException {
        return path2inode(path, new RootInode(this));
    }

    @Override
    public FsInode path2inode(String path, FsInode startFrom) throws ChimeraFsException {
        FsInode inode = _sqlDriver.path2inode(startFrom, path);
        if (inode == null) {
            throw new FileNotFoundHimeraFsException(path);
        }
        return inode;
    }

    @Override
    public List<FsInode> path2inodes(String path) throws ChimeraFsException
    {
        return path2inodes(path, new RootInode(this));
    }

    @Override
    public List<FsInode> path2inodes(String path, FsInode startFrom)
        throws ChimeraFsException
    {
        List<FsInode> inodes = _sqlDriver.path2inodes(startFrom, path);
        if (inodes.isEmpty()) {
            throw new FileNotFoundHimeraFsException(path);
        }
        return inodes;
    }

    @Override
    public FsInode inodeOf(FsInode parent, String name) throws ChimeraFsException {
        // only if it's PNFS command
        if (name.startsWith(".(")) {

            if (name.startsWith(".(id)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1]);
                if (inode == null) {
                    throw new FileNotFoundHimeraFsException(cmd[1]);
                }
                return new FsInode_ID(this, inode.toString());
            }

            if (name.startsWith(".(use)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                try {
                    int level = Integer.parseInt(cmd[1]);

                    FsInode inode = _sqlDriver.inodeOf(parent, cmd[2]);
                    if (inode == null) {
                        throw new FileNotFoundHimeraFsException(cmd[2]);
                    }
                    if (level <= LEVELS_NUMBER) {
                        stat(inode, level);
                        return new FsInode(this, inode.toString(), level);
                    } else {
                        // is it error or a real file?
                    }
                } catch (NumberFormatException nfe) {
                    //	possible wrong format...ignore
                } catch (FileNotFoundHimeraFsException e) {
                    throw new FileNotFoundHimeraFsException(name);
                }

            }

            if (name.startsWith(".(access)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if ((cmd.length < 2) || (cmd.length > 3)) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                try {
                    int level = cmd.length == 2 ? 0 : Integer.parseInt(cmd[2]);

                    FsInode useInode = new FsInode(this, cmd[1]);

                    if (level <= LEVELS_NUMBER) {
                        this.stat(useInode, level);
                        return new FsInode(this, useInode.toString(), level);
                    } else {
                        // is it error or a real file?
                    }
                } catch (NumberFormatException nfe) {
                    //	possible wrong format...ignore
                } catch (FileNotFoundHimeraFsException e) {
                    throw new FileNotFoundHimeraFsException(name);
                }

            }

            if (name.startsWith(".(nameof)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode nameofInode = new FsInode_NAMEOF(this, cmd[1]);
                if (!nameofInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return nameofInode;
            }

            if (name.startsWith(".(const)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode constInode = new FsInode_CONST(this, parent.toString());
                if (!constInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return constInode;
            }

            if (name.startsWith(".(parent)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode parentInode = new FsInode_PARENT(this, cmd[1]);
                if (!parentInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return parentInode;
            }

            if (name.startsWith(".(pathof)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode pathofInode = new FsInode_PATHOF(this, cmd[1]);
                if (!pathofInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return pathofInode;
            }

            if (name.startsWith(".(tag)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode tagInode = new FsInode_TAG(this, parent.toString(), cmd[1]);
                if (!tagInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return tagInode;
            }

            if (name.equals(".(tags)()")) {
                return new FsInode_TAGS(this, parent.toString());
            }

            if (name.startsWith(".(pset)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                String[] args = new String[cmd.length - 2];
                System.arraycopy(cmd, 2, args, 0, args.length);
                FsInode psetInode = new FsInode_PSET(this, cmd[1], args);
                if (!psetInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return psetInode;
            }

            if (name.equals(".(get)(cursor)")) {
                FsInode pgetInode = new FsInode_PCUR(this, parent.toString());
                if (!pgetInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return pgetInode;
            }

            if (name.startsWith(".(get)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }

                FsInode inode = _sqlDriver.inodeOf(parent, cmd[1]);
                if (inode == null) {
                    throw new FileNotFoundHimeraFsException(cmd[1]);
                }
                switch(cmd[2]) {
                    case "locality":
                        return new FsInode_PLOC(this, inode.toString());
                    case "checksum":
                    case "checksums":
                        return new FsInode_PCRC(this, inode.toString());
                    default:
                        throw new ChimeraFsException
                            ("unsupported argument for .(get) " + cmd[2]);
                }
            }

            if (name.equals(".(config)")) {
                return new FsInode(this, _wormID);
            }

            if (name.startsWith(".(config)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                FsInode inode = _sqlDriver.inodeOf(new FsInode(this, _wormID), cmd[1]);
                if (inode == null) {
                    throw new FileNotFoundHimeraFsException(cmd[1]);
                }
                return inode;
            }

            if (name.startsWith(".(fset)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                String[] args = new String[cmd.length - 2];
                System.arraycopy(cmd, 2, args, 0, args.length);

                FsInode fsetInode = _sqlDriver.inodeOf(parent, cmd[1]);
                if (fsetInode == null) {
                    throw new FileNotFoundHimeraFsException(cmd[1]);
                }
                return new FsInode_PSET(this, fsetInode.toString(), args);
            }

        }

        FsInode inode = _sqlDriver.inodeOf(parent, name);
        if (inode == null) {
            throw new FileNotFoundHimeraFsException(name);
        }
        inode.setParent(parent);
        return inode;
    }

    @Override
    public String inode2path(FsInode inode) throws ChimeraFsException {
        return inode2path(inode, new RootInode(this));
    }

    /**
     *
     * @param inode
     * @param startFrom
     * @return path of inode starting from startFrom
     * @throws ChimeraFsException
     */
    @Override
    public String inode2path(FsInode inode, FsInode startFrom) throws ChimeraFsException {
        return _sqlDriver.inode2path(inode, startFrom);
    }

    @Override
    public boolean removeFileMetadata(String path, int level) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.removeInodeLevel(path2inode(path), level));
    }

    @Override
    public FsInode getParentOf(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getParentOf(inode);
    }

    @Override
    public void setInodeAttributes(FsInode inode, int level, Stat stat) throws ChimeraFsException {
        inTransaction(status -> {
            switch (inode.type()) {
            case INODE:
            case PSET:
                boolean applied = _sqlDriver.setInodeAttributes(inode, level, stat);
                if (!applied) {
                    /**
                     * there are two cases why update can fail: 1. inode
                     * does not exists 2. we try to set a size on a non file
                     * object
                     */
                    Stat s = _sqlDriver.stat(inode);
                    if (s == null) {
                        throw new FileNotFoundHimeraFsException();
                    }
                    if ((s.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR) {
                        throw new IsDirChimeraException(inode);
                    }
                    throw new InvalidArgumentChimeraException();
                }
                break;
            case TAG:
                if (stat.isDefined(Stat.StatAttributes.MODE)) {
                    _sqlDriver.setTagMode((FsInode_TAG) inode, stat.getMode());
                }
                if (stat.isDefined(Stat.StatAttributes.UID)) {
                    _sqlDriver.setTagOwner((FsInode_TAG) inode, stat.getUid());
                }
                if (stat.isDefined(Stat.StatAttributes.GID)) {
                    _sqlDriver.setTagOwnerGroup((FsInode_TAG) inode, stat.getGid());
                }
                break;
            }
            return null;
        });
    }

    @Override
    public boolean isIoEnabled(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.isIoEnabled(inode);
    }

    @Override
    public void setInodeIo(FsInode inode, boolean enable) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setInodeIo(inode, enable);
            return null;
        });
    }

    public int write(FsInode inode, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        return this.write(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int write(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        return inTransaction(status -> {
            try {
                if (level == 0 && !inode.isIoEnabled()) {
                    _log.debug("{}: IO (write) not allowed", inode);
                    return -1;
                }
                return _sqlDriver.write(inode, level, beginIndex, data, offset, len);
            } catch (ForeignKeyViolationException e) {
                throw new FileNotFoundHimeraFsException(e);
            }
        });
    }

    public int read(FsInode inode, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        return this.read(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int read(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        if (level == 0 && !inode.isIoEnabled()) {
            _log.debug("{}: IO(read) not allowed", inode);
            return -1;
        }
        return _sqlDriver.read(inode, level, beginIndex, data, offset, len);
    }

    @Override
    public byte[] readLink(String path) throws ChimeraFsException {
        return readLink(path2inode(path));
    }

    @Override
    public byte[] readLink(FsInode inode) throws ChimeraFsException {
        int len = (int) inode.statCache().getSize();
        byte[] b = new byte[len];
        int n = read(inode, 0, b, 0, b.length);
        return (n >= 0) ? b : new byte[0];
    }

    @Override
    public boolean rename(FsInode inode, FsInode srcDir, String source, FsInode destDir, String dest) throws ChimeraFsException {
        checkNameLength(dest);

        return inTransaction(status -> {
            if (!destDir.isDirectory()) {
                throw new NotDirChimeraException(destDir);
            }

            FsInode destInode = _sqlDriver.inodeOf(destDir, dest);

            if (destInode != null) {
                if (destInode.equals(inode)) {
                    // according to POSIX, we are done
                    return false;
                }

               /* Renaming into existing is only allowed for the same type of entry.
                */
                if (inode.isDirectory() != destInode.isDirectory()) {
                    throw new FileExistsChimeraFsException(dest);
                }

                if (!_sqlDriver.remove(destDir, dest, destInode)) {
                    // Concurrent modification - retry
                    return rename(inode, srcDir, source, destDir, dest);
                }
            }

            if (!_sqlDriver.rename(inode, srcDir, source, destDir, dest)) {
                throw new FileNotFoundHimeraFsException(source);
            }
            return true;
        });
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Location info
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public List<StorageLocatable> getInodeLocations(FsInode inode, int type) throws ChimeraFsException {
        return _sqlDriver.getInodeLocations(inode, type);
    }

    @Override
    public List<StorageLocatable> getInodeLocations(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getInodeLocations(inode);
    }

    @Override
    public void addInodeLocation(FsInode inode, int type, String location) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.addInodeLocation(inode, type, location);
            } catch (ForeignKeyViolationException e) {
                throw new FileNotFoundHimeraFsException(e);
            }
            return null;
        });
    }

    @Override
    public void clearInodeLocation(FsInode inode, int type, String location) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.clearInodeLocation(inode, type, location);
            return null;
        });
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Directory tags handling
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public String[] tags(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.tags(inode);
    }

    @Override
    public Map<String, byte[]> getAllTags(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getAllTags(inode);
    }

    @Override
    public void createTag(FsInode inode, String name) throws ChimeraFsException {
        this.createTag(inode, name, 0, 0, 0644);
    }

    @Override
    public void createTag(FsInode inode, String name, int uid, int gid, int mode) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.createTag(inode, name, uid, gid, mode);
                return null;
            } catch (DuplicateKeyException e) {
                throw new FileExistsChimeraFsException();
            }
        });
    }

    @Override
    public int setTag(FsInode inode, String tagName, byte[] data, int offset, int len) throws ChimeraFsException {
        return inTransaction(status -> _sqlDriver.setTag(inode, tagName, data, offset, len));
    }

    @Override
    public void removeTag(FsInode dir, String tagName) throws ChimeraFsException
    {
        inTransaction(status -> {
            _sqlDriver.removeTag(dir, tagName);
            return null;
        });
    }

    @Override
    public void removeTag(FsInode dir) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.removeTag(dir);
            return null;
        });
    }

    @Override
    public int getTag(FsInode inode, String tagName, byte[] data, int offset, int len) throws ChimeraFsException {
        return _sqlDriver.getTag(inode, tagName, data, offset, len);
    }

    @Override
    public Stat statTag(FsInode dir, String name) throws ChimeraFsException {
        return _sqlDriver.statTag(dir, name);
    }

    @Override
    public void setTagOwner(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagOwner(tagInode, owner);
            return null;
        });
    }

    @Override
    public void setTagOwnerGroup(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagOwnerGroup(tagInode, owner);
            return null;
        });
    }

    @Override
    public void setTagMode(FsInode_TAG tagInode, String name, int mode) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.setTagMode(tagInode, mode);
            return null;
        });
    }

    ///////////////////////////////////////////////////////////////
    //
    // Id and Co.
    //
    @Override
    public int getFsId() {
        return _fsId;
    }

    /*
     * Storage Information
     *
     * currently it's not allowed to modify it
     */
    @Override
    public void setStorageInfo(FsInode inode, InodeStorageInformation storageInfo) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.setStorageInfo(inode, storageInfo);
            } catch (ForeignKeyViolationException e) {
                throw new FileNotFoundHimeraFsException(e);
            }
            return null;
        });
    }

    @Override
    public InodeStorageInformation getStorageInfo(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.getStorageInfo(inode);
    }

    /*
     * inode checksum handling
     */
    @Override
    public void setInodeChecksum(FsInode inode, int type, String checksum) throws ChimeraFsException {
        inTransaction(status -> {
            try {
                _sqlDriver.setInodeChecksum(inode, type, checksum);
            } catch (ForeignKeyViolationException e) {
                throw new FileNotFoundHimeraFsException(e);
            }
            return null;
        });
    }

    @Override
    public void removeInodeChecksum(FsInode inode, int type) throws ChimeraFsException {
        inTransaction(status -> {
            _sqlDriver.removeInodeChecksum(inode, type);
            return null;
        });
    }

    @Override
    public Set<Checksum> getInodeChecksums(FsInode inode) throws ChimeraFsException {
        return new HashSet<>(_sqlDriver.getInodeChecksums(inode));
    }

    /**
     * Get inode's Access Control List. An empty list is returned if there are no ACL assigned
     * to the <code>inode</code>.
     * @param inode
     * @return acl
     */
    @Override
    public List<ACE> getACL(FsInode inode) throws ChimeraFsException {
        return _sqlDriver.readAcl(inode);
    }

    /**
     * Set inode's Access Control List. The existing ACL will be replaced.
     * @param inode
     * @param acl
     */
    @Override
    public void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException {
        inTransaction(status -> {
            boolean modified = _sqlDriver.deleteAcl(inode);
            if (!acl.isEmpty()) {
                _sqlDriver.writeAcl(inode, inode.isDirectory() ? RsType.DIR : RsType.FILE, acl);
                modified = true;
            }
            if (modified) {
                // empty stat will update ctime
                _sqlDriver.setInodeAttributes(inode, 0, new Stat());
            }
            return null;
        });
    }

    private static void checkNameLength(String name) throws InvalidNameChimeraException {
        if (name.length() > MAX_NAME_LEN) {
            throw new InvalidNameChimeraException("Name too long");
        }
    }

    public FsStat getFsStat0() throws ChimeraFsException {
        return _sqlDriver.getFsStat();
    }

    @Override
    public FsStat getFsStat() throws ChimeraFsException {
        try {
            return _fsStatCache.get(DUMMY_KEY);
        } catch(ExecutionException e) {
            Throwable t = e.getCause();
            Throwables.propagateIfPossible(t, ChimeraFsException.class);
            throw new ChimeraFsException(t.getMessage(), t);
        }
    }

    ///////////////////////////////////////////////////////////////
    //
    //  Some information
    //
    @Override
    public String getInfo() {

        String databaseProductName = "Unknown";
        String databaseProductVersion = "Unknown";
        Connection dbConnection = null;
        try {
            dbConnection = _dbConnectionsPool.getConnection();
            if (dbConnection != null) {
                databaseProductName = dbConnection.getMetaData().getDatabaseProductName();
                databaseProductVersion = dbConnection.getMetaData().getDatabaseProductVersion();
            }
        } catch (SQLException se) {
            // ignored
        } finally {
            tryToClose(dbConnection);
        }

        StringBuilder sb = new StringBuilder();

        sb.append("DB        : ").append(_dbConnectionsPool).append("\n");
        sb.append("DB Engine : ").append(databaseProductName).append(" ").append(databaseProductVersion).append("\n");
        sb.append("rootID    : ").append(_rootInode).append("\n");
        sb.append("wormID    : ").append(_wormID).append("\n");
        sb.append("FsId      : ").append(_fsId).append("\n");
        return sb.toString();
    }


    /*
     * (non-Javadoc)
     * @see java.io.Closeable#close()
     */
    @Override
    public void close() throws IOException {
	// enforced by the interface
    }

    private final static byte[] FH_V0_BIN = new byte[] {0x30, 0x30, 0x30, 0x30};
    private final static byte[] FH_V0_REG = new byte[]{0x30, 0x3a};
    private final static byte[] FH_V0_PFS = new byte[]{0x32, 0x35, 0x35, 0x3a};

    private static boolean arrayStartsWith(byte[] a1, byte[] a2) {
        if (a1.length < a2.length) {
            return false;
        }
        for (int i = 0; i < a2.length; i++) {
            if (a1[i] != a2[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public FsInode inodeFromBytes(byte[] handle) throws ChimeraFsException {

        if (arrayStartsWith(handle, FH_V0_REG) || arrayStartsWith(handle, FH_V0_PFS)) {
            return inodeFromBytesOld(handle);
        } else if (arrayStartsWith(handle, FH_V0_BIN)) {
            return inodeFromBytesNew(InodeId.hexStringToByteArray(new String(handle)));
        } else {
            return inodeFromBytesNew(handle);
        }
    }

    private final static char[] HEX = new char[]{
        '0', '1', '2', '3', '4', '5', '6', '7',
        '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'
    };

    /**
     * Returns a hexadecimal representation of given byte array.
     *
     * @param bytes whose string representation to return
     * @return a string representation of <tt>bytes</tt>
     */
    public static String toHexString(byte[] bytes) {

        char[] chars = new char[bytes.length * 2];
        int p = 0;
        for (byte b : bytes) {
            int i = b & 0xff;
            chars[p++] = HEX[i / 16];
            chars[p++] = HEX[i % 16];
        }
        return new String(chars);
    }

    private String[] getArgs(byte[] bytes) {

        StringTokenizer st = new StringTokenizer(new String(bytes), "[:]");
        int argc = st.countTokens();
        String[] args = new String[argc];
        for (int i = 0; i < argc; i++) {
            args[i] = st.nextToken();
        }

        return args;
    }

    FsInode inodeFromBytesNew(byte[] handle) throws ChimeraFsException {

        FsInode inode;

        if (handle.length < MIN_HANDLE_LEN) {
            throw new FileNotFoundHimeraFsException("File handle too short");
        }

        ByteBuffer b = ByteBuffer.wrap(handle);
        int fsid = b.get();
        int type = b.get();
        int idLen = b.get();
        byte[] id = new byte[idLen];
        b.get(id);
        int opaqueLen = b.get();
        if (opaqueLen > b.remaining()) {
            throw new FileNotFoundHimeraFsException("Bad Opaque len");
        }

        byte[] opaque = new byte[opaqueLen];
        b.get(opaque);

        FsInodeType inodeType = FsInodeType.valueOf(type);
        String inodeId = toHexString(id);

        switch (inodeType) {
            case INODE:
                int level = Integer.parseInt( new String(opaque));
                inode = new FsInode(this, inodeId, level);
                break;

            case ID:
                inode = new FsInode_ID(this, inodeId);
                break;

            case TAGS:
                inode = new FsInode_TAGS(this, inodeId);
                break;

            case TAG:
                String tag = new String(opaque);
                inode = new FsInode_TAG(this, inodeId, tag);
                break;

            case NAMEOF:
                inode = new FsInode_NAMEOF(this, inodeId);
                break;
            case PARENT:
                inode = new FsInode_PARENT(this, inodeId);
                break;

            case PATHOF:
                inode = new FsInode_PATHOF(this, inodeId);
                break;

            case CONST:
                inode = new FsInode_CONST(this, inodeId);
                break;

            case PSET:
                inode = new FsInode_PSET(this, inodeId, getArgs(opaque));
                break;

            case PCUR:
                inode = new FsInode_PCUR(this, inodeId);
                break;

            case PLOC:
                inode = new FsInode_PLOC(this, inodeId);
                break;

            case PCRC:
                inode = new FsInode_PCRC(this, inodeId);
                break;

            default:
                throw new FileNotFoundHimeraFsException("Unsupported file handle type: " + inodeType);
        }
        return inode;
    }

    FsInode inodeFromBytesOld(byte[] handle) throws ChimeraFsException {
        FsInode inode = null;

        String strHandle = new String(handle);

        StringTokenizer st = new StringTokenizer(strHandle, "[:]");

        if (st.countTokens() < 3) {
            throw new IllegalArgumentException("Invalid HimeraNFS handler.("
                    + strHandle + ")");
        }

        /*
         * reserved for future use
         */
        int fsId = Integer.parseInt(st.nextToken());

        String type = st.nextToken();

        try {
            // IllegalArgumentException will be thrown is it's wrong type

            FsInodeType inodeType = FsInodeType.valueOf(type);
            String id;
            int argc;
            String[] args;

            switch (inodeType) {
                case INODE:
                    id = st.nextToken();
                    int level = 0;
                    if (st.countTokens() > 0) {
                        level = Integer.parseInt(st.nextToken());
                    }
                    inode = new FsInode(this, id, level);
                    break;

                case ID:
                    id = st.nextToken();
                    inode = new FsInode_ID(this, id);
                    break;

                case TAGS:
                    id = st.nextToken();
                    inode = new FsInode_TAGS(this, id);
                    break;

                case TAG:
                    id = st.nextToken();
                    String tag = st.nextToken();
                    inode = new FsInode_TAG(this, id, tag);
                    break;

                case NAMEOF:
                    id = st.nextToken();
                    inode = new FsInode_NAMEOF(this, id);
                    break;
                case PARENT:
                    id = st.nextToken();
                    inode = new FsInode_PARENT(this, id);
                    break;

                case PATHOF:
                    id = st.nextToken();
                    inode = new FsInode_PATHOF(this, id);
                    break;

                case CONST:
                    String cnst = st.nextToken();
                    inode = new FsInode_CONST(this, cnst);
                    break;

                case PSET:
                    id = st.nextToken();
                    argc = st.countTokens();
                    args = new String[argc];
                    for (int i = 0; i < argc; i++) {
                        args[i] = st.nextToken();
                    }
                    inode = new FsInode_PSET(this, id, args);
                    break;

                case PCUR:
                    id = st.nextToken();
                    inode = new FsInode_PCUR(this, id);
                    break;

                case PLOC:
                    id = st.nextToken();
                    inode = new FsInode_PLOC(this, id);
                    break;

                case PCRC:
                    id = st.nextToken();
                    inode = new FsInode_PCRC(this, id);
                    break;

            }
        } catch (IllegalArgumentException iae) {
            _log.info("Failed to generate an inode from file handle : {} : {}", strHandle, iae);
            inode = null;
        }

        return inode;
    }

    @Override
    public byte[] inodeToBytes(FsInode inode) throws ChimeraFsException {
        return inode.getIdentifier();
    }

    @Override
    public String getFileLocality(FsInode_PLOC node) throws ChimeraFsException {
        throw new ChimeraFsException(NOT_IMPL);
    }

    /**
     * To maintain the abstraction level, we relegate the actual
     * callout to the subclass.
     */
    @Override
    public void pin(String pnfsid, long lifetime) throws ChimeraFsException {
       throw new ChimeraFsException(NOT_IMPL);
    }

   /**
    * To maintain the abstraction level, we relegate the actual
    * callout to the subclass.
    */
    @Override
    public void unpin(String pnfsid) throws ChimeraFsException {
       throw new ChimeraFsException(NOT_IMPL);
    }

    private interface FallibleTransactionCallback<T>
    {
        T doInTransaction(TransactionStatus status) throws ChimeraFsException;
    }

    private static class RootInode extends FsInode
    {
        public RootInode(FileSystemProvider fs)
        {
            super(fs, "000000000000000000000000000000000000");
        }

        @Override
        public boolean exists() throws ChimeraFsException
        {
            return true;
        }

        @Override
        public boolean isDirectory()
        {
            return true;
        }

        @Override
        public boolean isLink()
        {
            return false;
        }

        @Override
        public FsInode getParent()
        {
            return null;
        }
    }
}
