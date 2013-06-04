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

import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.StringTokenizer;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;

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
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(JdbcFs.class);
    /**
     * the number of pnfs levels. Level zero associated with file real
     * content, which is not our regular case.
     */
    static private final int LEVELS_NUMBER = 7;
    private final FsInode _rootInode;
    private final String _wormID;
    /**
     * SQL query engine
     */
    private final FsSqlDriver _sqlDriver;

    /**
     * Database connection pool
     */
    private final DataSource _dbConnectionsPool;
    private final FsStatCache _fsStatCache;
    /**
     * current fs id
     */
    private final int _fsId;
    /**
     * available space (1 Exabyte)
     */
    private static final long AVAILABLE_SPACE = 1152921504606846976L;
    /**
     * total files
     */
    private static final long TOTAL_FILES = 62914560L;

    public JdbcFs(DataSource dataSource, String dialect) {
        this(dataSource, dialect, 0);
    }

    public JdbcFs(DataSource dataSource, String dialect, int id) {

        _dbConnectionsPool = dataSource;
        _fsId = id;


        // try to get database dialect specific query engine
        _sqlDriver = FsSqlDriver.getDriverInstance(dialect);

        _rootInode = new FsInode(this, "000000000000000000000000000000000000");

        String wormID = null;
        try {
            wormID = getWormID().toString();
        } catch (Exception e) {
        }
        _wormID = wormID;
        _fsStatCache = new FsStatCache(this);
    }

    private FsInode getWormID() throws ChimeraFsException {

        return this.path2inode("/admin/etc/config");
    }

    //////////////////////////////////////////////////////////
    ////
    ////
    ////      Fs operations
    ////
    /////////////////////////////////////////////////////////
    public long usedSpace() throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        long usedSpace = 0;
        try {
            // read only
            dbConnection.setAutoCommit(true);
            usedSpace = _sqlDriver.usedSpace(dbConnection);
        } catch (SQLException se) {
            _log.error("usedSpace: ", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return usedSpace;
    }

    public long usedFiles() throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        long usedFiles = 0;
        try {
            // read only
            dbConnection.setAutoCommit(true);
            usedFiles = _sqlDriver.usedFiles(dbConnection);
        } catch (SQLException se) {
            _log.error("usedFiles: ", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return usedFiles;
    }

    @Override
    public FsInode createLink(String src, String dest) throws ChimeraFsException {

        File file = new File(src);
        return createLink(this.path2inode(file.getParent()), file.getName(), dest);
    }

    @Override
    public FsInode createLink(FsInode parent, String name, String dest) throws ChimeraFsException {
        return createLink(parent, name, 0, 0, 0644, dest.getBytes());
    }

    @Override
    public FsInode createLink(FsInode parent, String name, int uid, int gid, int mode, byte[] dest) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        FsInode inode;
        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            inode = _sqlDriver.createFile(dbConnection, parent, name, uid, gid, mode, UnixPermission.S_IFLNK);
            // link is a regular file where content is a reference
            _sqlDriver.setInodeIo(dbConnection, inode, true);
            _sqlDriver.write(dbConnection, inode, 0, 0, dest, 0, dest.length);

            dbConnection.commit();

        } catch (SQLException se) {
            _log.error("createLink ", se);
            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("createLink rollback ", e);
            }
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return inode;
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

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.createEntryInParent(dbConnection, parent, name, inode);
            _sqlDriver.incNlink(dbConnection, inode);
            _sqlDriver.incNlink(dbConnection, parent);

            dbConnection.commit();

        } catch (SQLException e) {
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("create hlink rollback ", e);
            }
        } finally {
            tryToClose(dbConnection);
        }

        return inode;
    }

    @Override
    public FsInode createFile(String path) throws ChimeraFsException {

        File file = new File(path);

        return this.createFile(this.path2inode(file.getParent()), file.getName());

    }

    @Override
    public FsInode createFile(FsInode parent, String name) throws ChimeraFsException {

        return createFile(parent, name, 0, 0, 0644);
    }

    @Override
    public FsInode createFileLevel(FsInode inode, int level) throws ChimeraFsException {
        return createFileLevel(inode, 0, 0, 0644, level);
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode) throws ChimeraFsException {
        return createFile(parent, name, owner, group, mode, UnixPermission.S_IFREG);
    }

    @Override
    public FsInode createFile(FsInode parent, String name, int owner, int group, int mode, int type) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        FsInode inode = null;

        try {

            if (name.startsWith(".(")) { // special files only

                String[] cmd = PnfsCommandProcessor.process(name);

                if (name.startsWith(".(tag)(") && (cmd.length == 2)) {
                    this.createTag(parent, cmd[1], owner, group, 0644);
                    return new FsInode_TAG(this, parent.toString(), cmd[1]);
                }

                if (name.startsWith(".(pset)(") || name.startsWith(".(fset)(")) {
                    throw new ChimeraFsException("Not supported");
                }

                if (name.startsWith(".(use)(") && (cmd.length == 3)) {
                    FsInode useInode = this.inodeOf(parent, cmd[2]);
                    int level = Integer.parseInt(cmd[1]);

                    try {
                        // read/write only
                        dbConnection.setAutoCommit(false);

                        inode = _sqlDriver.createLevel(dbConnection, useInode, useInode.stat().getUid(), useInode.stat().getGid(),
                                useInode.stat().getMode(), level);
                        dbConnection.commit();

                    } catch (SQLException se) {
                        // according to SQL-92 standard, class-code 23 is
                        // Constraint Violation, in our case
                        // same pool for the same file,
                        // which is OK
                        if (se.getSQLState().startsWith("23")) {
                            throw new FileExistsChimeraFsException(name);
                        }
                        _log.error("create File: ", se);
                        try {
                            dbConnection.rollback();
                        } catch (SQLException e) {
                            _log.error("create File rollback ", e);
                        }
                    }
                }

                if (name.startsWith(".(access)(") && (cmd.length == 3)) {

                    FsInode accessInode = new FsInode(this, cmd[1]);
                    int accessLevel = Integer.parseInt(cmd[2]);
                    if (accessLevel == 0) {
                        inode = accessInode;
                    } else {
                        try {
                            // read/write only
                            dbConnection.setAutoCommit(false);

                            inode = _sqlDriver.createLevel(dbConnection, accessInode,
                                    accessInode.stat().getUid(), accessInode.stat().getGid(),
                                    accessInode.stat().getMode(), accessLevel);
                            dbConnection.commit();

                        } catch (SQLException se) {
                            // according to SQL-92 standard, class-code 23 is
                            // Constraint Violation, in our case file exist
                            if (se.getSQLState().startsWith("23")) {
                                throw new FileExistsChimeraFsException(name);
                            }
                            _log.error("create File: ", se);
                            try {
                                dbConnection.rollback();
                            } catch (SQLException e) {
                                _log.error("create File rollback ", e);
                            }
                        }
                    }
                }

                return inode;
            }

            try {

                if (!parent.exists()) {
                    throw new FileNotFoundHimeraFsException("parent=" + parent.toString());
                }

                if (parent.isDirectory()) {
                    // read/write only
                    dbConnection.setAutoCommit(false);

                    inode = _sqlDriver.createFile(dbConnection, parent, name, owner, group, mode, type);
                    dbConnection.commit();

                } else {
                    throw new NotDirChimeraException(parent);
                }

            } catch (SQLException se) {

                try {
                    dbConnection.rollback();
                } catch (SQLException e) {
                    _log.error("create File rollback ", e);
                }

                if (se.getSQLState().startsWith("23")) {
                    // according to SQL-92 standard, class-code 23 is
                    // Constraint Violation, in our case
                    // file exist
                    throw new FileExistsChimeraFsException();
                }
                _log.error("create File: ", se);
                throw new IOHimeraFsException(se.getMessage());
            }
        } finally {
            tryToClose(dbConnection);
        }


        return inode;
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

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {

            if (!parent.exists()) {
                throw new FileNotFoundHimeraFsException("parent=" + parent.toString());
            }

            if (parent.isDirectory()) {
                // read/write only
                dbConnection.setAutoCommit(false);

                inode = _sqlDriver.createFileWithId(dbConnection, parent, inode, name, owner, group, mode, type);
                dbConnection.commit();

            } else {
                throw new NotDirChimeraException(parent);
            }

        } catch (SQLException se) {

            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("create File rollback ", e);
            }

            if (se.getSQLState().startsWith("23")) {
                // according to SQL-92 standard, class-code 23 is
                // Constraint Violation, in our case
                // file exist
                throw new FileExistsChimeraFsException();
            }
            _log.error("create File: ", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    FsInode createFileLevel(FsInode inode, int owner, int group, int mode, int level) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        // if exist table parent_dir create an entry
        FsInode levelInode = null;
        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            levelInode = _sqlDriver.createLevel(dbConnection, inode, owner, group, mode | UnixPermission.S_IFREG, level);
            dbConnection.commit();

        } catch (SQLException se) {
            _log.error("create level: ", se);
            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("create level rollback ", e);
            }
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return levelInode;
    }

    public String[] listDir(String dir) {
        String[] list = null;

        try {
            list = this.listDir(this.path2inode(dir));
        } catch (Exception e) {
        }

        return list;
    }

    public String[] listDir(FsInode dir) throws IOHimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        String[] list = null;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            list = _sqlDriver.listDir(dbConnection, dir);
        } catch (SQLException se) {
            _log.error("list: ", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return list;
    }

    @Override
    public DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir) throws IOHimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {

            // read only
            dbConnection.setAutoCommit(true);

            return _sqlDriver.newDirectoryStream(dbConnection, dir);

        } catch (SQLException se) {
            _log.error("list full: ", se);
            tryToClose(dbConnection);
            throw new IOHimeraFsException(se.getMessage());
        }
        /*
         * Database resources are close by  DirectoryStreamB.close()
         */
    }

    @Override
    public boolean remove(String path) throws ChimeraFsException {

        FsInode inode = path2inode(path);
        FsInode parent = this.getParentOf(inode);
        if (parent == null) {
            return false;
        }

        File filePath = new File(path);
        String name = filePath.getName();

        return this.remove(parent, name);
    }

    @Override
    public boolean remove(FsInode parent, String name) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        boolean rc = false;

        try {

            FsInode inode = this.inodeOf(parent, name);

            if (inode.type() != FsInodeType.INODE) {
                // now allowed
                return false;
            }

            // read/write only
            dbConnection.setAutoCommit(false);

            rc = _sqlDriver.remove(dbConnection, parent, name);
            if (rc) {
                dbConnection.commit();
            } else {
                dbConnection.rollback();
            }

        } catch (ChimeraFsException hfe) {
            rc = false;
        } catch (SQLException e) {
            _log.error("delete", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("delete rollback", e);
            }
            rc = false;
        } finally {
            tryToClose(dbConnection);
        }

        return rc;
    }

    @Override
    public boolean remove(FsInode inode) throws ChimeraFsException {


        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        boolean rc = false;

        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            FsInode parent = _sqlDriver.getParentOf(dbConnection, inode);
            if (parent == null) {
                return false;
            }

            if (inode.type() != FsInodeType.INODE) {
                // now allowed
                return false;
            }

            if (_sqlDriver.remove(dbConnection, parent, inode)) {
                dbConnection.commit();
                rc = true;
            } else {
                dbConnection.rollback();
                rc = false;
            }
        } catch (ChimeraFsException hfe) {
            rc = false;
        } catch (SQLException e) {
            _log.error("delete", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("delete rollback", e);
            }
            rc = false;
        } finally {
            tryToClose(dbConnection);
        }

        return rc;
    }

    @Override
    public Stat stat(String path) throws ChimeraFsException {
        return this.stat(this.path2inode(path));
    }

    @Override
    public Stat stat(FsInode inode) throws ChimeraFsException {
        return this.stat(inode, 0);
    }

    @Override
    public Stat stat(FsInode inode, int level) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        Stat stat = null;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            stat = _sqlDriver.stat(dbConnection, inode, level);

        } catch (SQLException e) {
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        if (stat == null) {
            throw new FileNotFoundHimeraFsException(inode.toString());
        }

        return stat;
    }

    @Override
    public FsInode mkdir(String path) throws ChimeraFsException {

        int li = path.lastIndexOf('/');
        String file = path.substring(li + 1);
        String dir;
        if (li > 1) {
            dir = path.substring(0, li);
        } else {
            dir = "/";
        }

        return this.mkdir(this.path2inode(dir), file);
    }

    @Override
    public FsInode mkdir(FsInode parent, String name) throws ChimeraFsException {
        return mkdir(parent, name, 0, 0, 0755);
    }

    @Override
    public FsInode mkdir(FsInode parent, String name, int owner, int group, int mode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        FsInode inode = null;

        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            inode = _sqlDriver.mkdir(dbConnection, parent, name, owner, group, mode);
            _sqlDriver.copyTags(dbConnection, parent, inode);
            dbConnection.commit();

        } catch (SQLException se) {

            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("mkdir", se);
            }

            // according to SQL-92 standard, class-code 23 is
            // Constraint Violation, in our case
            // same pool for the same file,
            // which is OK
            if (se.getSQLState().startsWith("23")) {
                throw new FileExistsChimeraFsException(name);
            }
            _log.error("mkdir", se);
            throw new ChimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return inode;
    }

    @Override
    public FsInode path2inode(String path) throws ChimeraFsException {
        return path2inode(path, _rootInode);
    }

    @Override
    public FsInode path2inode(String path, FsInode startFrom) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        FsInode inode = null;

        try {

            dbConnection.setAutoCommit(true);
            inode = _sqlDriver.path2inode(dbConnection, startFrom, path);

            if (inode == null) {
                throw new FileNotFoundHimeraFsException(path);
            }

        } catch (SQLException e) {
            _log.error("path2inode", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return inode;
    }

    @Override
    public List<FsInode> path2inodes(String path) throws ChimeraFsException
    {
        return path2inodes(path, _rootInode);
    }

    @Override
    public List<FsInode> path2inodes(String path, FsInode startFrom)
        throws ChimeraFsException
    {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        List<FsInode> inodes;

        try {
            dbConnection.setAutoCommit(true);
            inodes = _sqlDriver.path2inodes(dbConnection, startFrom, path);

            if (inodes.isEmpty()) {
                throw new FileNotFoundHimeraFsException(path);
            }
        } catch (SQLException e) {
            _log.error("path2inode", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return inodes;
    }

    @Override
    public FsInode inodeOf(FsInode parent, String name) throws ChimeraFsException {
        FsInode inode = null;

        // only if it's PNFS command
        if (name.startsWith(".(")) {

            if (name.startsWith(".(id)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                inode = inodeOf(parent, cmd[1]);

                return new FsInode_ID(this, inode.toString());

            }

            if (name.startsWith(".(use)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                try {
                    int level = Integer.parseInt(cmd[1]);

                    FsInode useInode = this.inodeOf(parent, cmd[2]);

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
                FsInode constInode = new FsInode_CONST(this, cmd[1]);
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
                FsInode pgetInode = new FsInode_PGET(this, parent.toString(), new String[0]);
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

                /*
                 * pass in the name too (args 1 to n)
                 */
                String[] args = new String[cmd.length - 1];
                System.arraycopy(cmd, 1, args, 0, args.length);
                inode = getPGET(parent, args);
                if (!inode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return inode;
            }

            if (name.equals(".(config)")) {
                return new FsInode(this, _wormID);
            }

            if (name.startsWith(".(config)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length != 2) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return this.inodeOf(new FsInode(this, _wormID), cmd[1]);
            }

            if (name.startsWith(".(fset)(")) {
                String[] cmd = PnfsCommandProcessor.process(name);
                if (cmd.length < 3) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                String[] args = new String[cmd.length - 2];
                System.arraycopy(cmd, 2, args, 0, args.length);

                FsInode fsetInode = this.inodeOf(parent, cmd[1]);
                if (!fsetInode.exists()) {
                    throw new FileNotFoundHimeraFsException(name);
                }
                return new FsInode_PSET(this, fsetInode.toString(), args);
            }

        }

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read only
            dbConnection.setAutoCommit(true);

            inode = _sqlDriver.inodeOf(dbConnection, parent, name);

            if (inode == null) {
                throw new FileNotFoundHimeraFsException(name);
            }

        } catch (SQLException e) {
            _log.error("inodeOf", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        inode.setParent(parent);

        return inode;
    }

    @Override
    public String inode2path(FsInode inode) throws ChimeraFsException {
        return inode2path(inode, _rootInode, true);
    }

    /**
     *
     * @param inode
     * @param startFrom
     * @return path of inode starting from startFrom
     * @throws ChimeraFsException
     */
    @Override
    public String inode2path(FsInode inode, FsInode startFrom, boolean inclusive) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        String path = null;

        try {
            // read only
            dbConnection.setAutoCommit(true);

            path = _sqlDriver.inode2path(dbConnection, inode, startFrom, inclusive);

        } catch (SQLException e) {
            _log.error("inode2path", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return path;

    }

    @Override
    public boolean removeFileMetadata(String path, int level) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        boolean rc = false;

        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            rc = _sqlDriver.removeInodeLevel(dbConnection, this.path2inode(path), level);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("removeFileMetadata", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("removeFileMetadata rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return rc;
    }

    @Override
    public FsInode getParentOf(FsInode inode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        FsInode parent = null;

        try {
            // read only
            dbConnection.setAutoCommit(true);

            if (inode.isDirectory()) {
                parent = _sqlDriver.getParentOfDirectory(dbConnection, inode);
            } else {
                parent = _sqlDriver.getParentOf(dbConnection, inode);
            }

        } catch (SQLException e) {
            _log.error("getPathOf", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return parent;
    }

    @Override
    public void setFileSize(FsInode inode, long newSize) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileSize(dbConnection, inode, newSize);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileSize", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileSize rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

    }

    @Override
    public void setFileOwner(FsInode inode, int newOwner) throws ChimeraFsException {
        setFileOwner(inode, 0, newOwner);
    }

    @Override
    public void setFileOwner(FsInode inode, int level, int newOwner) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileOwner(dbConnection, inode, level, newOwner);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileOwner", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileOwner rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setFileName(FsInode dir, String oldName, String newName) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileName(dbConnection, dir, oldName, newName);
            dbConnection.commit();
        } catch (Exception e) {
            _log.error("setFileName", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileName rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setInodeAttributes(FsInode inode, int level, Stat stat) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            switch (inode.type()) {
                case INODE:
                    _sqlDriver.setInodeAttributes(dbConnection, inode, level, stat);
                    break;
                case TAG:
                    _sqlDriver.setTagMode(dbConnection, (FsInode_TAG) inode, stat.getMode());
                    _sqlDriver.setTagOwner(dbConnection, (FsInode_TAG) inode, stat.getUid());
                    _sqlDriver.setTagOwnerGroup(dbConnection, (FsInode_TAG) inode, stat.getGid());
                    break;
            }
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setInodeAttributes", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setInodeAttributes rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setFileATime(FsInode inode, long atime) throws ChimeraFsException {
        setFileATime(inode, 0, atime);
    }

    @Override
    public void setFileATime(FsInode inode, int level, long atime) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileATime(dbConnection, inode, level, atime);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileATime", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileATime rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setFileCTime(FsInode inode, long ctime) throws ChimeraFsException {
        setFileCTime(inode, 0, ctime);
    }

    @Override
    public void setFileCTime(FsInode inode, int level, long ctime) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileCTime(dbConnection, inode, level, ctime);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileCTime", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileCTime rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

    }

    @Override
    public void setFileMTime(FsInode inode, long mtime) throws ChimeraFsException {
        setFileMTime(inode, 0, mtime);
    }

    @Override
    public void setFileMTime(FsInode inode, int level, long mtime) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileMTime(dbConnection, inode, level, mtime);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileMTime", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileMTime rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

    }

    @Override
    public void setFileGroup(FsInode inode, int newGroup) throws ChimeraFsException {
        setFileGroup(inode, 0, newGroup);
    }

    @Override
    public void setFileGroup(FsInode inode, int level, int newGroup) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileGroup(dbConnection, inode, level, newGroup);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileGroup", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileGroup rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

    }

    @Override
    public void setFileMode(FsInode inode, int newMode) throws ChimeraFsException {
        setFileMode(inode, 0, newMode);
    }

    @Override
    public void setFileMode(FsInode inode, int level, int newMode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setFileMode(dbConnection, inode, level, newMode);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("setFileMode", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setFileMode rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public boolean isIoEnabled(FsInode inode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        boolean ioEnabled = false;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            ioEnabled = _sqlDriver.isIoEnabled(dbConnection, inode);

        } catch (SQLException e) {
            _log.error("isIoEnabled", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return ioEnabled;

    }

    @Override
    public void setInodeIo(FsInode inode, boolean enable) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setInodeIo(dbConnection, inode, enable);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("setInodeIo", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setInodeIo rollback", e1);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    public int write(FsInode inode, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        return this.write(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int write(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {


        if (level == 0 && !inode.isIoEnabled()) {
            _log.debug(inode + ": IO (write) not allowd");
            return -1;
        }

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.write(dbConnection, inode, level, beginIndex, data, offset, len);
            dbConnection.commit();
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("write rollback", e);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }
            _log.error("write", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return len;
    }

    public int read(FsInode inode, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {
        return this.read(inode, 0, beginIndex, data, offset, len);
    }

    @Override
    public int read(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) throws ChimeraFsException {

        int count = -1;

        if (level == 0 && !inode.isIoEnabled()) {
            _log.debug(inode + ": IO(read) not allowd");
            return -1;
        }

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read only
            dbConnection.setAutoCommit(true);

            count = _sqlDriver.read(dbConnection, inode, level, beginIndex, data, offset, len);

        } catch (SQLException se) {
            _log.debug("read:", se);
            throw new IOHimeraFsException(se.getMessage());
        } catch (IOException e) {
            _log.debug("read IO:", e);
        } finally {
            tryToClose(dbConnection);
        }

        return count;
    }

    @Override
    public byte[] readLink(String path) throws ChimeraFsException {
        return this.readLink(this.path2inode(path));
    }

    @Override
    public byte[] readLink(FsInode inode) throws ChimeraFsException {

        byte[] link;
        byte[] b = new byte[(int) inode.statCache().getSize()];

        int n = this.read(inode, 0, b, 0, b.length);
        if (n >= 0) {
            link = b;
        } else {
            link = new byte[0];
        }

        return link;
    }

    @Override
    public boolean move(String source, String dest) {
        boolean rc;

        try {

            File what = new File(source);
            File where = new File(dest);

            rc = this.move(this.path2inode(what.getParent()), what.getName(), this.path2inode(where.getParent()), where.getName());

        } catch (Exception e) {
            rc = false;
        }

        return rc;
    }

    @Override
    public boolean move(FsInode srcDir, String source, FsInode destDir, String dest) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        boolean rc = false;
        try {

            // read/write only
            dbConnection.setAutoCommit(false);

            if (!srcDir.equals(destDir)) {
                _sqlDriver.move(dbConnection, srcDir, source, destDir, dest);
            } else {
                // same directory
                _sqlDriver.setFileName(dbConnection, srcDir, source, dest);
            }

            dbConnection.commit();
            rc = true;
        } catch (SQLException e) {
            _log.error("move:", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("move rollback:", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return rc;
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Location info
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public List<StorageLocatable> getInodeLocations(FsInode inode, int type) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        List<StorageLocatable> locations = null;

        try {

            // read/write only
            dbConnection.setAutoCommit(true);

            locations = _sqlDriver.getInodeLocations(dbConnection, inode, type);

        } catch (SQLException se) {
            _log.error("getInodeLocations", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return locations;
    }

    @Override
    public void addInodeLocation(FsInode inode, int type, String location) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.addInodeLocation(dbConnection, inode, type, location);
            dbConnection.commit();
        } catch (SQLException se) {
            String sqlState = se.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("addInodeLocation rollback ", e);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }

            if (_sqlDriver.isDuplicatedKeyError(sqlState)) {
                // OK
            } else {
                _log.error("addInodeLocation:  [" + sqlState + "]", se);
                throw new IOHimeraFsException(se.getMessage());
            }
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void clearInodeLocation(FsInode inode, int type, String location) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.clearInodeLocation(dbConnection, inode, type, location);
            dbConnection.commit();
        } catch (SQLException se) {
            _log.error("clearInodeLocation", se);
            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("clearInodeLocation rollback ", se);
            }
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    /////////////////////////////////////////////////////////////////////
    ////
    ////   Directory tags handling
    ////
    ////////////////////////////////////////////////////////////////////
    @Override
    public String[] tags(FsInode inode) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        String[] list = null;
        try {
            // read/write only
            dbConnection.setAutoCommit(true);

            list = _sqlDriver.tags(dbConnection, inode);

        } catch (SQLException se) {
            _log.error("tags", se);
            throw new IOHimeraFsException(se.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return list;
    }

    @Override
    public void createTag(FsInode inode, String name) throws ChimeraFsException {
        this.createTag(inode, name, 0, 0, 0644);
    }

    @Override
    public void createTag(FsInode inode, String name, int uid, int gid, int mode) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.createTag(dbConnection, inode, name, uid, gid, mode);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("createTag", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("createTag rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public int setTag(FsInode inode, String tagName, byte[] data, int offset, int len) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setTag(dbConnection, inode, tagName, data, offset, len);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("setTag", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setTag rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } catch (ChimeraFsException e) {
            _log.error("setTag", e);
        } finally {
            tryToClose(dbConnection);
        }

        return len;

    }

    @Override
    public void removeTag(FsInode dir, String tagName) throws ChimeraFsException
    {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.removeTag(dbConnection, dir, tagName);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("removeTag", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("removeTag rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void removeTag(FsInode dir) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.removeTag(dbConnection, dir);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("removeTag", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("removeTag rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public int getTag(FsInode inode, String tagName, byte[] data, int offset, int len) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        int count = -1;

        try {
            // read only
            dbConnection.setAutoCommit(true);

            count = _sqlDriver.getTag(dbConnection, inode, tagName, data, offset, len);

        } catch (SQLException e) {

            _log.error("getTag", e);
            throw new IOHimeraFsException(e.getMessage());
        } catch (IOException e) {
            _log.error("getTag io", e);
        } finally {
            tryToClose(dbConnection);
        }

        return count;
    }

    @Override
    public Stat statTag(FsInode dir, String name) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }


        Stat ret = null;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            ret = _sqlDriver.statTag(dbConnection, dir, name);

        } catch (SQLException e) {
            _log.error("statTag", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return ret;
    }

    @Override
    public void setTagOwner(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(false);

            _sqlDriver.setTagOwner(dbConnection, tagInode, owner);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("setTagOwner", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setTagOwner rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setTagOwnerGroup(FsInode_TAG tagInode, String name, int owner) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(false);

            _sqlDriver.setTagOwnerGroup(dbConnection, tagInode, owner);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("setTagOwnerGroup", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setTagOwnerGroup rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setTagMode(FsInode_TAG tagInode, String name, int mode) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(false);

            _sqlDriver.setTagMode(dbConnection, tagInode, mode);
            dbConnection.commit();
        } catch (SQLException e) {
            _log.error("setTagMode", e);
            try {
                dbConnection.rollback();
            } catch (SQLException e1) {
                _log.error("setTagMode rollback", e);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
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
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setStorageInfo(dbConnection, inode, storageInfo);
            dbConnection.commit();
        } catch (SQLException se) {
            String sqlState = se.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException e) {
                _log.error("setStorageInfo rollback ", e);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }

            if (_sqlDriver.isDuplicatedKeyError(sqlState)) {
                // OK
            } else {
                _log.error("setStorageInfo:  [" + sqlState + "]", se);
                throw new IOHimeraFsException(se.getMessage());
            }
        } finally {
            tryToClose(dbConnection);
        }
    }

    /**
     *
     * @param inode
     * @param accessLatency
     * @throws ChimeraFsException
     */
    @Override
    public void setAccessLatency(FsInode inode, AccessLatency accessLatency) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setAccessLatency(dbConnection, inode, accessLatency);
            dbConnection.commit();
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException ee) {
                _log.error("setAccessLatensy rollback ", ee);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }
            _log.error("setAccessLatency:  [" + sqlState + "]", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public void setRetentionPolicy(FsInode inode, RetentionPolicy retentionPolicy) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            // read/write only
            dbConnection.setAutoCommit(false);

            _sqlDriver.setRetentionPolicy(dbConnection, inode, retentionPolicy);
            dbConnection.commit();
        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException ee) {
                _log.error("setRetentionPolicy rollback ", ee);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }
            _log.error("setRetentionPolicy:  [" + sqlState + "]", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public InodeStorageInformation getStorageInfo(FsInode inode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        InodeStorageInformation storageInfo = null;

        try {

            dbConnection.setAutoCommit(true);

            storageInfo = _sqlDriver.getStorageInfo(dbConnection, inode);

        } catch (SQLException e) {
            _log.error("setSorageInfo", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return storageInfo;

    }

    @Override
    public AccessLatency getAccessLatency(FsInode inode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        AccessLatency accessLatency = null;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            accessLatency = _sqlDriver.getAccessLatency(dbConnection, inode);

        } catch (SQLException e) {
            _log.error("setSorageInfo", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return accessLatency;

    }

    @Override
    public RetentionPolicy getRetentionPolicy(FsInode inode) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        RetentionPolicy retentionPolicy = null;

        try {

            // read only
            dbConnection.setAutoCommit(true);

            retentionPolicy = _sqlDriver.getRetentionPolicy(dbConnection, inode);

        } catch (SQLException e) {
            _log.error("setSorageInfo", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return retentionPolicy;

    }

    /*
     * inode checksum handling
     */
    @Override
    public void setInodeChecksum(FsInode inode, int type, String checksum) throws ChimeraFsException {

        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }


        try {
            dbConnection.setAutoCommit(false);

            _sqlDriver.setInodeChecksum(dbConnection, inode, type, checksum);

            dbConnection.commit();

        } catch (SQLException e) {
            String sqlState = e.getSQLState();
            try {
                dbConnection.rollback();
            } catch (SQLException ee) {
                _log.error("setInodeChecksum rollback ", ee);
            }

            if (_sqlDriver.isForeignKeyError(sqlState)) {
                throw new FileNotFoundHimeraFsException();
            }

            if (_sqlDriver.isDuplicatedKeyError(sqlState)) {
                // OK
            } else {
                _log.error("setInodeChecksum:  [" + sqlState + "]", e);
                throw new IOHimeraFsException(e.getMessage());
            }
        } finally {
            tryToClose(dbConnection);
        }

    }

    @Override
    public void removeInodeChecksum(FsInode inode, int type) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(true);

            _sqlDriver.removeInodeChecksum(dbConnection, inode, type);

        } catch (SQLException e) {
            _log.error("removeInodeChecksum", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    @Override
    public String getInodeChecksum(FsInode inode, int type) throws ChimeraFsException {
        String checkSum = null;
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(true);

            checkSum = _sqlDriver.getInodeChecksum(dbConnection, inode, type);

        } catch (SQLException e) {
            _log.error("getInodeChecksum", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }

        return checkSum;
    }

    /**
     * Get inode's Access Control List. An empty list is returned if there are no ACL assigned
     * to the <code>inode</code>.
     * @param inode
     * @return acl
     */
    @Override
    public List<ACE> getACL(FsInode inode) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        List<ACE> acl;
        try {
            dbConnection.setAutoCommit(true);

            acl = _sqlDriver.getACL(dbConnection, inode);

        } catch (SQLException e) {
            _log.error("Failed go getACL:", e);
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
        return acl;
    }

    /**
     * Set inode's Access Control List. The existing ACL will be replaced.
     * @param dbConnection
     * @param inode
     * @param acl
     */
    @Override
    public void setACL(FsInode inode, List<ACE> acl) throws ChimeraFsException {
        Connection dbConnection;
        try {
            // get from pool
            dbConnection = _dbConnectionsPool.getConnection();
        } catch (SQLException e) {
            throw new BackEndErrorHimeraFsException(e.getMessage());
        }

        try {
            dbConnection.setAutoCommit(false);

            _sqlDriver.setACL(dbConnection, inode, acl);
            dbConnection.commit();

        } catch (SQLException e) {
            _log.error("Failed to set ACL: ", e);
            try {
                dbConnection.rollback();
            } catch (SQLException ee) {
                _log.error("setACL rollback ", ee);
            }
            throw new IOHimeraFsException(e.getMessage());
        } finally {
            tryToClose(dbConnection);
        }
    }

    /**
     * internal class to hide caching mechanism.
     */
    static class FsStatCache {

        private FsStat _fsStatCached;
        private long _fsStatLastUpdate;
        // FIXME: make it configurable
        private long _fsStateLifetime = 3600000;
        private final JdbcFs _fs;

        FsStatCache(JdbcFs fs) {
            _fs = fs;
        }

        public synchronized FsStat getFsStat() throws ChimeraFsException {

            if (_fsStatLastUpdate == 0 || _fsStatLastUpdate + _fsStateLifetime < System.currentTimeMillis()) {
                _fsStatCached = new FsStat(AVAILABLE_SPACE,
                                           TOTAL_FILES,
                                           _fs.usedSpace(),
                                           _fs.usedFiles());
                _log.debug("updateing cached value of FsStat");
                _fsStatLastUpdate = System.currentTimeMillis();
            } else {
                _log.debug("using cached value of FsStat");
            }

            return _fsStatCached;
        }
    }

    @Override
    public FsStat getFsStat() throws ChimeraFsException {
        return _fsStatCache.getFsStat();
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

        sb.append("DB        : ").append(_dbConnectionsPool.toString()).append("\n");
        sb.append("DB Engine : ").append(databaseProductName).append(" ").append(databaseProductVersion).append("\n");
        sb.append("rootID    : ").append(_rootInode.toString()).append("\n");
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
        if (_dbConnectionsPool instanceof BoneCPDataSource) {
            ((BoneCPDataSource) _dbConnectionsPool).close();
        } else if (_dbConnectionsPool instanceof Closeable) {
            ((Closeable) _dbConnectionsPool).close();
        }
    }

    @Override
    public FsInode inodeFromBytes(byte[] handle) throws ChimeraFsException {

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

                case PGET:
                    id = st.nextToken();
                    argc = st.countTokens();
                    args = new String[argc];
                    for (int i = 0; i < argc; i++) {
                        args[i] = st.nextToken();
                    }
                    inode = getPGET(id, args);
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
        return inode.toFullString().getBytes();
    }

    /**
     * So that subclasses can do something different (like caching).
     */
    protected FsInode_PGET getPGET(String id, String[] args)
                    throws ChimeraFsException {
        return new FsInode_PGET(this, id, args);
    }

    /**
     * So that subclasses can do something different (like caching).
     */
    protected FsInode_PGET getPGET(FsInode parent, String[] args)
                    throws ChimeraFsException {
        return new FsInode_PGET(this, parent.toString(), args);
    }
}
