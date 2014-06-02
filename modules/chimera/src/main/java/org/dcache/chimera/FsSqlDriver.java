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

import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.commons.util.SqlHelper;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

/**
 * SQL driver
 *
 *
 */
class FsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(FsSqlDriver.class);
    /**
     * default file IO mode
     */
    private final static int IOMODE_ENABLE = 1;
    private final static int IOMODE_DISABLE = 0;
    public static final String DUPLICATE_KEY_ERROR = "23505";
    public static final String FOREIGN_KEY_ERROR = "23503";
    private final int _ioMode;

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected FsSqlDriver() {

        if (Boolean.valueOf(System.getProperty("chimera.inodeIoMode"))) {
            _ioMode = IOMODE_ENABLE;
        } else {
            _ioMode = IOMODE_DISABLE;
        }

    }
    private static final String sqlUsedSpace = "SELECT SUM(isize) AS usedSpace FROM t_inodes WHERE itype=32768";

    /**
     *
     * @param dbConnection
     * @return total space used by files
     * @throws SQLException
     */
    long usedSpace(Connection dbConnection) throws SQLException {
        long usedSpace = 0;
        PreparedStatement stUsedSpace = null;
        ResultSet rs = null;
        try {

            stUsedSpace = dbConnection.prepareStatement(sqlUsedSpace);

            rs = stUsedSpace.executeQuery();
            if (rs.next()) {
                usedSpace = rs.getLong("usedSpace");
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stUsedSpace);
        }

        return usedSpace;
    }
    private static final String sqlUsedFiles = "SELECT count(ipnfsid) AS usedFiles FROM t_inodes WHERE itype=32768";

    /**
     *
     * @param dbConnection
     * @return total number of files
     * @throws SQLException
     */
    long usedFiles(Connection dbConnection) throws SQLException {
        long usedFiles = 0;
        PreparedStatement stUsedFiles = null;
        ResultSet rs = null;
        try {

            stUsedFiles = dbConnection.prepareStatement(sqlUsedFiles);

            rs = stUsedFiles.executeQuery();
            if (rs.next()) {
                usedFiles = rs.getLong("usedFiles");
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stUsedFiles);
        }

        return usedFiles;
    }

    /**
     *
     *  creates a new inode and an entry name in parent directory.
     * Parent reference count and modification time is updated.
     *
     * @param dbConnection
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws ChimeraFsException
     * @throws SQLException
     * @return
     */
    FsInode createFile(Connection dbConnection, FsInode parent, String name, int owner, int group, int mode, int type) throws
                                                                                                                       SQLException {

        FsInode inode;

        inode = new FsInode(parent.getFs());
        createFileWithId(dbConnection, parent, inode, name, owner, group, mode, type);

        return inode;
    }

    /**
     *
     *  Creates a new entry with given inode is in parent directory.
     * Parent reference count and modification time is updated.
     *
     * @param dbConnection
     * @param inode
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @throws SQLException
     * @return
     */
    FsInode createFileWithId(Connection dbConnection, FsInode parent, FsInode inode, String name, int owner, int group, int mode, int type) throws
                                                                                                                                            SQLException {

        createInode(dbConnection, inode, type, owner, group, mode, 1);
        createEntryInParent(dbConnection, parent, name, inode);
        incNlink(dbConnection, parent);

        return inode;
    }
    private static final String sqlListDir = "SELECT * FROM t_dirs WHERE iparent=?";

    /**
     * returns list of files in the directory. If there is no entries,
     * empty list is returned. inode is not tested to be a directory
     *
     * @param dbConnection
     * @param dir
     * @throws SQLException
     * @return
     */
    String[] listDir(Connection dbConnection, FsInode dir) throws SQLException {

        String[] list = null;
        ResultSet result = null;
        PreparedStatement stListDirectory = null;

        try {

            stListDirectory = dbConnection.prepareStatement(sqlListDir);
            stListDirectory.setString(1, dir.toString());
            stListDirectory.setFetchSize(1000);
            result = stListDirectory.executeQuery();


            List<String> directoryList = new ArrayList<>();
            while (result.next()) {
                directoryList.add(result.getString("iname"));
            }

            list = directoryList.toArray(new String[directoryList.size()]);
        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(stListDirectory);
        }

        return list;
    }
    private static final String sqlListDirFull = "SELECT "
            + "t_inodes.ipnfsid, t_dirs.iname, t_inodes.isize,t_inodes.inlink,t_inodes.imode,t_inodes.itype,t_inodes.iuid,t_inodes.igid,t_inodes.iatime,t_inodes.ictime,t_inodes.imtime  "
            + "FROM t_inodes, t_dirs WHERE iparent=? AND t_inodes.ipnfsid = t_dirs.ipnfsid";

    /**
     * the same as listDir, but array of {@HimeraDirectoryEntry} is returned, which contains
     * file attributes as well.
     *
     * @param dbConnection
     * @param dir
     * @throws SQLException
     * @return
     */
    DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(Connection dbConnection, FsInode dir) throws SQLException {

        ResultSet result;
        PreparedStatement stListDirectoryFull;

        stListDirectoryFull = dbConnection.prepareStatement(sqlListDirFull);
        stListDirectoryFull.setFetchSize(50);
        stListDirectoryFull.setString(1, dir.toString());

        result = stListDirectoryFull.executeQuery();
        return new DirectoryStreamImpl(dir, dbConnection, stListDirectoryFull, result);
        /*
         * DB resources freed by
         * DirectoryStreamB.close()
         */
    }

    void remove(Connection dbConnection, FsInode parent, String name) throws ChimeraFsException, SQLException {

        FsInode inode = inodeOf(dbConnection, parent, name);
        if (inode == null || inode.type() != FsInodeType.INODE) {
            throw new FileNotFoundHimeraFsException("Not a file.");
        }

        if (inode.isDirectory()) {
            removeDir(dbConnection, parent, inode, name);
        } else {
            removeFile(dbConnection, parent, inode, name);
        }
    }

    private void removeDir(Connection dbConnection, FsInode parent, FsInode inode, String name) throws ChimeraFsException, SQLException {

        Stat dirStat = inode.statCache();
        if (dirStat.getNlink() > 2) {
            throw new DirNotEmptyHimeraFsException("directory is not empty");
        }

        removeEntryInParent(dbConnection, inode, ".");
        removeEntryInParent(dbConnection, inode, "..");
        // decrease reference count ( '.' , '..', and in parent directory ,
        // and inode itself)
        decNlink(dbConnection, inode, 2);
        removeTag(dbConnection, inode);

        removeEntryInParent(dbConnection, parent, name);
        decNlink(dbConnection, parent);

        removeInode(dbConnection, inode);
    }

    private void removeFile(Connection dbConnection, FsInode parent, FsInode inode, String name) throws ChimeraFsException, SQLException {

        boolean isLast = inode.stat().getNlink() == 1;

        decNlink(dbConnection, inode);
        removeEntryInParent(dbConnection, parent, name);

        if (isLast) {
            removeInode(dbConnection, inode);
        }

        /* During bulk deletion of files in the same directory,
         * updating the parent inode is often a contention point. The
         * link count on the parent is updated last to reduce the time
         * in which the directory inode is locked by the database.
         */
        decNlink(dbConnection, parent);
    }

    void remove(Connection dbConnection, FsInode parent, FsInode inode) throws ChimeraFsException, SQLException {

        if (inode.isDirectory()) {

            Stat dirStat = inode.statCache();
            if (dirStat.getNlink() > 2) {
                throw new DirNotEmptyHimeraFsException("directory is not empty");
            }
            removeEntryInParent(dbConnection, inode, ".");
            removeEntryInParent(dbConnection, inode, "..");
            // decrease reference count ( '.' , '..', and in parent directory ,
            // and inode itself)
            decNlink(dbConnection, inode, 2);
            removeTag(dbConnection, inode);

        } else {
            decNlink(dbConnection, inode);

            /*
             * TODO: put into trash
             */
            for (int i = 1; i <= 7; i++) {
                removeInodeLevel(dbConnection, inode, i);
            }
        }

        removeEntryInParentByID(dbConnection, parent, inode);
        decNlink(dbConnection, parent);

        setFileMTime(dbConnection, parent, 0, System.currentTimeMillis());
        removeStorageInfo(dbConnection, inode);

        removeInode(dbConnection, inode);
    }

    public Stat stat(Connection dbConnection, FsInode inode) throws SQLException {
        return stat(dbConnection, inode, 0);
    }
    private static final String sqlStat = "SELECT isize,inlink,itype,imode,iuid,igid,iatime,ictime,imtime,icrtime,igeneration FROM t_inodes WHERE ipnfsid=?";

    public Stat stat(Connection dbConnection, FsInode inode, int level) throws SQLException {

        Stat ret = null;
        PreparedStatement stStatInode = null;
        ResultSet statResult = null;
        try {

            if (level == 0) {
                stStatInode = dbConnection.prepareStatement(sqlStat);

            } else {
                stStatInode = dbConnection.prepareStatement("SELECT isize,inlink,imode,iuid,igid,iatime,ictime,imtime FROM t_level_" + level + " WHERE ipnfsid=?");
            }

            stStatInode.setString(1, inode.toString());
            statResult = stStatInode.executeQuery();

            if (statResult.next()) {
                ret = new Stat();
                int inodeType;

                if (level == 0) {
                    inodeType = statResult.getInt("itype");
                    ret.setCrTime(statResult.getTimestamp("icrtime").getTime());
                    ret.setGeneration(statResult.getLong("igeneration"));
                } else {
                    inodeType = UnixPermission.S_IFREG;
                    ret.setCrTime(statResult.getTimestamp("imtime").getTime());
                    ret.setGeneration(0);
                }

                ret.setSize(statResult.getLong("isize"));
                ret.setATime(statResult.getTimestamp("iatime").getTime());
                ret.setCTime(statResult.getTimestamp("ictime").getTime());
                ret.setMTime(statResult.getTimestamp("imtime").getTime());
                ret.setUid(statResult.getInt("iuid"));
                ret.setGid(statResult.getInt("igid"));
                ret.setMode(statResult.getInt("imode") | inodeType);
                ret.setNlink(statResult.getInt("inlink"));
                ret.setIno((int) inode.id());
                ret.setDev(17);
            }

        } finally {
            SqlHelper.tryToClose(statResult);
            SqlHelper.tryToClose(stStatInode);
        }

        return ret;
    }

    /**
     * create a new directory in parent with name. The reference count if parent directory
     * as well modification time and reference count of newly created directory are updated.
     *
     *
     * @param dbConnection
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @throws ChimeraFsException
     * @throws SQLException
     * @return
     */
    FsInode mkdir(Connection dbConnection, FsInode parent, String name, int owner, int group, int mode) throws ChimeraFsException, SQLException {

        // if exist table parent_dir create an entry

        FsInode inode;

        if (parent.isDirectory()) {

            inode = new FsInode(parent.getFs());

            // as soon as directory is created nlink == 2
            createInode(dbConnection, inode, UnixPermission.S_IFDIR, owner, group, mode, 2);
            createEntryInParent(dbConnection, parent, name, inode);

            // increase parent nlink only
            incNlink(dbConnection, parent);

            createEntryInParent(dbConnection, inode, ".", inode);
            createEntryInParent(dbConnection, inode, "..", parent);

        } else {
            throw new NotDirChimeraException(parent);
        }

        return inode;
    }

    FsInode mkdir(Connection dbConnection, FsInode parent, String name, int owner, int group, int mode,
                  Map<String,byte[]> tags) throws ChimeraFsException, SQLException
    {
        FsInode inode = mkdir(dbConnection, parent, name, owner, group, mode);
        createTags(dbConnection, inode, owner, group, mode & 0666, tags);
        return inode;
    }


    private static final String sqlMove = "UPDATE t_dirs SET iparent=?, iname=? WHERE iparent=? AND iname=?";
    private static final String sqlSetParent = "UPDATE t_dirs SET ipnfsid=? WHERE iparent=? AND iname='..'";

    /**
     * move source from srcDir into dest in destDir.
     * The reference counts if srcDir and destDir is updates.
     *
     * @param dbConnection
     * @param srcDir
     * @param source
     * @param destDir
     * @param dest
     * @throws SQLException
     */
    void move(Connection dbConnection, FsInode srcDir, String source, FsInode destDir, String dest) throws SQLException, ChimeraFsException {

        PreparedStatement stMove = null;
        PreparedStatement stParentMove = null;

        try {

            FsInode srcInode = inodeOf(dbConnection, srcDir, source);
            stMove = dbConnection.prepareStatement(sqlMove);

            stMove.setString(1, destDir.toString());
            stMove.setString(2, dest);
            stMove.setString(3, srcDir.toString());
            stMove.setString(4, source);
            stMove.executeUpdate();

            /*
             * if moving a directory, point '..' to the new parent
             */
            Stat stat = stat(dbConnection, srcInode);
            if ( (stat.getMode() & UnixPermission.F_TYPE) == UnixPermission.S_IFDIR) {
                stParentMove = dbConnection.prepareStatement(sqlSetParent);
                stParentMove.setString(1, destDir.toString());
                stParentMove.setString(2, srcInode.toString());
                stParentMove.executeUpdate();
            }

        } finally {
            SqlHelper.tryToClose(stMove);
            SqlHelper.tryToClose(stParentMove);
        }

    }
    private static final String sqlInodeOf = "SELECT ipnfsid FROM t_dirs WHERE iname=? AND iparent=?";

    /**
     * return the inode of path in directory. In case of pnfs magic commands ( '.(' )
     * command specific inode is returned.
     *
     * @param dbConnection
     * @param parent
     * @param name
     * @throws SQLException
     * @return null if path is not found
     */
    FsInode inodeOf(Connection dbConnection, FsInode parent, String name) throws SQLException {

        FsInode inode = null;
        String id = null;
        PreparedStatement stGetInodeByName = null;

        ResultSet result = null;
        try {

            stGetInodeByName = dbConnection.prepareStatement(sqlInodeOf);
            stGetInodeByName.setString(1, name);
            stGetInodeByName.setString(2, parent.toString());

            result = stGetInodeByName.executeQuery();

            if (result.next()) {
                id = result.getString("ipnfsid");
            }

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(stGetInodeByName);
        }

        if (id != null) {
            inode = new FsInode(parent.getFs(), id);
        }
        return inode;
    }
    private static final String sqlInode2Path_name = "SELECT iname FROM t_dirs WHERE ipnfsid=? AND iparent=? and iname !='.' and iname != '..'";
    private static final String sqlInode2Path_inode = "SELECT iparent FROM t_dirs WHERE ipnfsid=?  and iname != '.' and iname != '..'";

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param dbConnection
     * @param inode
     * @param startFrom defined the "root"
     * @throws SQLException
     * @return
     */
    String inode2path(Connection dbConnection, FsInode inode, FsInode startFrom, boolean inclusive) throws SQLException {

        if (inode.equals(startFrom)) {
            return "/";
        }

        String path = null;
        PreparedStatement ps = null;
        try {

            List<String> pList = new ArrayList<>();
            String parentId = getParentOf(dbConnection, inode).toString();
            String elementId = inode.toString();

            boolean done = false;
            do {

                ps = dbConnection.prepareStatement(sqlInode2Path_name);
                ps.setString(1, elementId);
                ps.setString(2, parentId);

                ResultSet pSearch = ps.executeQuery();
                if (pSearch.next()) {
                    pList.add(pSearch.getString("iname"));
                }
                elementId = parentId;

                SqlHelper.tryToClose(ps);
                if (inclusive && elementId.equals(startFrom.toString())) {
                    done = true;
                }

                ps = dbConnection.prepareStatement(sqlInode2Path_inode);
                ps.setString(1, parentId);

                pSearch = ps.executeQuery();

                if (pSearch.next()) {
                    parentId = pSearch.getString("iparent");
                }
                ps.close();

                if (!inclusive && parentId.equals(startFrom.toString())) {
                    done = true;
                }
            } while (!done);


            StringBuilder sb = new StringBuilder();

            for (int i = pList.size(); i > 0; i--) {
                sb.append("/").append(pList.get(i - 1));
            }

            path = sb.toString();

        } finally {
            SqlHelper.tryToClose(ps);
        }

        return path;
    }
    private static final String sqlCreateInode = "INSERT INTO t_inodes VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)";

    /**
     *
     * creates an entry in t_inodes table with initial values.
     * for optimization, initial value of reference count may be defined.
     * for newly created files , file size is zero. For directories 512.
     *
     * @param dbConnection
     * @param inode
     * @param uid
     * @param gid
     * @param mode
     * @param nlink
     * @throws SQLException
     */
    public void createInode(Connection dbConnection, FsInode inode, int type, int uid, int gid, int mode, int nlink) throws SQLException {

        PreparedStatement stCreateInode = null;

        try {

            // default inode - nlink =1, size=0 ( 512 if directory), IO not allowed

            stCreateInode = dbConnection.prepareStatement(sqlCreateInode);

            Timestamp now = new Timestamp(System.currentTimeMillis());

            stCreateInode.setString(1, inode.toString());
            stCreateInode.setInt(2, type);
            stCreateInode.setInt(3, mode & UnixPermission.S_PERMS);
            stCreateInode.setInt(4, nlink);
            stCreateInode.setInt(5, uid);
            stCreateInode.setInt(6, gid);
            stCreateInode.setLong(7, (type == UnixPermission.S_IFDIR) ? 512 : 0);
            stCreateInode.setInt(8, _ioMode);
            stCreateInode.setTimestamp(9, now);
            stCreateInode.setTimestamp(10, now);
            stCreateInode.setTimestamp(11, now);
            stCreateInode.setTimestamp(12, now);
            stCreateInode.setLong(13, 0);

            stCreateInode.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stCreateInode);
        }

    }

    /**
     *
     * creates an entry in t_level_x table
     *
     * @param dbConnection
     * @param inode
     * @param uid
     * @param gid
     * @param mode
     * @param level
     * @throws SQLException
     * @return
     */
    FsInode createLevel(Connection dbConnection, FsInode inode, int uid, int gid, int mode, int level) throws SQLException {

        PreparedStatement stCreateInodeLevel = null;

        try {

            Timestamp now = new Timestamp(System.currentTimeMillis());
            stCreateInodeLevel = dbConnection.prepareStatement("INSERT INTO t_level_" + level + " VALUES(?,?,1,?,?,0,?,?,?, NULL)");

            stCreateInodeLevel.setString(1, inode.toString());
            stCreateInodeLevel.setInt(2, mode);
            stCreateInodeLevel.setInt(3, uid);
            stCreateInodeLevel.setInt(4, gid);
            stCreateInodeLevel.setTimestamp(5, now);
            stCreateInodeLevel.setTimestamp(6, now);
            stCreateInodeLevel.setTimestamp(7, now);
            stCreateInodeLevel.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stCreateInodeLevel);
        }

        return new FsInode(inode.getFs(), inode.toString(), level);
    }
    private static final String sqlRemoveInode = "DELETE FROM t_inodes WHERE ipnfsid=? AND inlink = 0";

    boolean removeInode(Connection dbConnection, FsInode inode) throws SQLException {
        int rc = 0;
        PreparedStatement stRemoveInode = null; //remove inode from t_inodes

        try {

            stRemoveInode = dbConnection.prepareStatement(sqlRemoveInode);

            stRemoveInode.setString(1, inode.toString());

            rc = stRemoveInode.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stRemoveInode);
        }

        return rc > 0;
    }

    boolean removeInodeLevel(Connection dbConnection, FsInode inode, int level) throws
                                                                                SQLException {

        int rc = 0;
        PreparedStatement stRemoveInodeLevel = null;
        try {

            stRemoveInodeLevel = dbConnection.prepareStatement("DELETE FROM t_level_" + level + " WHERE ipnfsid=?");
            stRemoveInodeLevel.setString(1, inode.toString());
            rc = stRemoveInodeLevel.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stRemoveInodeLevel);
        }

        return rc > 0;
    }

    /**
     * increase inode reference count by 1;
     * the same as incNlink(dbConnection, inode, 1)
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     */
    void incNlink(Connection dbConnection, FsInode inode) throws SQLException {
        incNlink(dbConnection, inode, 1);
    }
    private static final String sqlIncNlink = "UPDATE t_inodes SET inlink=inlink +?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    /**
     * increases the reference count of the inode by delta
     *
     * @param dbConnection
     * @param inode
     * @param delta
     * @throws SQLException
     */
    void incNlink(Connection dbConnection, FsInode inode, int delta) throws SQLException {

        PreparedStatement stIncNlinkCount = null; // increase nlink count of the inode
        Timestamp now = new Timestamp(System.currentTimeMillis());
        try {
            stIncNlinkCount = dbConnection.prepareStatement(sqlIncNlink);

            stIncNlinkCount.setInt(1, delta);
            stIncNlinkCount.setTimestamp(2, now);
            stIncNlinkCount.setTimestamp(3, now);
            stIncNlinkCount.setString(4, inode.toString());

            stIncNlinkCount.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stIncNlinkCount);
        }

    }

    /**
     *  decreases inode reverence count by 1.
     *  the same as decNlink(dbConnection, inode, 1)
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     */
    void decNlink(Connection dbConnection, FsInode inode) throws SQLException {
        decNlink(dbConnection, inode, 1);
    }
    private static final String sqlDecNlink = "UPDATE t_inodes SET inlink=inlink -?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    /**
     * decreases inode reference count by delta
     *
     * @param dbConnection
     * @param inode
     * @param delta
     * @throws SQLException
     */
    void decNlink(Connection dbConnection, FsInode inode, int delta) throws SQLException {

        PreparedStatement stDecNlinkCount = null; // decrease nlink count of the inode
        Timestamp now = new Timestamp(System.currentTimeMillis());
        try {

            stDecNlinkCount = dbConnection.prepareStatement(sqlDecNlink);
            stDecNlinkCount.setInt(1, delta);
            stDecNlinkCount.setTimestamp(2, now);
            stDecNlinkCount.setTimestamp(3, now);
            stDecNlinkCount.setString(4, inode.toString());

            stDecNlinkCount.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stDecNlinkCount);
        }

    }
    private static final String sqlCreateEntryInParent = "INSERT INTO t_dirs VALUES(?,?,?)";

    /**
     *
     * creates an entry name for the inode in the directory parent.
     * parent's reference count is not increased
     *
     * @param dbConnection
     * @param parent
     * @param name
     * @param inode
     * @throws SQLException
     */
    void createEntryInParent(Connection dbConnection, FsInode parent, String name, FsInode inode) throws SQLException {

        PreparedStatement stInserIntoParent = null;
        try {

            stInserIntoParent = dbConnection.prepareStatement(sqlCreateEntryInParent);
            stInserIntoParent.setString(1, parent.toString());
            stInserIntoParent.setString(2, name);
            stInserIntoParent.setString(3, inode.toString());
            stInserIntoParent.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stInserIntoParent);
        }

    }
    private static final String sqlRemoveEntryInParentByID = "DELETE FROM t_dirs WHERE ipnfsid=? AND iparent=?";

    void removeEntryInParentByID(Connection dbConnection, FsInode parent, FsInode inode) throws SQLException {

        PreparedStatement stRemoveFromParentById = null; // remove entry from parent
        try {

            stRemoveFromParentById = dbConnection.prepareStatement(sqlRemoveEntryInParentByID);
            stRemoveFromParentById.setString(1, inode.toString());
            stRemoveFromParentById.setString(2, parent.toString());

            stRemoveFromParentById.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stRemoveFromParentById);
        }

    }
    private static final String sqlRemoveEntryInParentByName = "DELETE FROM t_dirs WHERE iname=? AND iparent=?";

    void removeEntryInParent(Connection dbConnection, FsInode parent, String name) throws SQLException {
        PreparedStatement stRemoveFromParentByName = null; // remove entry from parent
        try {

            stRemoveFromParentByName = dbConnection.prepareStatement(sqlRemoveEntryInParentByName);
            stRemoveFromParentByName.setString(1, name);
            stRemoveFromParentByName.setString(2, parent.toString());

            stRemoveFromParentByName.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stRemoveFromParentByName);
        }

    }
    private static final String sqlGetParentOf = "SELECT iparent FROM t_dirs WHERE ipnfsid=? AND iname != '.' and iname != '..'";

    /**
     *
     * return a parent of inode. In case of hard links, one of the parents is returned
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    FsInode getParentOf(Connection dbConnection, FsInode inode) throws SQLException {

        FsInode parent = null;
        ResultSet result = null;
        PreparedStatement stGetParentId = null;
        try {

            stGetParentId = dbConnection.prepareStatement(sqlGetParentOf);
            stGetParentId.setString(1, inode.toString());

            result = stGetParentId.executeQuery();

            if (result.next()) {
                parent = new FsInode(inode.getFs(), result.getString("iparent"));
            }

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(stGetParentId);
        }

        return parent;
    }
    private static final String sqlGetParentOfDirectory = "SELECT iparent FROM t_dirs WHERE ipnfsid=? AND iname!='..' AND iname !='.'";

    /**
     *
     * return a parent of inode. In case of hard links, one of the parents is returned
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    FsInode getParentOfDirectory(Connection dbConnection, FsInode inode) throws SQLException {

        FsInode parent = null;
        ResultSet result = null;
        PreparedStatement stGetParentId = null;
        try {

            stGetParentId = dbConnection.prepareStatement(sqlGetParentOfDirectory);
            stGetParentId.setString(1, inode.toString());

            result = stGetParentId.executeQuery();

            if (result.next()) {
                parent = new FsInode(inode.getFs(), result.getString("iparent"));
            }

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(stGetParentId);
        }

        return parent;
    }
    private static final String sqlGetNameOf = "SELECT iname FROM t_dirs WHERE ipnfsid=? AND iparent=?";

    /**
     *
     * return the the name of the inode in parent
     *
     * @param dbConnection
     * @param parent
     * @param inode
     * @throws SQLException
     * @return
     */
    String getNameOf(Connection dbConnection, FsInode parent, FsInode inode) throws SQLException {

        ResultSet result = null;
        PreparedStatement stGetName = null;
        String name = null;
        try {

            stGetName = dbConnection.prepareStatement(sqlGetNameOf);
            stGetName.setString(1, inode.toString());
            stGetName.setString(2, parent.toString());

            result = stGetName.executeQuery();

            if (result.next()) {
                name = result.getString("iname");
            }

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(stGetName);
        }

        return name;
    }
    private static final String sqlSetFileSize = "UPDATE t_inodes SET isize=?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileSize(Connection dbConnection, FsInode inode, long newSize) throws SQLException {

        PreparedStatement ps = null;

        try {

            ps = dbConnection.prepareStatement(sqlSetFileSize);

            ps.setLong(1, newSize);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            ps.setString(4, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlSetFileOwner = "UPDATE t_inodes SET iuid=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileOwner(Connection dbConnection, FsInode inode, int level, int newOwner) throws SQLException {

        PreparedStatement ps = null;

        try {

            String fileSetModeQuery;

            if (level == 0) {
                fileSetModeQuery = sqlSetFileOwner;
            } else {
                fileSetModeQuery = "UPDATE t_level_" + level + " SET iuid=?,ictime=? WHERE ipnfsid=?";
            }
            ps = dbConnection.prepareStatement(fileSetModeQuery);

            ps.setInt(1, newOwner);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }

    }
    private static final String sqlSetFileName = "UPDATE t_dirs SET iname=? WHERE iname=? AND iparent=?";

    void setFileName(Connection dbConnection, FsInode dir, String oldName, String newName) throws SQLException, ChimeraFsException {

        PreparedStatement ps = null;

        try {

            ps = dbConnection.prepareStatement(sqlSetFileName);

            ps.setString(1, newName);
            ps.setString(2, oldName);
            ps.setString(3, dir.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlSetInodeAttributes = "UPDATE t_inodes SET iatime=?, imtime=?, ictime=?, icrtime=?, isize=?, iuid=?, igid=?, imode=?, itype=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setInodeAttributes(Connection dbConnection, FsInode inode, int level, Stat stat) throws SQLException {

        PreparedStatement ps = null;

        try {

            // attributes atime, mtime, size, uid, gid, mode

            /*
             *  only level 0 , e.g. original file allowed to have faked file size
             */
            if (level == 0) {

                ps = dbConnection.prepareStatement(sqlSetInodeAttributes);

                ps.setTimestamp(1, new Timestamp(stat.getATime()));
                ps.setTimestamp(2, new Timestamp(stat.getMTime()));
                ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                ps.setTimestamp(4, new Timestamp(stat.getCrTime()));
                ps.setLong(5, stat.getSize());
                ps.setInt(6, stat.getUid());
                ps.setInt(7, stat.getGid());
                ps.setInt(8, stat.getMode() & UnixPermission.S_PERMS);
                ps.setInt(9, stat.getMode() & UnixPermission.S_TYPE);
                ps.setString(10, inode.toString());
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level
                        + " SET iatime=?, imtime=?, iuid=?, igid=?, imode=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);

                ps.setTimestamp(1, new Timestamp(stat.getATime()));
                ps.setTimestamp(2, new Timestamp(stat.getMTime()));
                ps.setInt(3, stat.getUid());
                ps.setInt(4, stat.getGid());
                ps.setInt(5, stat.getMode());
                ps.setString(6, inode.toString());
            }

            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlSetFileATime = "UPDATE t_inodes SET iatime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileATime(Connection dbConnection, FsInode inode, int level, long atime) throws SQLException {

        PreparedStatement ps = null;

        try {

            if (level == 0) {
                ps = dbConnection.prepareStatement(sqlSetFileATime);
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level + " SET iatime=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);
            }

            ps.setTimestamp(1, new Timestamp(atime));
            ps.setString(2, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlSetFileCTime = "UPDATE t_inodes SET ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileCTime(Connection dbConnection, FsInode inode, int level, long ctime) throws SQLException {

        PreparedStatement ps = null;

        try {

            if (level == 0) {
                ps = dbConnection.prepareStatement(sqlSetFileCTime);
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level + " SET ictime=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);
            }

            ps.setTimestamp(1, new Timestamp(ctime));
            ps.setString(2, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }

    }
    private static final String sqlSetFileMTime = "UPDATE t_inodes SET imtime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileMTime(Connection dbConnection, FsInode inode, int level, long mtime) throws SQLException {

        PreparedStatement ps = null;

        try {

            if (level == 0) {
                ps = dbConnection.prepareStatement(sqlSetFileMTime);
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level + " SET imtime=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);
            }
            ps.setTimestamp(1, new Timestamp(mtime));
            ps.setString(2, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }

    }
    private static final String sqlSetFileGroup = "UPDATE t_inodes SET igid=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileGroup(Connection dbConnection, FsInode inode, int level, int newGroup) throws SQLException {

        PreparedStatement ps = null;
        try {

            if (level == 0) {
                ps = dbConnection.prepareStatement(sqlSetFileGroup);
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level + " SET igid=?,ictime=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);
            }
            ps.setInt(1, newGroup);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }

    }
    private static final String sqlSetFileMode = "UPDATE t_inodes SET imode=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?";

    void setFileMode(Connection dbConnection, FsInode inode, int level, int newMode) throws SQLException {

        PreparedStatement ps = null;
        try {

            if (level == 0) {
                ps = dbConnection.prepareStatement(sqlSetFileMode);
            } else {
                String fileSetModeQuery = "UPDATE t_level_" + level + " SET imode=?,ictime=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(fileSetModeQuery);
            }
            ps.setInt(1, newMode & UnixPermission.S_PERMS);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, inode.toString());
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlIsIoEnabled = "SELECT iio FROM t_inodes WHERE ipnfsid=?";

    /**
     * checks for IO flag of the inode. if IO enabled, regular read and write operations are allowed
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    boolean isIoEnabled(Connection dbConnection, FsInode inode) throws SQLException {

        boolean ioEnabled = false;
        ResultSet rs = null;
        PreparedStatement stIsIoEnabled = null;

        try {
            stIsIoEnabled = dbConnection.prepareStatement(sqlIsIoEnabled);
            stIsIoEnabled.setString(1, inode.toString());

            rs = stIsIoEnabled.executeQuery();
            if (rs.next()) {
                ioEnabled = rs.getInt("iio") == 1 ? true : false;
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stIsIoEnabled);
        }
        return ioEnabled;

    }
    private static final String sqlSetInodeIo = "UPDATE t_inodes SET iio=? WHERE ipnfsid=?";

    void setInodeIo(Connection dbConnection, FsInode inode, boolean enable) throws SQLException {

        PreparedStatement ps = null;

        try {

            ps = dbConnection.prepareStatement(sqlSetInodeIo);
            ps.setInt(1, enable ? 1 : 0);
            ps.setString(2, inode.toString());

            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }

    int write(Connection dbConnection, FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) throws SQLException {

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {

            if (level == 0) {

                ps = dbConnection.prepareStatement("SELECT ipnfsid FROM t_inodes_data WHERE ipnfsid=?");
                ps.setString(1, inode.toString());

                rs = ps.executeQuery();
                boolean exist = rs.next();
                SqlHelper.tryToClose(rs);
                SqlHelper.tryToClose(ps);

                if (exist) {
                    // entry exist, update only
                    String writeStream = "UPDATE t_inodes_data SET ifiledata=? WHERE ipnfsid=?";

                    ps = dbConnection.prepareStatement(writeStream);

                    ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                    ps.setString(2, inode.toString());

                    ps.executeUpdate();
                    SqlHelper.tryToClose(ps);

                } else {
                    // new entry
                    String writeStream = "INSERT INTO t_inodes_data VALUES (?,?)";

                    ps = dbConnection.prepareStatement(writeStream);

                    ps.setString(1, inode.toString());
                    ps.setBinaryStream(2, new ByteArrayInputStream(data, offset, len), len);

                    ps.executeUpdate();
                    SqlHelper.tryToClose(ps);
                }

                // correct file size
                String writeStream = "UPDATE t_inodes SET isize=? WHERE ipnfsid=?";

                ps = dbConnection.prepareStatement(writeStream);

                ps.setLong(1, len);
                ps.setString(2, inode.toString());

                ps.executeUpdate();

            } else {

                // if level does not exist, create it

                if (stat(dbConnection, inode, level) == null) {
                    createLevel(dbConnection, inode, 0, 0, 644, level);
                }

                String writeStream = "UPDATE t_level_" + level + " SET ifiledata=?,isize=? WHERE ipnfsid=?";
                ps = dbConnection.prepareStatement(writeStream);

                ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                ps.setLong(2, len);
                ps.setString(3, inode.toString());

                ps.executeUpdate();
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(ps);
        }

        return len;
    }

    int read(Connection dbConnection, FsInode inode, int level, long beginIndex, byte[] data, int offset, int len)
            throws SQLException, IOHimeraFsException {

        int count = 0;
        PreparedStatement stReadFromInode = null;
        ResultSet rs = null;

        try {

            if (level == 0) {
                stReadFromInode = dbConnection.prepareStatement("SELECT ifiledata FROM t_inodes_data WHERE ipnfsid=?");
            } else {
                stReadFromInode = dbConnection.prepareStatement("SELECT ifiledata FROM t_level_" + level + " WHERE ipnfsid=?");
            }

            stReadFromInode.setString(1, inode.toString());
            rs = stReadFromInode.executeQuery();

            if (rs.next()) {
                InputStream in = rs.getBinaryStream(1);

                in.skip(beginIndex);
                int c;
                while (((c = in.read()) != -1) && (count < len)) {
                    data[offset + count] = (byte) c;
                    ++count;
                }
                //count = in.available() > len ? len : in.available() ;
                //in.read(data, offset, count);
            }

        } catch (IOException e) {
            throw new IOHimeraFsException(e.toString());
        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stReadFromInode);
        }

        return count;
    }
    /////////////////////////////////////////////////////////////////////
    ////
    ////   Location info
    ////
    ////////////////////////////////////////////////////////////////////
    private static final String sqlGetInodeLocationsByType =
            "SELECT ilocation,ipriority,ictime,iatime  "
            + "FROM t_locationinfo WHERE itype=? AND ipnfsid=? AND istate=1 ORDER BY ipriority DESC";

    /**
     *
     *  returns a list of locations of defined type for the inode.
     *  only 'online' locations is returned
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @throws SQLException
     * @return
     */
    List<StorageLocatable> getInodeLocations(Connection dbConnection, FsInode inode, int type) throws
                                                                                               SQLException {

        List<StorageLocatable> locations = new ArrayList<>();
        ResultSet rs = null;
        PreparedStatement stGetInodeLocations = null;
        try {

            stGetInodeLocations = dbConnection.prepareStatement(sqlGetInodeLocationsByType);

            stGetInodeLocations.setInt(1, type);
            stGetInodeLocations.setString(2, inode.toString());

            rs = stGetInodeLocations.executeQuery();

            while (rs.next()) {

                long ctime = rs.getTimestamp("ictime").getTime();
                long atime = rs.getTimestamp("iatime").getTime();
                int priority = rs.getInt("ipriority");
                String location = rs.getString("ilocation");

                StorageLocatable inodeLocation = new StorageGenericLocation(type, priority, location, ctime, atime, true);
                locations.add(inodeLocation);
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetInodeLocations);
        }

        return locations;
    }

    private static final String sqlGetInodeLocations =
            "SELECT itype,ilocation,ipriority,ictime,iatime  "
            + "FROM t_locationinfo WHERE ipnfsid=? AND istate=1 ORDER BY ipriority DESC";

    /**
     *
     *  returns a list of locations for the inode.
     *  only 'online' locations is returned
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    List<StorageLocatable> getInodeLocations(Connection dbConnection, FsInode inode)
            throws SQLException
    {
        List<StorageLocatable> locations = new ArrayList<>();
        ResultSet rs = null;
        PreparedStatement stGetInodeLocations = null;
        try {

            stGetInodeLocations = dbConnection.prepareStatement(sqlGetInodeLocations);

            stGetInodeLocations.setString(1, inode.toString());

            rs = stGetInodeLocations.executeQuery();

            while (rs.next()) {

                int type = rs.getInt("itype");
                long ctime = rs.getTimestamp("ictime").getTime();
                long atime = rs.getTimestamp("iatime").getTime();
                int priority = rs.getInt("ipriority");
                String location = rs.getString("ilocation");

                StorageLocatable inodeLocation = new StorageGenericLocation(type, priority, location, ctime, atime, true);
                locations.add(inodeLocation);
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetInodeLocations);
        }

        return locations;
    }


    private static final String sqlAddInodeLocation = "INSERT INTO t_locationinfo VALUES(?,?,?,?,?,?,?)";

    /**
     *
     * adds a new location for the inode
     *
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @param location
     * @throws SQLException
     */
    void addInodeLocation(Connection dbConnection, FsInode inode, int type, String location) throws
                                                                                             SQLException {
        PreparedStatement stAddInodeLocation = null; // add a new  location in the storage system for the inode
        try {

            stAddInodeLocation = dbConnection.prepareStatement(sqlAddInodeLocation);

            Timestamp now = new Timestamp(System.currentTimeMillis());
            stAddInodeLocation.setString(1, inode.toString());
            stAddInodeLocation.setInt(2, type);
            stAddInodeLocation.setString(3, location);
            stAddInodeLocation.setInt(4, 10); // default priority
            stAddInodeLocation.setTimestamp(5, now);
            stAddInodeLocation.setTimestamp(6, now);
            stAddInodeLocation.setInt(7, 1); // online

            stAddInodeLocation.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stAddInodeLocation);
        }
    }
    private static final String sqlClearInodeLocation = "DELETE FROM t_locationinfo WHERE ipnfsid=? AND itype=? AND ilocation=?";

    /**
     *
     *  remove the location for a inode
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @param location
     * @throws SQLException
     */
    void clearInodeLocation(Connection dbConnection, FsInode inode, int type, String location) throws
                                                                                               SQLException {
        PreparedStatement stClearInodeLocation = null; // clear a location in the storage system for the inode

        try {
            stClearInodeLocation = dbConnection.prepareStatement(sqlClearInodeLocation);
            stClearInodeLocation.setString(1, inode.toString());
            stClearInodeLocation.setInt(2, type);
            stClearInodeLocation.setString(3, location);

            stClearInodeLocation.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stClearInodeLocation);
        }
    }
    private static final String sqlClearInodeLocations = "DELETE FROM t_locationinfo WHERE ipnfsid=?";

    /**
     *
     * remove all locations for a inode
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     */
    void clearInodeLocations(Connection dbConnection, FsInode inode) throws
                                                                     SQLException {
        PreparedStatement stClearInodeLocations = null; // clear a location in the storage system for the inode

        try {
            stClearInodeLocations = dbConnection.prepareStatement(sqlClearInodeLocations);
            stClearInodeLocations.setString(1, inode.toString());

            stClearInodeLocations.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stClearInodeLocations);
        }
    }
    /////////////////////////////////////////////////////////////////////
    ////
    ////   Directory tags handling
    ////
    ////////////////////////////////////////////////////////////////////
    private static final String sqlTags = "SELECT itagname FROM t_tags where ipnfsid=?";

    String[] tags(Connection dbConnection, FsInode inode) throws SQLException {

        String[] list = null;
        ResultSet rs = null;
        PreparedStatement stGetTags = null;
        try {

            stGetTags = dbConnection.prepareStatement(sqlTags);
            stGetTags.setString(1, inode.toString());
            rs = stGetTags.executeQuery();

            List<String> v = new ArrayList<>();

            while (rs.next()) {
                v.add(rs.getString("itagname"));
            }
            rs.close();

            list = v.toArray(new String[v.size()]);

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetTags);
        }

        return list;
    }

    private static final String sqlGetTags =
            "SELECT t.itagname, i.ivalue, i.isize FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid WHERE t.ipnfsid=?";

    Map<String,byte[]> getAllTags(Connection dbConnection, FsInode inode) throws SQLException, IOException
    {
        Map<String,byte[]> tags = new HashMap<>();
        try (PreparedStatement stGetAllTags = dbConnection.prepareStatement(sqlGetTags)) {
            stGetAllTags.setString(1, inode.toString());
            try (ResultSet rs = stGetAllTags.executeQuery()) {
                while (rs.next()) {
                    try (InputStream in = rs.getBinaryStream("ivalue")) {
                        byte[] data = new byte[Ints.saturatedCast(rs.getLong("isize"))];
                        ByteStreams.readFully(in, data);
                        tags.put(rs.getString("itagname"), data);
                    }
                }
            }
        }
        return tags;
    }

    /**
     * creates a new tag for the inode.
     * the inode becomes the tag origin.
     *
     *
     * @param dbConnection
     * @param inode
     * @param name
     * @param uid
     * @param gid
     * @param mode
     * @throws SQLException
     */
    void createTag(Connection dbConnection, FsInode inode, String name, int uid, int gid, int mode) throws SQLException {

        String id = createTagInode(dbConnection, uid, gid, mode);
        assignTagToDir(dbConnection, id, name, inode, false, true);
    }
    private static final String sqlGetTagId = "SELECT itagid FROM t_tags WHERE ipnfsid=? AND itagname=?";

    /**
     * returns tag id of a tag associated with inode
     *
     * @param dbConnection
     * @param dir
     * @param tag
     * @throws SQLException
     * @return
     */
    String getTagId(Connection dbConnection, FsInode dir, String tag) throws SQLException {
        String tagId = null;
        ResultSet rs = null;
        PreparedStatement stGetTagId = null;

        try {
            stGetTagId = dbConnection.prepareStatement(sqlGetTagId);

            stGetTagId.setString(1, dir.toString());
            stGetTagId.setString(2, tag);

            rs = stGetTagId.executeQuery();
            if (rs.next()) {
                tagId = rs.getString("itagid");
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetTagId);
        }
        return tagId;
    }
    private static final String sqlCreateTagInode = "INSERT INTO t_tags_inodes VALUES(?,?,1,?,?,0,?,?,?,NULL)";

    /**
     *
     *  creates a new id for a tag and sores it into t_tags_inodes table.
     *
     * @param dbConnection
     * @param uid
     * @param gid
     * @param mode
     * @throws SQLException
     * @return
     */
    String createTagInode(Connection dbConnection, int uid, int gid, int mode) throws SQLException {

        String id = UUID.randomUUID().toString().toUpperCase();
        PreparedStatement stCreateTagInode = null;
        try {

            stCreateTagInode = dbConnection.prepareStatement(sqlCreateTagInode);

            Timestamp now = new Timestamp(System.currentTimeMillis());

            stCreateTagInode.setString(1, id);
            stCreateTagInode.setInt(2, mode | UnixPermission.S_IFREG);
            stCreateTagInode.setInt(3, uid);
            stCreateTagInode.setInt(4, gid);
            stCreateTagInode.setTimestamp(5, now);
            stCreateTagInode.setTimestamp(6, now);
            stCreateTagInode.setTimestamp(7, now);

            stCreateTagInode.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stCreateTagInode);
        }
        return id;
    }
    private static final String sqlAssignTagToDir_update = "UPDATE t_tags SET itagid=?,isorign=? WHERE ipnfsid=? AND itagname=?";
    private static final String sqlAssignTagToDir_add = "INSERT INTO t_tags VALUES(?,?,?,1)";

    /**
     *
     * creates a new or update existing tag for a directory
     *
     * @param dbConnection
     * @param tagId
     * @param tagName
     * @param dir
     * @param isUpdate
     * @param isOrign
     * @throws SQLException
     */
    void assignTagToDir(Connection dbConnection, String tagId, String tagName, FsInode dir, boolean isUpdate, boolean isOrign) throws SQLException {

        PreparedStatement ps = null;
        try {

            if (isUpdate) {
                ps = dbConnection.prepareStatement(sqlAssignTagToDir_update);

                ps.setString(1, tagId);
                ps.setInt(2, isOrign ? 1 : 0);
                ps.setString(3, dir.toString());
                ps.setString(4, tagName);

            } else {
                ps = dbConnection.prepareStatement(sqlAssignTagToDir_add);

                ps.setString(1, dir.toString());
                ps.setString(2, tagName);
                ps.setString(3, tagId);
            }

            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlSetTag = "UPDATE t_tags_inodes SET ivalue=?, isize=?, imtime=? WHERE itagid=?";

    int setTag(Connection dbConnection, FsInode inode, String tagName, byte[] data, int offset, int len) throws SQLException, ChimeraFsException {

        PreparedStatement stSetTag = null;
        try {

            String tagId = getTagId(dbConnection, inode, tagName);

            if (!isTagOwner(dbConnection, inode, tagName)) {
                // tag bunching
                Stat tagStat = statTag(dbConnection, inode, tagName);

                tagId = createTagInode(dbConnection, tagStat.getUid(), tagStat.getGid(), tagStat.getMode());
                assignTagToDir(dbConnection, tagId, tagName, inode, true, true);

            }

            stSetTag = dbConnection.prepareStatement(sqlSetTag);
            stSetTag.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
            stSetTag.setLong(2, len);
            stSetTag.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
            stSetTag.setString(4, tagId);
            stSetTag.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stSetTag);
        }

        return len;

    }

    private static final String sqlRemoveSingleTag = "DELETE FROM t_tags WHERE ipnfsid=? AND itagname=?";

    void removeTag(Connection dbConnection, FsInode dir, String tag)
            throws SQLException {
        PreparedStatement ps = null;
        try {
            ps = dbConnection.prepareStatement(sqlRemoveSingleTag);
            ps.setString(1, dir.toString());
            ps.setString(2, tag);

            ps.executeUpdate();
        } finally {
            SqlHelper.tryToClose(ps);
        }
    }

    private static final String sqlRemoveTag = "DELETE FROM t_tags WHERE ipnfsid=?";

    void removeTag(Connection dbConnection, FsInode dir) throws SQLException {

        PreparedStatement ps = null;
        try {

            ps = dbConnection.prepareStatement(sqlRemoveTag);
            ps.setString(1, dir.toString());

            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }
    private static final String sqlGetTag = "SELECT i.ivalue,i.isize FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid WHERE t.ipnfsid=? AND t.itagname=?";

    /**
     * get content of the tag associated with name for inode
     *
     * @param dbConnection
     * @param inode
     * @param tagName
     * @param data
     * @param offset
     * @param len
     * @throws SQLException
     * @throws IOException
     * @return
     */
    int getTag(Connection dbConnection, FsInode inode, String tagName, byte[] data, int offset, int len) throws SQLException, IOException {

        int count = 0;
        ResultSet rs = null;
        PreparedStatement stGetTag = null;
        try {
            stGetTag = dbConnection.prepareStatement(sqlGetTag);
            stGetTag.setString(1, inode.toString());
            stGetTag.setString(2, tagName);
            rs = stGetTag.executeQuery();

            if (rs.next()) {
                try (InputStream in = rs.getBinaryStream("ivalue")) {
                    /*
                     * some databases (hsqldb in particular) fill a full record for
                     * BLOBs and on read reads a full record, which is not what we expect.
                     *
                     */
                    int size = Math.min(len, (int) rs.getLong("isize"));

                    while (count < size) {

                        int c = in.read();
                        if (c == -1) {
                            break;
                        }

                        data[offset + count] = (byte) c;
                        ++count;
                    }
                }
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetTag);
        }

        return count;
    }
    private static final String sqlStatTag = "SELECT isize,inlink,imode,iuid,igid,iatime,ictime,imtime FROM t_tags_inodes WHERE itagid=?";

    Stat statTag(Connection dbConnection, FsInode dir, String name) throws ChimeraFsException, SQLException {


        Stat ret = new Stat();
        PreparedStatement stStatTag = null; // get tag attributes
        try {

            String tagId = getTagId(dbConnection, dir, name);

            if (tagId == null) {
                throw new FileNotFoundHimeraFsException("tag do not exist");
            }

            stStatTag = dbConnection.prepareStatement(sqlStatTag);
            stStatTag.setString(1, tagId);
            ResultSet statResult = stStatTag.executeQuery();

            if (statResult.next()) {

                ret.setSize(statResult.getLong("isize"));
                ret.setATime(statResult.getTimestamp("iatime").getTime());
                ret.setCTime(statResult.getTimestamp("ictime").getTime());
                ret.setMTime(statResult.getTimestamp("imtime").getTime());
                ret.setUid(statResult.getInt("iuid"));
                ret.setGid(statResult.getInt("igid"));
                ret.setMode(statResult.getInt("imode"));
                ret.setNlink(statResult.getInt("inlink"));
                ret.setIno((int) dir.id());
                ret.setDev(17);

            } else {
                // file not found
                throw new FileNotFoundHimeraFsException(name);
            }

        } finally {
            SqlHelper.tryToClose(stStatTag);
        }

        return ret;
    }
    private static final String sqlIsTagOwner = "SELECT isorign FROM t_tags WHERE ipnfsid=? AND itagname=?";

    /**
     * checks for tag ownership
     *
     * @param dbConnection
     * @param dir
     * @param tagName
     * @throws SQLException
     * @return true, if inode is the origin of the tag
     */
    boolean isTagOwner(Connection dbConnection, FsInode dir, String tagName) throws SQLException {

        boolean isOwner = false;
        PreparedStatement stTagOwner = null;
        ResultSet rs = null;

        try {

            stTagOwner = dbConnection.prepareStatement(sqlIsTagOwner);
            stTagOwner.setString(1, dir.toString());
            stTagOwner.setString(2, tagName);

            rs = stTagOwner.executeQuery();
            if (rs.next()) {
                int rc = rs.getInt("isorign");
                if (rc == 1) {
                    isOwner = true;
                }
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stTagOwner);
        }

        return isOwner;
    }

    void createTags(Connection dbConnection, FsInode inode, int uid, int gid, int mode, Map<String, byte[]> tags)
            throws SQLException
    {
        PreparedStatement stmt = null;
        try {
            Map<String,String> ids = new HashMap<>();
            Timestamp now = new Timestamp(System.currentTimeMillis());

            stmt = dbConnection.prepareStatement("INSERT INTO t_tags_inodes VALUES(?,?,1,?,?,?,?,?,?,?)");
            for (Map.Entry<String, byte[]> tag : tags.entrySet()) {
                String id = UUID.randomUUID().toString().toUpperCase();
                ids.put(tag.getKey(), id);
                byte[] value = tag.getValue();
                int len = value.length;
                stmt.setString(1, id);
                stmt.setInt(2, mode | UnixPermission.S_IFREG);
                stmt.setInt(3, uid);
                stmt.setInt(4, gid);
                stmt.setLong(5, len);
                stmt.setTimestamp(6, now);
                stmt.setTimestamp(7, now);
                stmt.setTimestamp(8, now);
                stmt.setBinaryStream(9, new ByteArrayInputStream(value), len);
                stmt.addBatch();
            }
            stmt.executeBatch();
            stmt.close();

            stmt = dbConnection.prepareStatement("INSERT INTO t_tags VALUES(?,?,?,1)");
            for (Map.Entry<String, String> tag : ids.entrySet()) {
                stmt.setString(1, inode.toString()); // ipnfsid
                stmt.setString(2, tag.getKey());     // itagname
                stmt.setString(3, tag.getValue());   // itagid
                stmt.addBatch();
            }
            stmt.executeBatch();
        } finally {
            SqlHelper.tryToClose(stmt);
        }
    }

    private final static String sqlCopyTag = "INSERT INTO t_tags ( SELECT ?, itagname, itagid, 0 from t_tags WHERE ipnfsid=?)";

    /**
     *
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param dbConnection
     * @param orign
     * @param destination
     * @throws SQLException
     */
    void copyTags(Connection dbConnection, FsInode orign, FsInode destination) throws SQLException {

        PreparedStatement stCopyTags = null;
        try {

            stCopyTags = dbConnection.prepareStatement(sqlCopyTag);
            stCopyTags.setString(1, destination.toString());
            stCopyTags.setString(2, orign.toString());
            stCopyTags.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stCopyTags);
        }
    }

    private static final String sqlSetTagOwner = "UPDATE t_tags_inodes SET iuid=?, ictime=? WHERE itagid=?";

    void setTagOwner(Connection dbConnection, FsInode_TAG tagInode, int newOwner) throws SQLException {

        PreparedStatement ps = null;
        String tagId = getTagId(dbConnection, tagInode, tagInode.tagName());

        try {

            ps = dbConnection.prepareStatement(sqlSetTagOwner);

            ps.setInt(1, newOwner);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, tagId);
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }

    private static final String sqlSetTagOwnerGroup = "UPDATE t_tags_inodes SET igid=?, ictime=? WHERE itagid=?";

    void setTagOwnerGroup(Connection dbConnection, FsInode_TAG tagInode, int newOwner) throws SQLException {

        PreparedStatement ps = null;
        String tagId = getTagId(dbConnection, tagInode, tagInode.tagName());

        try {

            ps = dbConnection.prepareStatement(sqlSetTagOwnerGroup);

            ps.setInt(1, newOwner);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, tagId);
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }

    private static final String sqlSetTagMode = "UPDATE t_tags_inodes SET imode=?, ictime=? WHERE itagid=?";

    void setTagMode(Connection dbConnection, FsInode_TAG tagInode, int mode) throws SQLException {

        PreparedStatement ps = null;
        String tagId = getTagId(dbConnection, tagInode, tagInode.tagName());

        try {

            ps = dbConnection.prepareStatement(sqlSetTagMode);

            ps.setInt(1, mode & UnixPermission.S_PERMS);
            ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
            ps.setString(3, tagId);
            ps.executeUpdate();

        } finally {
            SqlHelper.tryToClose(ps);
        }
    }

    /*
     * Storage Information
     *
     * Currently it's not allowed to modify it
     */
    private static final String sqlSetStorageInfo = "INSERT INTO t_storageinfo VALUES(?,?,?,?)";

    /**
     * set storage info of inode in t_storageinfo table.
     * once storage info is stores, it's not allowed to modify it
     *
     * @param dbConnection
     * @param inode
     * @param storageInfo
     * @throws SQLException
     */
    void setStorageInfo(Connection dbConnection, FsInode inode, InodeStorageInformation storageInfo) throws
                                                                                                     SQLException {

        PreparedStatement stSetStorageInfo = null; // clear locations in the storage system for the inode

        try {

            // no records updated - insert a new one
            stSetStorageInfo = dbConnection.prepareStatement(sqlSetStorageInfo);
            stSetStorageInfo.setString(1, inode.toString());
            stSetStorageInfo.setString(2, storageInfo.hsmName());
            stSetStorageInfo.setString(3, storageInfo.storageGroup());
            stSetStorageInfo.setString(4, storageInfo.storageSubGroup());

            stSetStorageInfo.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stSetStorageInfo);
        }
    }
    private static final String sqlGetAccessLatency = "SELECT iaccessLatency FROM t_access_latency WHERE ipnfsid=?";

    /**
     *
     * @param dbConnection
     * @param inode
     * @return Access Latency or null if not defined
     * @throws SQLException
     */
    AccessLatency getAccessLatency(Connection dbConnection, FsInode inode) throws
                                                                           SQLException {
        AccessLatency accessLatency = null;
        PreparedStatement stGetAccessLatency = null;
        ResultSet alResultSet = null;

        try {

            stGetAccessLatency = dbConnection.prepareStatement(sqlGetAccessLatency);
            stGetAccessLatency.setString(1, inode.toString());

            alResultSet = stGetAccessLatency.executeQuery();
            if (alResultSet.next()) {
                accessLatency = AccessLatency.getAccessLatency(alResultSet.getInt("iaccessLatency"));
            }

        } finally {
            SqlHelper.tryToClose(alResultSet);
            SqlHelper.tryToClose(stGetAccessLatency);
        }

        return accessLatency;
    }
    private static final String sqlGetRetentionPolicy = "SELECT iretentionPolicy FROM t_retention_policy WHERE ipnfsid=?";

    /**
     *
     * @param dbConnection
     * @param inode
     * @return Retention Policy or null if not defined
     * @throws SQLException
     */
    RetentionPolicy getRetentionPolicy(Connection dbConnection, FsInode inode) throws
                                                                               SQLException {
        RetentionPolicy retentionPolicy = null;
        PreparedStatement stRetentionPolicy = null;
        ResultSet rpResultSet = null;

        try {

            stRetentionPolicy = dbConnection.prepareStatement(sqlGetRetentionPolicy);
            stRetentionPolicy.setString(1, inode.toString());

            rpResultSet = stRetentionPolicy.executeQuery();
            if (rpResultSet.next()) {
                retentionPolicy = RetentionPolicy.getRetentionPolicy(rpResultSet.getInt("iretentionPolicy"));
            }

        } finally {
            SqlHelper.tryToClose(rpResultSet);
            SqlHelper.tryToClose(stRetentionPolicy);
        }

        return retentionPolicy;
    }
    private static final String sqlSetAccessLatency = "INSERT INTO t_access_latency VALUES(?,?)";
    private static final String sqlUpdateAccessLatency = "UPDATE t_access_latency SET iaccessLatency=? WHERE ipnfsid=?";

    void setAccessLatency(Connection dbConnection, FsInode inode, AccessLatency accessLatency) throws
                                                                                               SQLException {

        PreparedStatement stSetAccessLatency = null; // clear locations in the storage system for the inode
        PreparedStatement stUpdateAccessLatency = null;
        try {

            stUpdateAccessLatency = dbConnection.prepareStatement(sqlUpdateAccessLatency);
            stUpdateAccessLatency.setInt(1, accessLatency.getId());
            stUpdateAccessLatency.setString(2, inode.toString());

            if (stUpdateAccessLatency.executeUpdate() == 0) {

                // no records updated - insert a new one

                stSetAccessLatency = dbConnection.prepareStatement(sqlSetAccessLatency);
                stSetAccessLatency.setString(1, inode.toString());
                stSetAccessLatency.setInt(2, accessLatency.getId());

                stSetAccessLatency.executeUpdate();
            }

        } finally {
            SqlHelper.tryToClose(stSetAccessLatency);
            SqlHelper.tryToClose(stUpdateAccessLatency);
        }
    }
    private static final String sqlSetRetentionPolicy = "INSERT INTO t_retention_policy VALUES(?,?)";
    private static final String sqlUpdateRetentionPolicy = "UPDATE t_retention_policy SET iretentionPolicy=? WHERE ipnfsid=?";

    void setRetentionPolicy(Connection dbConnection, FsInode inode, RetentionPolicy accessLatency) throws
                                                                                                   SQLException {

        PreparedStatement stSetRetentionPolicy = null; // clear locations in the storage system for the inode
        PreparedStatement stUpdateRetentionPolicy = null;
        try {

            stUpdateRetentionPolicy = dbConnection.prepareStatement(sqlUpdateRetentionPolicy);
            stUpdateRetentionPolicy.setInt(1, accessLatency.getId());
            stUpdateRetentionPolicy.setString(2, inode.toString());

            if (stUpdateRetentionPolicy.executeUpdate() == 0) {

                // no records updated - insert a new one

                stSetRetentionPolicy = dbConnection.prepareStatement(sqlSetRetentionPolicy);
                stSetRetentionPolicy.setString(1, inode.toString());
                stSetRetentionPolicy.setInt(2, accessLatency.getId());

                stSetRetentionPolicy.executeUpdate();
            }

        } finally {
            SqlHelper.tryToClose(stSetRetentionPolicy);
            SqlHelper.tryToClose(stUpdateRetentionPolicy);
        }
    }
    private static final String sqlRemoveStorageInfo = "DELETE FROM t_storageinfo WHERE ipnfsid=?";

    void removeStorageInfo(Connection dbConnection, FsInode inode) throws
                                                                   SQLException {

        PreparedStatement stRemoveStorageInfo = null; // clear locations in the storage system for the inode
        try {
            stRemoveStorageInfo = dbConnection.prepareStatement(sqlRemoveStorageInfo);
            stRemoveStorageInfo.setString(1, inode.toString());
            stRemoveStorageInfo.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stRemoveStorageInfo);
        }
    }
    private static final String sqlGetStorageInfo = "SELECT ihsmName, istorageGroup, istorageSubGroup " +
            "FROM t_storageinfo WHERE t_storageinfo.ipnfsid=?";

    /**
     *
     * returns storage information like storage group, storage sub group, hsm,
     * retention policy and access latency associated with the inode.
     *
     * @param dbConnection
     * @param inode
     * @throws ChimeraFsException
     * @throws SQLException
     * @return
     */
    InodeStorageInformation getStorageInfo(Connection dbConnection, FsInode inode) throws ChimeraFsException, SQLException {

        InodeStorageInformation storageInfo = null;

        ResultSet storageInfoResult = null;
        PreparedStatement stGetStorageInfo = null; // clear locations in the storage system for the inode
        try {

            stGetStorageInfo = dbConnection.prepareStatement(sqlGetStorageInfo);
            stGetStorageInfo.setString(1, inode.toString());
            storageInfoResult = stGetStorageInfo.executeQuery();

            if (storageInfoResult.next()) {

                String hsmName = storageInfoResult.getString("ihsmName");
                String storageGroup = storageInfoResult.getString("istoragegroup");
                String storageSubGroup = storageInfoResult.getString("istoragesubgroup");

                storageInfo = new InodeStorageInformation(inode, hsmName, storageGroup, storageSubGroup);
            } else {
                // file not found
                throw new FileNotFoundHimeraFsException(inode.toString());
            }

        } finally {
            SqlHelper.tryToClose(storageInfoResult);
            SqlHelper.tryToClose(stGetStorageInfo);
        }

        return storageInfo;
    }
    /*
     * directory caching
     * the following set of methods should help to path2inode and inode2path operations
     */
    private static final String sqlGetInodeFromCache = "SELECT ipnfsid FROM t_dir_cache WHERE ipath=?";

    String getInodeFromCache(Connection dbConnection, String path) throws SQLException {

        String inodeString = null;
        PreparedStatement stGetInodeFromCache = null;
        ResultSet getInodeFromCacheResultSet = null;

        try {
            stGetInodeFromCache = dbConnection.prepareStatement(sqlGetInodeFromCache);

            stGetInodeFromCache.setString(1, path);

            getInodeFromCacheResultSet = stGetInodeFromCache.executeQuery();
            if (getInodeFromCacheResultSet.next()) {
                inodeString = getInodeFromCacheResultSet.getString("ipnfsid");
            }

        } finally {
            SqlHelper.tryToClose(getInodeFromCacheResultSet);
            SqlHelper.tryToClose(stGetInodeFromCache);
        }

        return inodeString;

    }
    private static final String sqlGetPathFromCache = "SELECT ipath FROM t_dir_cache WHERE ipnfsid=?";

    String getPathFromCache(Connection dbConnection, FsInode inode) throws SQLException {

        String path = null;
        PreparedStatement stGetPathFromCache = null;
        ResultSet getPathFromCacheResultSet = null;

        try {
            stGetPathFromCache = dbConnection.prepareStatement(sqlGetPathFromCache);

            stGetPathFromCache.setString(1, inode.toString());

            getPathFromCacheResultSet = stGetPathFromCache.executeQuery();
            if (getPathFromCacheResultSet.next()) {
                path = getPathFromCacheResultSet.getString("ipath");
            }

        } finally {
            SqlHelper.tryToClose(getPathFromCacheResultSet);
            SqlHelper.tryToClose(stGetPathFromCache);
        }

        return path;
    }
    private static final String sqlSetInodeChecksum = "INSERT INTO t_inodes_checksum VALUES(?,?,?)";

    /**
     * add a checksum value of <i>type</i> to an inode
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @param value
     * @throws SQLException
     */
    void setInodeChecksum(Connection dbConnection, FsInode inode, int type, String value) throws SQLException {

        PreparedStatement stSetInodeChecksum = null;

        try {

            stSetInodeChecksum = dbConnection.prepareStatement(sqlSetInodeChecksum);
            stSetInodeChecksum.setString(1, inode.toString());
            stSetInodeChecksum.setInt(2, type);
            stSetInodeChecksum.setString(3, value);

            stSetInodeChecksum.executeUpdate();

        } finally {
            SqlHelper.tryToClose(stSetInodeChecksum);
        }

    }

    private static final String sqlGetInodeChecksums = "SELECT isum, itype FROM t_inodes_checksum WHERE ipnfsid=?";
    /**
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @param results holds set of checksums and their types {@link Checksum}
     *        for this inode
     * @throws SQLException
     */
    void getInodeChecksums(Connection dbConnection, FsInode inode, Set<Checksum> results)
                    throws SQLException {
        PreparedStatement stGetInodeChecksums = null;
        ResultSet getGetInodeChecksumResultSet = null;
        try {
            stGetInodeChecksums = dbConnection.prepareStatement(sqlGetInodeChecksums);
            stGetInodeChecksums.setString(1, inode.toString());
            getGetInodeChecksumResultSet = stGetInodeChecksums.executeQuery();
            if (getGetInodeChecksumResultSet.next()) {
                String checksum = getGetInodeChecksumResultSet.getString("isum");
                int type = getGetInodeChecksumResultSet.getInt("itype");
                results.add(new Checksum(ChecksumType.getChecksumType(type), checksum));
            }
        } finally {
            SqlHelper.tryToClose(getGetInodeChecksumResultSet);
            SqlHelper.tryToClose(stGetInodeChecksums);
        }
    }
    private static final String sqlRemoveInodeChecksum = "DELETE FROM t_inodes_checksum WHERE ipnfsid=? AND itype=?";
    private static final String sqlRemoveInodeAllChecksum = "DELETE FROM t_inodes_checksum WHERE ipnfsid=?";

    /**
     *
     * @param dbConnection
     * @param inode
     * @param type
     * @throws SQLException
     */
    void removeInodeChecksum(Connection dbConnection, FsInode inode, int type) throws SQLException {

        PreparedStatement stRemoveInodeChecksum = null;

        try {

            if (type >= 0) {
                stRemoveInodeChecksum = dbConnection.prepareStatement(sqlRemoveInodeChecksum);
                stRemoveInodeChecksum.setInt(2, type);
            } else {
                stRemoveInodeChecksum = dbConnection.prepareStatement(sqlRemoveInodeAllChecksum);
            }

            stRemoveInodeChecksum.setString(1, inode.toString());

            stRemoveInodeChecksum.executeUpdate();


        } finally {
            SqlHelper.tryToClose(stRemoveInodeChecksum);
        }

    }

    /**
     * get inode of given path starting <i>root</i> inode.
     * @param dbConnection
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     * @throws SQLException
     */
    FsInode path2inode(Connection dbConnection, FsInode root, String path)
            throws SQLException, IOHimeraFsException {


        File pathFile = new File(path);
        List<String> pathElemts = new ArrayList<>();


        do {
            String fileName = pathFile.getName();
            if (fileName.length() != 0) {
                /*
                 * skip multiple '/'
                 */
                pathElemts.add(pathFile.getName());
            }

            pathFile = pathFile.getParentFile();
        } while (pathFile != null);

        FsInode parentInode = root;
        FsInode inode = root;
        /*
         * while list in reverse order, we have too go backward
         */
        for (int i = pathElemts.size(); i > 0; i--) {
            String f = pathElemts.get(i - 1);
            inode = inodeOf(dbConnection, parentInode, f);

            if (inode == null) {
                /*
                 * element not found stop walking
                 */
                break;
            }

            /*
             * if is a link, then resove it
             */
            Stat s = stat(dbConnection, inode);
            if (UnixPermission.getType(s.getMode()) == UnixPermission.S_IFLNK) {
                byte[] b = new byte[(int) s.getSize()];
                int n = read(dbConnection, inode, 0, 0, b, 0, b.length);
                String link = new String(b, 0, n);
                if (link.charAt(0) == File.separatorChar) {
                    // FIXME: have to be done more elegant
                    parentInode = new FsInode(parentInode.getFs(), "000000000000000000000000000000000000");
                }
                inode = path2inode(dbConnection, parentInode, link);
            }
            parentInode = inode;
        }

        return inode;
    }

    /**
     * Get the inodes of given the path starting at <i>root</i>.
     *
     * @param dbConnection
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     * @throws SQLException
     */
    List<FsInode>
        path2inodes(Connection dbConnection, FsInode root, String path)
        throws SQLException, IOHimeraFsException
    {
        File pathFile = new File(path);
        List<String> pathElements = new ArrayList<>();

        do {
            String fileName = pathFile.getName();
            if (fileName.length() != 0) {
                /* Skip multiple file separators.
                 */
                pathElements.add(pathFile.getName());
            }
            pathFile = pathFile.getParentFile();
        } while (pathFile != null);

        FsInode parentInode = root;
        FsInode inode;

        List<FsInode> inodes = new ArrayList<>(pathElements.size() + 1);
        inodes.add(root);

        /* Path elements are in reverse order.
         */
        for (String f: Lists.reverse(pathElements)) {
            inode = inodeOf(dbConnection, parentInode, f);

            if (inode == null) {
                return Collections.emptyList();
            }

            inodes.add(inode);

            /* If inode is a link then resolve it.
             */
            Stat s = stat(dbConnection, inode);
            inode.setStatCache(s);
            if (UnixPermission.getType(s.getMode()) == UnixPermission.S_IFLNK) {
                byte[] b = new byte[(int) s.getSize()];
                int n = read(dbConnection, inode, 0, 0, b, 0, b.length);
                String link = new String(b, 0, n);
                if (link.charAt(0) == '/') {
                    // FIXME: has to be done more elegantly
                    parentInode = new FsInode(parentInode.getFs(), "000000000000000000000000000000000000");
                    inodes.add(parentInode);
                }
                List<FsInode> linkInodes =
                    path2inodes(dbConnection, parentInode, link);
                if (linkInodes.isEmpty()) {
                    return Collections.emptyList();
                }
                inodes.addAll(linkInodes.subList(1, linkInodes.size()));
                inode = linkInodes.get(linkInodes.size() - 1);
            }
            parentInode = inode;
        }

        return inodes;
    }

    private final static String  sqlGetACL = "SELECT * FROM t_acl WHERE rs_id =  ? ORDER BY ace_order";
    /**
     * Get inode's Access Control List. An empty list is returned if there are no ACL assigned
     * to the <code>inode</code>.
     * @param dbConnection
     * @param inode
     * @return
     * @throws SQLException
     */
    List<ACE> getACL(Connection dbConnection, FsInode inode) throws SQLException {
        List<ACE> acl = new ArrayList<>();
        PreparedStatement stGetAcl = null;
        ResultSet rs = null;
        try {
            stGetAcl = dbConnection.prepareStatement(sqlGetACL);
            stGetAcl.setString(1, inode.toString());

            rs = stGetAcl.executeQuery();
            while (rs.next()) {

                int type = rs.getInt("type");
                acl.add(new ACE(type == 0 ? AceType.ACCESS_ALLOWED_ACE_TYPE : AceType.ACCESS_DENIED_ACE_TYPE,
                        rs.getInt("flags"),
                        rs.getInt("access_msk"),
                        Who.valueOf(rs.getInt("who")),
                        rs.getInt("who_id"),
                        rs.getString("address_msk")));
            }

        }finally{
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetAcl);
        }
        return acl;
    }

    private static final String sqlDeleteACL = "DELETE FROM t_acl WHERE rs_id = ?";
    private static final String sqlAddACL = "INSERT INTO t_acl VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";
    /**
     * Set inode's Access Control List. The existing ACL will be replaced.
     * @param dbConnection
     * @param inode
     * @param acl
     * @throws SQLException
     */
    void setACL(Connection dbConnection, FsInode inode, List<ACE> acl) throws SQLException {

        PreparedStatement stDeleteACL = null;
        PreparedStatement stAddACL = null;

        try {
            stDeleteACL = dbConnection.prepareStatement(sqlDeleteACL);
            stDeleteACL.setString(1, inode.toString());
            stDeleteACL.executeUpdate();

            if(acl.isEmpty()) {
                return;
            }
            stAddACL = dbConnection.prepareStatement(sqlAddACL);

            int order = 0;
            RsType rsType = inode.isDirectory() ? RsType.DIR : RsType.FILE;
            for (ACE ace : acl) {

                stAddACL.setString(1, inode.toString());
                stAddACL.setInt(2, rsType.getValue() );
                stAddACL.setInt(3, ace.getType().getValue());
                stAddACL.setInt(4, ace.getFlags());
                stAddACL.setInt(5, ace.getAccessMsk());
                stAddACL.setInt(6, ace.getWho().getValue());
                stAddACL.setInt(7, ace.getWhoID());
                stAddACL.setString(8, ace.getAddressMsk());
                stAddACL.setInt(9, order);

                stAddACL.addBatch();
                order++;
            }
            stAddACL.executeBatch();
            setFileCTime(dbConnection, inode, 0, System.currentTimeMillis());
        }finally{
            SqlHelper.tryToClose(stDeleteACL);
            SqlHelper.tryToClose(stAddACL);
        }
    }

     /**
      * Check <i>sqlState</i> for unique key violation.
      * @param sqlState
      * @return true is sqlState is a unique key violation and false other wise
      */
    public boolean isDuplicatedKeyError(String sqlState) {
        return sqlState.equals(DUPLICATE_KEY_ERROR);
    }

    /**
     * Check <i>sqlState</i> for foreign key violation.
     * @param sqlState
     * @return true is sqlState is a foreign key violation and false other wise
     */
    public boolean isForeignKeyError(String sqlState) {
        return sqlState.equals(FOREIGN_KEY_ERROR);
    }

    /**
     *  creates an instance of org.dcache.chimera.&lt;dialect&gt;FsSqlDriver or
     *  default driver, if specific driver not available
     *
     * @param dialect
     * @return FsSqlDriver
     */
    static FsSqlDriver getDriverInstance(String dialect) {

        FsSqlDriver driver = null;

        String dialectDriverClass = "org.dcache.chimera." + dialect + "FsSqlDriver";

        try {
            driver = (FsSqlDriver) Class.forName(dialectDriverClass).newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
        } catch (ClassNotFoundException e) {
            _log.info(dialectDriverClass + " not found, using default FsSQLDriver.");
            driver = new FsSqlDriver();
        }

        return driver;
    }
}
