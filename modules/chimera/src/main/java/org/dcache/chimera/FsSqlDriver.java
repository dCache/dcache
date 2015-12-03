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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.IncorrectResultSizeDataAccessException;
import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.LobRetrievalFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ParameterizedPreparedStatementSetter;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator;

import javax.sql.DataSource;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.util.RetentionPolicy;

import org.dcache.acl.ACE;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.AceType;
import org.dcache.acl.enums.RsType;
import org.dcache.acl.enums.Who;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.dcache.util.Checksum;
import org.dcache.util.ChecksumType;

import static java.util.stream.Collectors.toList;
import static org.dcache.chimera.FileSystemProvider.*;
import static org.dcache.chimera.FileSystemProvider.StatCacheOption.STAT;

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
    private static final int IOMODE_ENABLE = 1;
    private static final int IOMODE_DISABLE = 0;

    protected final int _ioMode;

    final JdbcTemplate _jdbc;

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected FsSqlDriver(DataSource dataSource) {
        _ioMode = Boolean.valueOf(System.getProperty("chimera.inodeIoMode")) ? IOMODE_ENABLE : IOMODE_DISABLE;
        _jdbc = new JdbcTemplate(dataSource);
        _jdbc.setExceptionTranslator(new SQLErrorCodeSQLExceptionTranslator(dataSource) {
            @Override
            protected DataAccessException customTranslate(String task, String sql, SQLException sqlEx)
            {
                if (isForeignKeyError(sqlEx)) {
                    return new ForeignKeyViolationException(buildMessage(task, sql, sqlEx), sqlEx);
                }
                return super.customTranslate(task, sql, sqlEx);
            }
        });
    }


    /**
     * Get FsStat for a given filesystem.
     * @return fsStat
     */
    FsStat getFsStat() {
        return _jdbc.queryForObject(
                "SELECT count(ipnfsid) AS usedFiles, SUM(isize) AS usedSpace FROM t_inodes WHERE itype=32768",
                (rs, rowNum) -> {
                    long usedFiles = rs.getLong("usedFiles");
                    long usedSpace = rs.getLong("usedSpace");
                    return new FsStat(JdbcFs.AVAILABLE_SPACE, JdbcFs.TOTAL_FILES, usedSpace, usedFiles);
                });
    }

    /**
     *
     *  creates a new inode and an entry name in parent directory.
     * Parent reference count and modification time is updated.
     *
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @return
     */
    FsInode createFile(FsInode parent, String name, int owner, int group, int mode, int type) {
        return createFileWithId(parent, new FsInode(parent.getFs()), name, owner, group, mode, type);
    }

    /**
     *
     *  Creates a new entry with given inode is in parent directory.
     * Parent reference count and modification time is updated.
     *
     * @param inode
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @return
     */
    FsInode createFileWithId(FsInode parent, FsInode inode, String name, int owner, int group, int mode, int type) {
        return createInodeInParent(parent, name, inode, owner, group, mode, type, 1, 0);
    }

    /**
     * returns list of files in the directory. If there is no entries,
     * empty list is returned. inode is not tested to be a directory
     *
     * @param dir
     * @return
     */
    String[] listDir(FsInode dir) {
        List<String> directoryList = _jdbc.queryForList("SELECT iname FROM t_dirs WHERE iparent=?",
                                                        String.class, dir.toString());
        return Stream.concat(Stream.of(".", ".."), directoryList.stream()).toArray(String[]::new);
    }

    /**
     * the same as listDir, but array of {@HimeraDirectoryEntry} is returned, which contains
     * file attributes as well.
     *
     * @param dir
     * @return
     */
    DirectoryStreamB<HimeraDirectoryEntry> newDirectoryStream(FsInode dir) {
        return new DirectoryStreamB<HimeraDirectoryEntry>()
        {
            final DirectoryStreamImpl stream = new DirectoryStreamImpl(dir, _jdbc);

            @Override
            public Iterator<HimeraDirectoryEntry> iterator()
            {
                return new Iterator<HimeraDirectoryEntry>()
                {
                    private HimeraDirectoryEntry current = innerNext();

                    @Override
                    public boolean hasNext()
                    {
                        return current != null;
                    }

                    @Override
                    public HimeraDirectoryEntry next()
                    {
                        if (current == null) {
                            throw new NoSuchElementException("No more entries");
                        }
                        HimeraDirectoryEntry entry = current;
                        current = innerNext();
                        return entry;
                    }

                    protected HimeraDirectoryEntry innerNext()
                    {
                        try {
                            ResultSet rs = stream.next();
                            if (rs == null) {
                                return null;
                            }
                            Stat stat = toStat(rs);
                            FsInode inode = new FsInode(dir.getFs(), rs.getString("ipnfsid"), FsInodeType.INODE, 0, stat);
                            inode.setParent(dir);
                            stat.setIno((int)inode.id());
                            return new HimeraDirectoryEntry(rs.getString("iname"), inode, stat);
                        } catch (SQLException e) {
                            _log.error("failed to fetch next entry: {}", e.getMessage());
                            return null;
                        }
                    }
                };
            }

            @Override
            public void close() throws IOException
            {
                stream.close();
            }
        };
    }

    /**
     * Removes the hard link {@code name} in {@code parent} to {@code inode}. If the
     * last link is removed the object is deleted.
     *
     * @return true if removed, false if the link did not exist.
     */
    boolean remove(FsInode parent, String name, FsInode inode) throws ChimeraFsException {
        if (inode.type() != FsInodeType.INODE) {
            throw new InvalidArgumentChimeraException("Not a file.");
        }
        if (name.equals("..") || name.equals(".")) {
            throw new InvalidNameChimeraException("bad name: '" + name + "'");
        }
        return inode.isDirectory() ? removeDir(parent, inode, name) : removeFile(parent, inode, name);
    }

    private boolean removeDir(FsInode parent, FsInode inode, String name) throws ChimeraFsException {
        if (!removeEntryInParent(parent, name, inode)) {
            return false;
        }

        // A directory contains two pseudo entries for '.' and '..'
        decNlink(inode, 2);

        removeTag(inode);

        if (!removeInodeIfUnlinked(inode)) {
            throw new DirNotEmptyHimeraFsException("directory is not empty");
        }

        /* During bulk deletion of files in the same directory,
         * updating the parent inode is often a contention point. The
         * link count on the parent is updated last to reduce the time
         * in which the directory inode is locked by the database.
         */
        decNlink(parent);

        return true;
    }

    private boolean removeFile(FsInode parent, FsInode inode, String name) throws ChimeraFsException {
        if (!removeEntryInParent(parent, name, inode)) {
            return false;
        }
        decNlink(inode);

        removeInodeIfUnlinked(inode);

        /* During bulk deletion of files in the same directory,
         * updating the parent inode is often a contention point. The
         * link count on the parent is updated last to reduce the time
         * in which the directory inode is locked by the database.
         */
        decNlink(parent);

        return true;
    }

    void remove(FsInode inode) {
        if (inode.isDirectory()) {
            removeTag(inode);
        }

        /* Updating the inode effectively blocks anybody else from changing it and thus also from
         * adding more links.
         */
        _jdbc.update("UPDATE t_inodes SET inlink=0 WHERE ipnfsid=?", inode.toString());

        /* Remove all hard-links. */
        List<String> parents =
                _jdbc.queryForList(
                        "SELECT iparent FROM t_dirs WHERE ipnfsid=?",
                        String.class, inode.toString());
        for (String parent : parents) {
            decNlink(new FsInode(inode.getFs(), parent));
        }
        int n = _jdbc.update("DELETE FROM t_dirs WHERE ipnfsid=?", inode.toString());
        if (n != parents.size()) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("DELETE FROM t_dirs WHERE ipnfsid=?", parents.size(), n);
        }

        removeInodeIfUnlinked(inode);
    }

    public Stat stat(FsInode inode) {
        return stat(inode, 0);
    }

    public Stat stat(FsInode inode, int level) {
        Stat stat;
        if (level == 0) {
            stat = _jdbc.query(
                    "SELECT * FROM t_inodes WHERE ipnfsid = ?",
                    ps -> ps.setString(1, inode.toString()),
                    rs -> rs.next() ? toStat(rs) : null);
        } else {
            stat = _jdbc.query(
                    "SELECT * FROM t_level_" + level + " WHERE ipnfsid = ?",
                    ps -> ps.setString(1, inode.toString()),
                    rs -> rs.next() ? toStatLevel(rs) : null);
        }
        if (stat != null) {
            stat.setIno((int) inode.id());
        }
        return stat;
    }

    private Stat toStat(ResultSet rs) throws SQLException
    {
        Stat stat = new Stat();
        stat.setCrTime(rs.getTimestamp("icrtime").getTime());
        stat.setGeneration(rs.getLong("igeneration"));
        int rp = rs.getInt("iretention_policy");
        if (!rs.wasNull()) {
            stat.setRetentionPolicy(RetentionPolicy.getRetentionPolicy(rp));
        }
        int al = rs.getInt("iaccess_latency");
        if (!rs.wasNull()) {
            stat.setAccessLatency(AccessLatency.getAccessLatency(al));
        }
        stat.setSize(rs.getLong("isize"));
        stat.setATime(rs.getTimestamp("iatime").getTime());
        stat.setCTime(rs.getTimestamp("ictime").getTime());
        stat.setMTime(rs.getTimestamp("imtime").getTime());
        stat.setUid(rs.getInt("iuid"));
        stat.setGid(rs.getInt("igid"));
        stat.setMode(rs.getInt("imode") | rs.getInt("itype"));
        stat.setNlink(rs.getInt("inlink"));
        stat.setDev(17);
        stat.setRdev(13);
        return stat;
    }

    private Stat toStatLevel(ResultSet rs) throws SQLException
    {
        Stat stat = new Stat();
        stat.setCrTime(rs.getTimestamp("imtime").getTime());
        stat.setGeneration(0);
        stat.setSize(rs.getLong("isize"));
        stat.setATime(rs.getTimestamp("iatime").getTime());
        stat.setCTime(rs.getTimestamp("ictime").getTime());
        stat.setMTime(rs.getTimestamp("imtime").getTime());
        stat.setUid(rs.getInt("iuid"));
        stat.setGid(rs.getInt("igid"));
        stat.setMode(rs.getInt("imode") | UnixPermission.S_IFREG);
        stat.setNlink(rs.getInt("inlink"));
        stat.setDev(17);
        stat.setRdev(13);
        return stat;
    }

    /**
     * create a new directory in parent with name. The reference count if parent directory
     * as well modification time and reference count of newly created directory are updated.
     *
     *
     * @param parent
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @throws ChimeraFsException
     * @return
     */
    FsInode mkdir(FsInode parent, String name, int owner, int group, int mode) {
        return createInodeInParent(parent, name, new FsInode(parent.getFs()), owner, group, mode,
                                   UnixPermission.S_IFDIR, 2, 512);
    }

    /**
     * Move/rename inode from source in srcDir to dest in destDir. The reference counts
     * of srcDir and destDir are updated.
     *
     * @param srcDir
     * @param source
     * @param destDir
     * @param dest
     * @param inode
     * @return true if moved, false if source did not exist
     */
    boolean rename(FsInode inode, FsInode srcDir, String source, FsInode destDir, String dest) {

        String moveLink = "UPDATE t_dirs SET iparent=?, iname=? WHERE iparent=? AND iname=? AND ipnfsid=?";
        int n = _jdbc.update(moveLink,
                             ps -> {
                                 ps.setString(1, destDir.toString());
                                 ps.setString(2, dest);
                                 ps.setString(3, srcDir.toString());
                                 ps.setString(4, source);
                                 ps.setString(5, inode.toString());
                             });
        if (n == 0) {
            return false;
        }
        if (n > 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(moveLink, 1, n);
        }

        if (!srcDir.equals(destDir)) {
            incNlink(destDir);
            decNlink(srcDir);
        } else {
            incNlink(srcDir, 0);
        }
        return true;
    }

    /**
     * return the inode of path in directory. In case of pnfs magic commands ( '.(' )
     * command specific inode is returned.
     *
     * @param parent
     * @param name
     * @return null if path is not found
     */
    FsInode inodeOf(FsInode parent, String name, StatCacheOption stat) {
        switch (name) {
        case ".":
            return parent.isDirectory() ? parent : null;
        case "..":
            if (!parent.isDirectory()) {
                return null;
            }
            FsInode dir = parent.getParent();
            return (dir == null) ? parent : dir;
        default:
            if (stat == STAT) {
                return _jdbc.query(
                        "SELECT c.* FROM t_dirs d JOIN t_inodes c ON d.ipnfsid = c.ipnfsid " +
                        "WHERE d.iparent = ? AND d.iname = ?",
                        ps -> {
                            ps.setString(1, parent.toString());
                            ps.setString(2, name);
                        },
                        rs -> rs.next() ? new FsInode(parent.getFs(), rs.getString("ipnfsid"),
                                                      FsInodeType.INODE, 0, toStat(rs)) : null);
            } else {
                return _jdbc.query("SELECT ipnfsid FROM t_dirs WHERE iname=? AND iparent=?",
                                   ps -> {
                                       ps.setString(1, name);
                                       ps.setString(2, parent.toString());
                                   },
                                   rs -> rs.next() ? new FsInode(parent.getFs(), rs.getString("ipnfsid")) : null);
            }
        }
    }

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param inode
     * @param startFrom defined the "root"
     * @return
     */
    String inode2path(FsInode inode, FsInode startFrom) {
        if (inode.equals(startFrom)) {
            return "/";
        }

        try {
            List<String> pList = new ArrayList<>();
            String root = startFrom.toString();
            String elementId = inode.toString();
            do {
                Map<String, Object> map = _jdbc.queryForMap(
                        "SELECT iparent, iname FROM t_dirs WHERE ipnfsid=?", elementId);
                pList.add((String) map.get("iname"));
                elementId = (String) map.get("iparent");
            } while (!elementId.equals(root));
            return Lists.reverse(pList).stream().collect(Collectors.joining("/", "/", ""));
        } catch (IncorrectResultSizeDataAccessException e) {
            return "";
        }
    }

    FsInode createInodeInParent(FsInode parent, String name, FsInode inode, int owner, int group, int mode,
                                int type, int nlink, long size)
    {
        inode = createInode(inode, type, owner, group, mode, nlink, size);
        createEntryInParent(parent, name, inode);
        incNlink(parent);
        return inode;
    }

    /**
     *
     * creates an entry in t_inodes table with initial values.
     * for optimization, initial value of reference count may be defined.
     * for newly created files , file size is zero. For directories 512.
     *
     * @param inode
     * @param uid
     * @param gid
     * @param mode
     * @param nlink
     */
    FsInode createInode(FsInode inode, int type, int uid, int gid, int mode, int nlink, long size) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("INSERT INTO t_inodes (ipnfsid,itype,imode,inlink,iuid,igid,isize,iio," +
                     "ictime,iatime,imtime,icrtime,igeneration) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setInt(3, mode & UnixPermission.S_PERMS);
                         ps.setInt(4, nlink);
                         ps.setInt(5, uid);
                         ps.setInt(6, gid);
                         ps.setLong(7, size);
                         ps.setInt(8, _ioMode);
                         ps.setTimestamp(9, now);
                         ps.setTimestamp(10, now);
                         ps.setTimestamp(11, now);
                         ps.setTimestamp(12, now);
                         ps.setLong(13, 0);
                     });

        Stat stat = new Stat();
        stat.setCrTime(now.getTime());
        stat.setGeneration(0);
        stat.setSize(size);
        stat.setATime(now.getTime());
        stat.setCTime(now.getTime());
        stat.setMTime(now.getTime());
        stat.setUid(uid);
        stat.setGid(gid);
        stat.setMode(mode & UnixPermission.S_PERMS | type);
        stat.setNlink(nlink);
        stat.setIno((int) inode.id());
        stat.setDev(17);
        stat.setRdev(13);

        return new FsInode(inode.getFs(), inode.toString(), inode.type(), inode.getLevel(), stat);
    }

    /**
     *
     * creates an entry in t_level_x table
     *
     * @param inode
     * @param uid
     * @param gid
     * @param mode
     * @param level
     * @return
     */
    FsInode createLevel(FsInode inode, int uid, int gid, int mode, int level) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("INSERT INTO t_level_" + level + " VALUES(?,?,1,?,?,0,?,?,?, NULL)",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, mode);
                         ps.setInt(3, uid);
                         ps.setInt(4, gid);
                         ps.setTimestamp(5, now);
                         ps.setTimestamp(6, now);
                         ps.setTimestamp(7, now);
                     });
        Stat stat = new Stat();
        stat.setCrTime(now.getTime());
        stat.setGeneration(0);
        stat.setSize(0);
        stat.setATime(now.getTime());
        stat.setCTime(now.getTime());
        stat.setMTime(now.getTime());
        stat.setUid(uid);
        stat.setGid(gid);
        stat.setMode(mode | UnixPermission.S_IFREG);
        stat.setNlink(1);
        stat.setIno((int) inode.id());
        stat.setDev(17);
        stat.setRdev(13);
        return new FsInode(inode.getFs(), inode.toString(), FsInodeType.INODE, level, stat);
    }

    boolean removeInodeIfUnlinked(FsInode inode) {
        return _jdbc.update("DELETE FROM t_inodes WHERE ipnfsid=? AND inlink = 0", inode.toString()) > 0;
    }

    boolean removeInodeLevel(FsInode inode, int level) {
        return _jdbc.update("DELETE FROM t_level_" + level + " WHERE ipnfsid=?", inode.toString()) > 0;
    }

    /**
     * increase inode reference count by 1;
     * the same as incNlink(dbConnection, inode, 1)
     *
     * @param inode
     */
    void incNlink(FsInode inode) {
        incNlink(inode, 1);
    }

    /**
     * increases the reference count of the inode by delta
     *
     * @param inode
     * @param delta
     */
    void incNlink(FsInode inode, int delta) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("UPDATE t_inodes SET inlink=inlink +?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?",
                     ps -> {
                         ps.setInt(1, delta);
                         ps.setTimestamp(2, now);
                         ps.setTimestamp(3, now);
                         ps.setString(4, inode.toString());
                     });
    }

    /**
     *  decreases inode reverence count by 1.
     *  the same as decNlink(dbConnection, inode, 1)
     *
     * @param inode
     */
    void decNlink(FsInode inode) {
        decNlink(inode, 1);
    }

    /**
     * decreases inode reference count by delta
     *
     * @param inode
     * @param delta
     */
    void decNlink(FsInode inode, int delta) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("UPDATE t_inodes SET inlink=inlink -?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE ipnfsid=?",
                     ps -> {
                         ps.setInt(1, delta);
                         ps.setTimestamp(2, now);
                         ps.setTimestamp(3, now);
                         ps.setString(4, inode.toString());
                     });
    }

    /**
     *
     * creates an entry name for the inode in the directory parent.
     * parent's reference count is not increased
     *
     * @param parent
     * @param name
     * @param inode
     */
    void createEntryInParent(FsInode parent, String name, FsInode inode) {
        _jdbc.update("INSERT INTO t_dirs VALUES(?,?,?)",
                     ps -> {
                         ps.setString(1, parent.toString());
                         ps.setString(2, name);
                         ps.setString(3, inode.toString());
                     });
    }

    boolean removeEntryInParent(FsInode parent, String name, FsInode inode) {
        return _jdbc.update("DELETE FROM t_dirs WHERE iname=? AND iparent=? AND ipnfsid=?",
                            name, parent.toString(), inode.toString()) > 0;
    }

    /**
     *
     * return a parent of inode. In case of hard links, one of the parents is returned
     *
     * @param inode
     * @return
     */
    FsInode getParentOf(FsInode inode) {
        return _jdbc.query(
                "SELECT iparent FROM t_dirs WHERE ipnfsid=?",
                ps -> ps.setString(1, inode.toString()),
                rs -> rs.next() ? new FsInode(inode.getFs(), rs.getString("iparent")) : null);
    }

    boolean setInodeAttributes(FsInode inode, int level, Stat stat) {
        return _jdbc.update(con -> generateAttributeUpdateStatement(con, inode, stat, level)) > 0;
    }

    /**
     * checks for IO flag of the inode. if IO enabled, regular read and write operations are allowed
     *
     * @param inode
     * @return
     */
    boolean isIoEnabled(FsInode inode) {
        return _jdbc.query("SELECT iio FROM t_inodes WHERE ipnfsid=?",
                           ps -> ps.setString(1, inode.toString()),
                           rs -> rs.next() && rs.getInt("iio") == 1);
    }

    void setInodeIo(FsInode inode, boolean enable) {
        _jdbc.update("UPDATE t_inodes SET iio=? WHERE ipnfsid=?",
                     ps -> {
                         ps.setInt(1, enable ? 1 : 0);
                         ps.setString(2, inode.toString());
                     });
    }

    int write(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) {
        if (level == 0) {
            int n = _jdbc.queryForObject("SELECT count(ipnfsid) FROM t_inodes_data WHERE ipnfsid=?",
                                         Integer.class, inode.toString());
            if (n > 0) {
                // entry exist, update only
                _jdbc.update("UPDATE t_inodes_data SET ifiledata=? WHERE ipnfsid=?",
                             ps -> {
                                 ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                                 ps.setString(2, inode.toString());
                             });
            } else {
                // new entry
                _jdbc.update("INSERT INTO t_inodes_data VALUES (?,?)",
                             ps -> {
                                 ps.setString(1, inode.toString());
                                 ps.setBinaryStream(2, new ByteArrayInputStream(data, offset, len), len);
                             });
            }

            // correct file size
            _jdbc.update("UPDATE t_inodes SET isize=? WHERE ipnfsid=?",
                         ps -> {
                             ps.setLong(1, len);
                             ps.setString(2, inode.toString());
                         });
        } else {
            // if level does not exist, create it
            if (stat(inode, level) == null) {
                createLevel(inode, 0, 0, 644, level);
            }

            _jdbc.update("UPDATE t_level_" + level + " SET ifiledata=?,isize=? WHERE ipnfsid=?",
                         ps -> {
                             ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                             ps.setLong(2, len);
                             ps.setString(3, inode.toString());
                         });
        }

        return len;
    }

    int read(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) {
        String sql;
        if (level == 0) {
            sql = "SELECT ifiledata FROM t_inodes_data WHERE ipnfsid=?";
        } else {
            sql = "SELECT ifiledata FROM t_level_" + level + " WHERE ipnfsid=?";
        }

        return _jdbc.query(sql, rs -> {
            try {
                int count = 0;
                if (rs.next()) {
                    InputStream in = rs.getBinaryStream(1);
                    if (in != null) {
                        in.skip(beginIndex);
                        int c;
                        while (((c = in.read()) != -1) && (count < len)) {
                            data[offset + count] = (byte) c;
                            ++count;
                        }
                    }
                }
                return count;
            } catch (IOException e) {
                throw new LobRetrievalFailureException(e.getMessage(), e);
            }
        }, inode.toString());
    }

    /**
     *
     *  returns a list of locations of defined type for the inode.
     *  only 'online' locations is returned
     *
     * @param inode
     * @param type
     * @return
     */
    List<StorageLocatable> getInodeLocations(FsInode inode, int type) {
        return _jdbc.query("SELECT ilocation,ipriority,ictime,iatime  FROM t_locationinfo " +
                           "WHERE itype=? AND ipnfsid=? AND istate=1 ORDER BY ipriority DESC",
                           ps -> {
                               ps.setInt(1, type);
                               ps.setString(2, inode.toString());
                           },
                           (rs, rowNum) -> {
                               long ctime = rs.getTimestamp("ictime").getTime();
                               long atime = rs.getTimestamp("iatime").getTime();
                               int priority = rs.getInt("ipriority");
                               String location = rs.getString("ilocation");
                               return new StorageGenericLocation(type, priority, location, ctime, atime, true);
                           });
    }

    /**
     *
     *  returns a list of locations for the inode.
     *  only 'online' locations is returned
     *
     * @param inode
     * @return
     */
    List<StorageLocatable> getInodeLocations(FsInode inode)
    {
        return _jdbc.query("SELECT itype,ilocation,ipriority,ictime,iatime FROM t_locationinfo " +
                           "WHERE ipnfsid=? AND istate=1 ORDER BY ipriority DESC",
                           ps -> {
                               ps.setString(1, inode.toString());
                           },
                           (rs, rowNum) -> {
                               int type = rs.getInt("itype");
                               long ctime = rs.getTimestamp("ictime").getTime();
                               long atime = rs.getTimestamp("iatime").getTime();
                               int priority = rs.getInt("ipriority");
                               String location = rs.getString("ilocation");
                               return new StorageGenericLocation(type, priority, location, ctime, atime, true);
                           });
    }


    /**
     *
     * adds a new location for the inode
     *
     * @param inode
     * @param type
     * @param location
     */
    void addInodeLocation(FsInode inode, int type, String location) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("INSERT INTO t_locationinfo (SELECT * FROM (VALUES (?,?,?,?,?,?,?)) v WHERE NOT EXISTS " +
                     "(SELECT 1 FROM t_locationinfo WHERE ipnfsid=? AND itype=? AND ilocation=?))",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setString(3, location);
                         ps.setInt(4, 10); // default priority
                         ps.setTimestamp(5, now);
                         ps.setTimestamp(6, now);
                         ps.setInt(7, 1); // online
                         ps.setString(8, inode.toString());
                         ps.setInt(9, type);
                         ps.setString(10, location);
                     });
    }

    /**
     *
     *  remove the location for a inode
     *
     * @param inode
     * @param type
     * @param location
     */
    void clearInodeLocation(FsInode inode, int type, String location) {
        _jdbc.update("DELETE FROM t_locationinfo WHERE ipnfsid=? AND itype=? AND ilocation=?",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setString(3, location);
                     });
    }

    /**
     *
     * remove all locations for a inode
     *
     * @param inode
     * @throws SQLException
     */
    void clearInodeLocations(FsInode inode) {
        _jdbc.update("DELETE FROM t_locationinfo WHERE ipnfsid=?", inode.toString());
    }

    String[] tags(FsInode inode) {
        List<String> tags = _jdbc.queryForList("SELECT itagname FROM t_tags where ipnfsid=?",
                                               String.class, inode.toString());
        return tags.toArray(new String[tags.size()]);
    }

    Map<String,byte[]> getAllTags(FsInode inode)
    {
        Map<String,byte[]> tags = new HashMap<>();
        _jdbc.query("SELECT t.itagname, i.ivalue, i.isize " +
                    "FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid WHERE t.ipnfsid=?",
                    ps -> {
                        ps.setString(1, inode.toString());
                    },
                    rs -> {
                        try (InputStream in = rs.getBinaryStream("ivalue")) {
                            byte[] data = new byte[Ints.saturatedCast(rs.getLong("isize"))];
                            // we get null if filed id NULL, e.g not set
                            if (in != null) {
                                ByteStreams.readFully(in, data);
                                tags.put(rs.getString("itagname"), data);
                            }
                        } catch (IOException e) {
                            throw new LobRetrievalFailureException(e.getMessage(), e);
                        }
                    });
        return tags;
    }

    /**
     * creates a new tag for the inode.
     * the inode becomes the tag origin.
     *
     * @param inode
     * @param name
     * @param uid
     * @param gid
     * @param mode
     */
    void createTag(FsInode inode, String name, int uid, int gid, int mode) {
        long id = createTagInode(uid, gid, mode);
        assignTagToDir(id, name, inode, false, true);
    }

    /**
     * returns tag id of a tag associated with inode
     *
     * @param dir
     * @param tag
     * @return
     */
    Long getTagId(FsInode dir, String tag) {
        return _jdbc.query("SELECT itagid FROM t_tags WHERE ipnfsid=? AND itagname=?",
                           ps -> {
                               ps.setString(1, dir.toString());
                               ps.setString(2, tag);
                           },
                           rs -> rs.next() ? rs.getLong("itagid") : null);
    }

    /**
     *
     *  creates a new id for a tag and stores it into t_tags_inodes table.
     *
     * @param uid
     * @param gid
     * @param mode
     * @return
     */
    long createTagInode(int uid, int gid, int mode) {
        final String CREATE_TAG_INODE_WITHOUT_VALUE = "INSERT INTO t_tags_inodes (imode, inlink, iuid, igid, isize, " +
                                                      "ictime, iatime, imtime, ivalue) VALUES (?,1,?,?,0,?,?,?,NULL)";

        Timestamp now = new Timestamp(System.currentTimeMillis());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = _jdbc.update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            CREATE_TAG_INODE_WITHOUT_VALUE, Statement.RETURN_GENERATED_KEYS);
                    ps.setInt(1, mode | UnixPermission.S_IFREG);
                    ps.setInt(2, uid);
                    ps.setInt(3, gid);
                    ps.setTimestamp(4, now);
                    ps.setTimestamp(5, now);
                    ps.setTimestamp(6, now);
                    return ps;
                }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(CREATE_TAG_INODE_WITHOUT_VALUE, 1, rc);
        }
        return (Long) keyHolder.getKeys().get("itagid");
    }

    /**
     *
     *  creates a new id for a tag and stores it into t_tags_inodes table.
     *
     * @param uid
     * @param gid
     * @param mode
     * @param value
     * @return
     */
    long createTagInode(int uid, int gid, int mode, byte[] value) {
        final String CREATE_TAG_INODE_WITH_VALUE = "INSERT INTO t_tags_inodes (imode, inlink, iuid, igid, isize, " +
                                                   "ictime, iatime, imtime, ivalue) VALUES (?,1,?,?,?,?,?,?,?)";

        Timestamp now = new Timestamp(System.currentTimeMillis());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = _jdbc.update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            CREATE_TAG_INODE_WITH_VALUE, Statement.RETURN_GENERATED_KEYS);
                    ps.setInt(1, mode | UnixPermission.S_IFREG);
                    ps.setInt(2, uid);
                    ps.setInt(3, gid);
                    ps.setLong(4, value.length);
                    ps.setTimestamp(5, now);
                    ps.setTimestamp(6, now);
                    ps.setTimestamp(7, now);
                    ps.setBinaryStream(8, new ByteArrayInputStream(value), value.length);
                    return ps;
                }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(CREATE_TAG_INODE_WITH_VALUE, 1, rc);
        }
        return (Long) keyHolder.getKeys().get("itagid");
    }

    /**
     *
     * creates a new or update existing tag for a directory
     *
     * @param tagId
     * @param tagName
     * @param dir
     * @param isUpdate
     * @param isOrign
     */
    void assignTagToDir(long tagId, String tagName, FsInode dir, boolean isUpdate, boolean isOrign) {
        if (isUpdate) {
            _jdbc.update("UPDATE t_tags SET itagid=?,isorign=? WHERE ipnfsid=? AND itagname=?",
                         ps -> {
                             ps.setLong(1, tagId);
                             ps.setInt(2, isOrign ? 1 : 0);
                             ps.setString(3, dir.toString());
                             ps.setString(4, tagName);
                         });
        } else {
            _jdbc.update("INSERT INTO t_tags VALUES(?,?,?,1)",
                         ps -> {
                             ps.setString(1, dir.toString());
                             ps.setString(2, tagName);
                             ps.setLong(3, tagId);
                         });
        }
    }

    int setTag(FsInode inode, String tagName, byte[] data, int offset, int len) throws ChimeraFsException {
        long tagId;

        if (!isTagOwner(inode, tagName)) {
            // tag bunching
            Stat tagStat = statTag(inode, tagName);
            tagId = createTagInode(tagStat.getUid(), tagStat.getGid(), tagStat.getMode());
            assignTagToDir(tagId, tagName, inode, true, true);
        } else {
            tagId = getTagId(inode, tagName);
        }

        _jdbc.update("UPDATE t_tags_inodes SET ivalue=?, isize=?, imtime=? WHERE itagid=?",
                     ps -> {
                         ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                         ps.setLong(2, len);
                         ps.setTimestamp(3, new Timestamp(System.currentTimeMillis()));
                         ps.setLong(4, tagId);
                     });
        return len;

    }

    void removeTag(FsInode dir, String tag) {
        _jdbc.update("DELETE FROM t_tags WHERE ipnfsid=? AND itagname=?", dir.toString(), tag);
    }

    void removeTag(FsInode dir) {
        /* Get the tag IDs of the tag links to be removed.
         */
        List<Long> ids = _jdbc.queryForList("SELECT itagid FROM t_tags WHERE ipnfsid=?", Long.class, dir.toString());
        if (!ids.isEmpty()) {
            /* Remove the links.
             */
            _jdbc.update("DELETE FROM t_tags WHERE ipnfsid=?", dir.toString());

            /* Remove any tag inode of of the tag links removed above, which are
             * not referenced by any other links either.
             *
             * We ought to maintain the link count in the inode, but Chimera has
             * not done so in the past. In the interest of avoiding costly schema
             * corrections in patch level releases, the current solution queries
             * for the existence of other links instead.
             *
             * The statement below relies on concurrent transactions not deleting
             * other links to affected tag inodes. Otherwise we could come into a
             * situation in which two concurrent transactions remove two links to
             * the same inode, yet none of them realize that the inode is left
             * without links (as there is another link).
             *
             * One way to ensure this would be to use repeatable read transaction
             * isolation, but PostgreSQL doesn't support changing the isolation level
             * in the middle of a transaction. Always running any operation that
             * might call this method with repeatable read was deemed unacceptable.
             * Another solution would be to lock the tag inode at the beginning of
             * this method using SELECT FOR UPDATE. This would be fairly expensive
             * way of solving this race.
             *
             * For now we decide to ignore the race: It seems unlikely to run into
             * and even if one does, the consequence is merely an orphaned inode.
             */
            _jdbc.batchUpdate("DELETE FROM t_tags_inodes i WHERE itagid=? " +
                              "AND NOT EXISTS (SELECT 1 FROM t_tags WHERE itagid=?)",
                              ids, ids.size(),
                              (ps, tagid) -> {
                                  ps.setLong(1, tagid);
                                  ps.setLong(2, tagid);
                              });
        }
    }

    /**
     * get content of the tag associated with name for inode
     *
     * @param inode
     * @param tagName
     * @param data
     * @param offset
     * @param len
     * @return
     */
    int getTag(FsInode inode, String tagName, byte[] data, int offset, int len) {
        return _jdbc.query("SELECT i.ivalue,i.isize FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid " +
                           "WHERE t.ipnfsid=? AND t.itagname=?",
                           ps -> {
                               ps.setString(1, inode.toString());
                               ps.setString(2, tagName);
                           },
                           rs -> {
                               if (rs.next()) {
                                   try (InputStream in = rs.getBinaryStream("ivalue")) {
                                       /* some databases (hsqldb in particular) fill a full record for
                                        * BLOBs and on read reads a full record, which is not what we expect.
                                        */
                                       return ByteStreams.read(in, data, offset, Math.min(len, (int) rs.getLong("isize")));
                                   } catch (IOException e) {
                                       throw new LobRetrievalFailureException(e.getMessage(), e);
                                   }
                               }
                               return 0;
                           });
    }

    Stat statTag(FsInode dir, String name) throws ChimeraFsException {
        Long tagId = getTagId(dir, name);

        if (tagId == null) {
            throw new FileNotFoundHimeraFsException("tag does not exist");
        }

        try {
            return _jdbc.queryForObject("SELECT isize,inlink,imode,iuid,igid,iatime,ictime,imtime " +
                                        "FROM t_tags_inodes WHERE itagid=?",
                                        (rs, rowNum) -> {
                                            Stat ret = new Stat();
                                            ret.setSize(rs.getLong("isize"));
                                            ret.setATime(rs.getTimestamp("iatime").getTime());
                                            ret.setCTime(rs.getTimestamp("ictime").getTime());
                                            ret.setMTime(rs.getTimestamp("imtime").getTime());
                                            ret.setUid(rs.getInt("iuid"));
                                            ret.setGid(rs.getInt("igid"));
                                            ret.setMode(rs.getInt("imode"));
                                            ret.setNlink(rs.getInt("inlink"));
                                            ret.setIno((int) dir.id());
                                            ret.setGeneration(rs.getTimestamp("imtime").getTime());
                                            ret.setDev(17);
                                            ret.setRdev(13);
                                            return ret;
                                        },
                                        tagId);
        } catch (IncorrectResultSizeDataAccessException e) {
            throw new FileNotFoundHimeraFsException(name);
        }
    }

    /**
     * checks for tag ownership
     *
     * @param dir
     * @param tagName
     * @return true, if inode is the origin of the tag
     */
    boolean isTagOwner(FsInode dir, String tagName) {
        return _jdbc.query("SELECT isorign FROM t_tags WHERE ipnfsid=? AND itagname=?",
                           ps -> {
                               ps.setString(1, dir.toString());
                               ps.setString(2, tagName);
                           },
                           rs -> rs.next() && rs.getInt("isorign") == 1);
    }

    void createTags(FsInode inode, int uid, int gid, int mode, Map<String, byte[]> tags)
    {
        if (!tags.isEmpty()) {
            Map<String, Long> ids = new HashMap<>();
            tags.forEach((key, value) -> ids.put(key, createTagInode(uid, gid, mode, value)));
            _jdbc.batchUpdate("INSERT INTO t_tags VALUES(?,?,?,1)",
                              ids.entrySet(),
                              ids.size(),
                              (ps, tag) -> {
                                  ps.setString(1, inode.toString()); // ipnfsid
                                  ps.setString(2, tag.getKey());     // itagname
                                  ps.setLong(3, tag.getValue());     // itagid
                              });
        }
    }

    /**
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param orign
     * @param destination
     */
    void copyTags(FsInode orign, FsInode destination) {
        _jdbc.update("INSERT INTO t_tags (SELECT ?, itagname, itagid, 0 from t_tags WHERE ipnfsid=?)",
                     destination.toString(), orign.toString());
    }

    void setTagOwner(FsInode_TAG tagInode, int newOwner) throws FileNotFoundHimeraFsException {
        Long tagId = getTagId(tagInode, tagInode.tagName());
        if (tagId == null) {
            throw new FileNotFoundHimeraFsException("tag does not exist");
        }
        _jdbc.update("UPDATE t_tags_inodes SET iuid=?, ictime=? WHERE itagid=?",
                     ps -> {
                         ps.setInt(1, newOwner);
                         ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                         ps.setLong(3, tagId);
                     });
    }

    void setTagOwnerGroup(FsInode_TAG tagInode, int newOwner) throws FileNotFoundHimeraFsException {
        Long tagId = getTagId(tagInode, tagInode.tagName());
        if (tagId == null) {
            throw new FileNotFoundHimeraFsException("tag does not exist");
        }
        _jdbc.update("UPDATE t_tags_inodes SET igid=?, ictime=? WHERE itagid=?",
                     ps -> {
                         ps.setInt(1, newOwner);
                         ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                         ps.setLong(3, tagId);
                     });
    }

    void setTagMode(FsInode_TAG tagInode, int mode) throws FileNotFoundHimeraFsException {
        Long tagId = getTagId(tagInode, tagInode.tagName());
        if (tagId == null) {
            throw new FileNotFoundHimeraFsException("tag does not exist");
        }
        _jdbc.update("UPDATE t_tags_inodes SET imode=?, ictime=? WHERE itagid=?",
                     ps -> {
                         ps.setInt(1, mode & UnixPermission.S_PERMS);
                         ps.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
                         ps.setLong(3, tagId);
                     });
    }

    /**
     * set storage info of inode in t_storageinfo table.
     * once storage info is stores, it's not allowed to modify it
     *
     * @param inode
     * @param storageInfo
     */
    void setStorageInfo(FsInode inode, InodeStorageInformation storageInfo) {
        _jdbc.update("INSERT INTO t_storageinfo (SELECT * FROM (VALUES (?,?,?,?)) v WHERE NOT EXISTS " +
                     "(SELECT 1 FROM t_storageinfo WHERE ipnfsid=?))",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setString(2, storageInfo.hsmName());
                         ps.setString(3, storageInfo.storageGroup());
                         ps.setString(4, storageInfo.storageSubGroup());
                         ps.setString(5, inode.toString());
                     });
    }

    void removeStorageInfo(FsInode inode) {
        _jdbc.update("DELETE FROM t_storageinfo WHERE ipnfsid=?", inode.toString());
    }

    /**
     *
     * returns storage information like storage group, storage sub group, hsm,
     * retention policy and access latency associated with the inode.
     *
     * @param inode
     * @throws ChimeraFsException
     * @return
     */
    InodeStorageInformation getStorageInfo(FsInode inode) throws ChimeraFsException {
        try {
            return _jdbc.queryForObject(
                    "SELECT ihsmName, istorageGroup, istorageSubGroup FROM t_storageinfo WHERE ipnfsid=?",
                    (rs, rowNum) -> {
                        String hsmName = rs.getString("ihsmName");
                        String storageGroup = rs.getString("istoragegroup");
                        String storageSubGroup = rs.getString("istoragesubgroup");
                        return new InodeStorageInformation(inode, hsmName, storageGroup, storageSubGroup);
                    },
                    inode.toString());
        } catch (IncorrectResultSizeDataAccessException e) {
            throw new FileNotFoundHimeraFsException(inode.toString());
        }
    }

    /**
     * add a checksum value of <i>type</i> to an inode
     *
     * @param inode
     * @param type
     * @param value
     */
    void setInodeChecksum(FsInode inode, int type, String value) {
        _jdbc.update("INSERT INTO t_inodes_checksum (SELECT * FROM (VALUES (?,?,?)) v WHERE NOT EXISTS " +
                     "(SELECT 1 FROM t_inodes_checksum WHERE ipnfsid=?))",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setString(3, value);
                         ps.setString(4, inode.toString());
                     });
    }

    /**
     *
     * @param inode
     */
    List<Checksum> getInodeChecksums(FsInode inode) {
        return _jdbc.query("SELECT isum, itype FROM t_inodes_checksum WHERE ipnfsid=?",
                           ps -> ps.setString(1, inode.toString()),
                           (rs, rowNum) -> {
                               String checksum = rs.getString("isum");
                               int type = rs.getInt("itype");
                               return new Checksum(ChecksumType.getChecksumType(type), checksum);
                           });
    }

    /**
     *
     * @param inode
     * @param type
     */
    void removeInodeChecksum(FsInode inode, int type) {
        if (type >= 0) {
            _jdbc.update("DELETE FROM t_inodes_checksum WHERE ipnfsid=? AND itype=?",
                         ps -> {
                             ps.setString(1, inode.toString());
                             ps.setInt(2, type);
                         });
        } else {
            _jdbc.update("DELETE FROM t_inodes_checksum WHERE ipnfsid=?", inode.toString());
        }
    }

    /**
     * get inode of given path starting <i>root</i> inode.
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     */
    FsInode path2inode(FsInode root, String path) throws ChimeraFsException
    {
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
            inode = inodeOf(parentInode, f, STAT);

            if (inode == null) {
                /*
                 * element not found stop walking
                 */
                break;
            }

            /*
             * if is a link, then resolve it
             */
            Stat s = inode.statCache();
            if (UnixPermission.getType(s.getMode()) == UnixPermission.S_IFLNK) {
                byte[] b = new byte[(int) s.getSize()];
                int n = read(inode, 0, 0, b, 0, b.length);
                String link = new String(b, 0, n);
                if (link.charAt(0) == File.separatorChar) {
                    // FIXME: have to be done more elegant
                    parentInode = new FsInode(parentInode.getFs(), "000000000000000000000000000000000000");
                }
                inode = path2inode(parentInode, link);
            }
            parentInode = inode;
        }

        return inode;
    }

    /**
     * Get the inodes of given the path starting at <i>root</i>.
     *
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     */
    List<FsInode> path2inodes(FsInode root, String path) throws ChimeraFsException
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
            inode = inodeOf(parentInode, f, STAT);

            if (inode == null) {
                return Collections.emptyList();
            }

            inodes.add(inode);

            /* If inode is a link then resolve it.
             */
            Stat s = inode.statCache();
            if (UnixPermission.getType(s.getMode()) == UnixPermission.S_IFLNK) {
                byte[] b = new byte[(int) s.getSize()];
                int n = read(inode, 0, 0, b, 0, b.length);
                String link = new String(b, 0, n);
                if (link.charAt(0) == '/') {
                    // FIXME: has to be done more elegantly
                    parentInode = new FsInode(parentInode.getFs(), "000000000000000000000000000000000000");
                    inodes.add(parentInode);
                }
                List<FsInode> linkInodes =
                    path2inodes(parentInode, link);
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

    /**
     * Get inode's Access Control List. An empty list is returned if there are no ACL assigned
     * to the <code>inode</code>.
     * @param inode
     * @return
     */
    List<ACE> readAcl(FsInode inode) {
        return _jdbc.query("SELECT * FROM t_acl WHERE rs_id =  ? ORDER BY ace_order",
                           ps -> ps.setString(1, inode.toString()),
                           (rs, rowNum) -> {
                               AceType type =
                                       (rs.getInt("type") == 0)
                                       ? AceType.ACCESS_ALLOWED_ACE_TYPE
                                       : AceType.ACCESS_DENIED_ACE_TYPE;
                               return new ACE(type,
                                              rs.getInt("flags"),
                                              rs.getInt("access_msk"),
                                              Who.valueOf(rs.getInt("who")),
                                              rs.getInt("who_id"));
                           });
    }

    /**
     * Set inode's Access Control List. The inode must not have any ACLs prior to this call.
     * @param inode
     * @param acl
     */
    void writeAcl(FsInode inode, RsType rsType, List<ACE> acl) {
        _jdbc.batchUpdate("INSERT INTO t_acl VALUES (?, ?, ?, ?, ?, ?, ?, ?)", acl, acl.size(),
                          new ParameterizedPreparedStatementSetter<ACE>()
                          {
                              int order = 0;

                              @Override
                              public void setValues(PreparedStatement ps, ACE ace) throws SQLException
                              {
                                  ps.setString(1, inode.toString());
                                  ps.setInt(2, rsType.getValue());
                                  ps.setInt(3, ace.getType().getValue());
                                  ps.setInt(4, ace.getFlags());
                                  ps.setInt(5, ace.getAccessMsk());
                                  ps.setInt(6, ace.getWho().getValue());
                                  ps.setInt(7, ace.getWhoID());
                                  ps.setInt(8, order);
                                  order++;
                              }
                          });
    }

    boolean deleteAcl(FsInode inode)
    {
        return _jdbc.update("DELETE FROM t_acl WHERE rs_id = ?", inode.toString()) > 0;
    }

    /**
     * Copies ACL entries from source to inode. The inode must not have any ACLs prior to this call.
     *
     * @param source inode whose ACLs to copy
     * @param inode inode to add the ACLs to
     * @param type ACE object type
     * @param mask Flags to remove from the copied ACEs
     * @param flags Only copy ACEs that have one or more of these flags set
     */
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags)
    {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        List<ACE> acl = readAcl(source).stream()
                .filter(ace -> (ace.getFlags() & flgs) > 0)
                .map(ace -> new ACE(ace.getType(), (ace.getFlags() | msk) ^ msk, ace.getAccessMsk(), ace.getWho(), ace.getWhoID()))
                .collect(toList());
        writeAcl(inode, type, acl);
    }

    /**
     * Check <i>SQLException</i> for foreign key violation.
     * @return true is sqlState is a foreign key violation and false other wise
     */
    public boolean isForeignKeyError(SQLException e) {
        return e.getSQLState().equals("23503");
    }

    /**
     *  creates an instance of org.dcache.chimera.&lt;dialect&gt;FsSqlDriver or
     *  default driver, if specific driver not available
     *
     * @param dialect
     * @return FsSqlDriver
     */
    static FsSqlDriver getDriverInstance(String dialect, DataSource dataSource) {

        String dialectDriverClass = "org.dcache.chimera." + dialect + "FsSqlDriver";

        try {
            return (FsSqlDriver) Class.forName(dialectDriverClass).getDeclaredConstructor(DataSource.class).newInstance(dataSource);
        } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Failed to instantiate Chimera driver: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            _log.info(dialectDriverClass + " not found, using default FsSQLDriver.");
            return new FsSqlDriver(dataSource);
        }
    }

    private PreparedStatement generateAttributeUpdateStatement(Connection dbConnection, FsInode inode, Stat stat, int level)
	    throws SQLException {

        final String attrUpdatePrefix = level == 0
                ? "UPDATE t_inodes SET ictime=?,igeneration=igeneration+1"
                : "UPDATE t_level_" + level + " SET ictime=?";
        final String attrUpdateSuffix = (level == 0 && stat.isDefined(Stat.StatAttributes.SIZE))
                ? " WHERE ipnfsid=? AND itype = " + UnixPermission.S_IFREG : " WHERE ipnfsid=?";

        StringBuilder sb = new StringBuilder(128);
        long ctime = stat.isDefined(Stat.StatAttributes.CTIME) ? stat.getCTime() :
                System.currentTimeMillis();

        // set size always must trigger mtime update
        if (stat.isDefined(Stat.StatAttributes.SIZE) && !stat.isDefined(Stat.StatAttributes.MTIME)) {
            stat.setMTime(ctime);
        }

        sb.append(attrUpdatePrefix);

        if (stat.isDefined(Stat.StatAttributes.UID)) {
            sb.append(",iuid=?");
        }
        if (stat.isDefined(Stat.StatAttributes.GID)) {
            sb.append(",igid=?");
        }
        if (stat.isDefined(Stat.StatAttributes.SIZE)) {
            sb.append(",isize=?");
        }
        if (stat.isDefined(Stat.StatAttributes.MODE)) {
            sb.append(",imode=?");
        }
        if (stat.isDefined(Stat.StatAttributes.MTIME)) {
            sb.append(",imtime=?");
        }
        if (stat.isDefined(Stat.StatAttributes.ATIME)) {
            sb.append(",iatime=?");
        }
        if (stat.isDefined(Stat.StatAttributes.CRTIME)) {
            sb.append(",icrtime=?");
        }
        if (stat.isDefined(Stat.StatAttributes.ACCESS_LATENCY)) {
            sb.append(",iaccess_latency=?");
        }
        if (stat.isDefined(Stat.StatAttributes.RETENTION_POLICY)) {
            sb.append(",iretention_policy=?");
        }
        sb.append(attrUpdateSuffix);

        String statement = sb.toString();
        PreparedStatement preparedStatement = dbConnection.prepareStatement(statement);

        int idx = 1;
        preparedStatement.setTimestamp(idx++, new Timestamp(ctime));
        // NOTICE: order here MUST match the order of processing attributes above.
        if (stat.isDefined(Stat.StatAttributes.UID)) {
            preparedStatement.setInt(idx++, stat.getUid());
        }
        if (stat.isDefined(Stat.StatAttributes.GID)) {
            preparedStatement.setInt(idx++, stat.getGid());
        }
        if (stat.isDefined(Stat.StatAttributes.SIZE)) {
            preparedStatement.setLong(idx++, stat.getSize());
        }
        if (stat.isDefined(Stat.StatAttributes.MODE)) {
            preparedStatement.setInt(idx++, stat.getMode() & UnixPermission.S_PERMS);
        }
        if (stat.isDefined(Stat.StatAttributes.MTIME)) {
            preparedStatement.setTimestamp(idx++, new Timestamp(stat.getMTime()));
        }
        if (stat.isDefined(Stat.StatAttributes.ATIME)) {
            preparedStatement.setTimestamp(idx++, new Timestamp(stat.getATime()));
        }
        if (stat.isDefined(Stat.StatAttributes.CRTIME)) {
            preparedStatement.setTimestamp(idx++, new Timestamp(stat.getCrTime()));
        }
        if (stat.isDefined(Stat.StatAttributes.ACCESS_LATENCY)) {
            preparedStatement.setInt(idx++, stat.getAccessLatency().getId());
        }
        if (stat.isDefined(Stat.StatAttributes.RETENTION_POLICY)) {
            preparedStatement.setInt(idx++, stat.getRetentionPolicy().getId());
        }
        preparedStatement.setString(idx++, inode.toString());
        return preparedStatement;
    }
}
