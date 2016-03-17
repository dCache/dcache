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
import org.springframework.jdbc.core.ResultSetExtractor;
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
import static org.dcache.chimera.FileSystemProvider.StatCacheOption;
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

    private final long _root;

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected FsSqlDriver(DataSource dataSource) throws ChimeraFsException
    {
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
        Long root = getInumber("000000000000000000000000000000000000");
        if (root == null) {
            throw new FileNotFoundHimeraFsException("Root inode does not exist.");
        }
        _root = root;
    }

    long getRootInumber()
    {
        return _root;
    }

    /**
     * Get FsStat for a given filesystem.
     * @return fsStat
     */
    FsStat getFsStat() {
        return _jdbc.queryForObject(
                "SELECT count(*) AS usedFiles, SUM(isize) AS usedSpace FROM t_inodes WHERE itype=32768",
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
        return createFileWithId(parent, FsInode.generateNewID(), name, owner, group, mode, type);
    }

    /**
     *
     *  Creates a new entry with given inode is in parent directory.
     * Parent reference count and modification time is updated.
     *
     * @param parent
     * @param id
     * @param name
     * @param owner
     * @param group
     * @param mode
     * @param type
     * @return
     */
    FsInode createFileWithId(FsInode parent, String id, String name, int owner, int group, int mode, int type) {
        return createInodeInParent(parent, name, id, owner, group, mode, type, 1, 0);
    }

    Long getInumber(String id)
    {
        return _jdbc.query(
                "SELECT inumber FROM t_inodes WHERE ipnfsid = ?",
                ps -> ps.setString(1, id),
                rs -> rs.next() ? rs.getLong("inumber") : null);
    }

    String getId(FsInode inode)
    {
        return _jdbc.query(
                "SELECT ipnfsid FROM t_inodes WHERE inumber=?",
                ps -> ps.setLong(1, inode.ino()),
                rs -> rs.next() ? rs.getString("ipnfsid") : null);
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
                                                        String.class, dir.ino());
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
                            FsInode inode = new FsInode(dir.getFs(), rs.getLong("inumber"), FsInodeType.INODE, 0, stat);
                            inode.setParent(dir);
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
        _jdbc.update("UPDATE t_inodes SET inlink=0 WHERE inumber=?", inode.ino());

        /* Remove all hard-links. */
        List<Long> parents =
                _jdbc.queryForList(
                        "SELECT iparent FROM t_dirs WHERE ichild=?",
                        Long.class, inode.ino());
        for (Long parent : parents) {
            decNlink(new FsInode(inode.getFs(), parent));
        }
        int n = _jdbc.update("DELETE FROM t_dirs WHERE ichild=?", inode.ino());
        if (n != parents.size()) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException("DELETE FROM t_dirs WHERE ichild=?", parents.size(), n);
        }

        removeInodeIfUnlinked(inode);
    }

    public Stat stat(String id) {
        return _jdbc.query(
                "SELECT * FROM t_inodes WHERE ipnfsid=?",
                ps -> ps.setString(1, id),
                rs -> rs.next() ? toStat(rs) : null);
    }

    public Stat stat(FsInode inode) {
        return stat(inode, 0);
    }

    public Stat stat(FsInode inode, int level) {
        if (level == 0) {
            return _jdbc.query(
                    "SELECT * FROM t_inodes WHERE inumber=?",
                    ps -> ps.setLong(1, inode.ino()),
                    rs -> rs.next() ? toStat(rs) : null);
        } else {
            return _jdbc.query(
                    "SELECT * FROM t_level_" + level + " WHERE inumber=?",
                    ps -> ps.setLong(1, inode.ino()),
                    rs -> rs.next() ? toStatLevel(rs) : null);
        }
    }

    private Stat toStat(ResultSet rs) throws SQLException
    {
        Stat stat = new Stat();
        stat.setIno(rs.getLong("inumber"));
        stat.setId(rs.getString("ipnfsid"));
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
        stat.setIno(rs.getLong("inumber"));
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
        return createInodeInParent(parent, name, FsInode.generateNewID(), owner, group, mode,
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
        String moveLink = "UPDATE t_dirs SET iparent=?, iname=? WHERE iparent=? AND iname=? AND ichild=?";
        int n = _jdbc.update(moveLink,
                             ps -> {
                                 ps.setLong(1, destDir.ino());
                                 ps.setString(2, dest);
                                 ps.setLong(3, srcDir.ino());
                                 ps.setString(4, source);
                                 ps.setLong(5, inode.ino());
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
                        "SELECT c.* FROM t_dirs d JOIN t_inodes c ON d.ichild = c.inumber " +
                        "WHERE d.iparent = ? AND d.iname = ?",
                        ps -> {
                            ps.setLong(1, parent.ino());
                            ps.setString(2, name);
                        },
                        rs -> rs.next() ? new FsInode(parent.getFs(), rs.getLong("inumber"),
                                                      FsInodeType.INODE, 0, toStat(rs)) : null);
            } else {
                return _jdbc.query("SELECT ichild FROM t_dirs WHERE iparent=? AND iname=?",
                                   ps -> {
                                       ps.setLong(1, parent.ino());
                                       ps.setString(2, name);
                                   },
                                   rs -> rs.next() ? new FsInode(parent.getFs(), rs.getLong("ichild")) : null);
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
            long root = startFrom.ino();
            long elementId = inode.ino();
            do {
                Map<String, Object> map = _jdbc.queryForMap(
                        "SELECT iparent, iname FROM t_dirs WHERE ichild=?", elementId);
                pList.add((String) map.get("iname"));
                elementId = (long) map.get("iparent");
            } while (elementId != root);
            return Lists.reverse(pList).stream().collect(Collectors.joining("/", "/", ""));
        } catch (IncorrectResultSizeDataAccessException e) {
            return "";
        }
    }

    FsInode createInodeInParent(FsInode parent, String name, String id, int owner, int group, int mode,
                                int type, int nlink, long size)
    {
        Stat stat = createInode(id, type, owner, group, mode, nlink, size);
        FsInode inode = new FsInode(parent.getFs(), stat.getIno(), FsInodeType.INODE, 0, stat);
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
     * @param id
     * @param uid
     * @param gid
     * @param mode
     * @param nlink
     */
    Stat createInode(String id, int type, int uid, int gid, int mode, int nlink, long size) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        _jdbc.update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO t_inodes (ipnfsid,itype,imode,inlink,iuid,igid,isize,iio," +
                            "ictime,iatime,imtime,icrtime,igeneration) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, id);
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
                    return ps;
                }, keyHolder);

        Stat stat = new Stat();
        stat.setIno((Long) keyHolder.getKeys().get("inumber"));
        stat.setId(id);
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
        stat.setDev(17);
        stat.setRdev(13);

        return stat;
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
        _jdbc.update("INSERT INTO t_level_" + level + "(inumber,imode,inlink,iuid,igid,isize,ictime,iatime,imtime,ifiledata) VALUES(?,?,1,?,?,0,?,?,?, NULL)",
                     ps -> {
                         ps.setLong(1, inode.ino());
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
        stat.setIno(inode.ino());
        stat.setDev(17);
        stat.setRdev(13);
        return new FsInode(inode.getFs(), inode.ino(), FsInodeType.INODE, level, stat);
    }

    boolean removeInodeIfUnlinked(FsInode inode) {
        List<String> ids
                = _jdbc.queryForList("SELECT ipnfsid FROM t_inodes WHERE inumber=? AND inlink=0 FOR UPDATE",
                                     String.class, inode.ino());
        if (ids.isEmpty()) {
            return false;
        }
        if (ids.size() > 1) {
            throw new IncorrectResultSizeDataAccessException(1, ids.size());
        }
        String id = ids.get(0);
        _jdbc.update("INSERT INTO t_locationinfo_trash (ipnfsid,itype,ilocation,ipriority,ictime,iatime,istate) " +
                     "(SELECT ?,l.itype,l.ilocation,l.ipriority,l.ictime,l.iatime,l.istate " +
                     "FROM t_locationinfo l WHERE l.inumber=?)",
                     ps -> {
                         ps.setString(1, id);
                         ps.setLong(2, inode.ino());
                     });
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update(
                "INSERT INTO t_locationinfo_trash (ipnfsid,itype,ilocation,ipriority,ictime,iatime,istate) VALUES (?,2,'',0,?,?,1)",
                ps -> {
                    ps.setString(1, id);
                    ps.setTimestamp(2, now);
                    ps.setTimestamp(3, now);
                });
        _jdbc.update("DELETE FROM t_inodes WHERE inumber=?", inode.ino());
        return true;
    }

    boolean removeInodeLevel(FsInode inode, int level) {
        return _jdbc.update("DELETE FROM t_level_" + level + " WHERE inumber=?", inode.ino()) > 0;
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
        _jdbc.update("UPDATE t_inodes SET inlink=inlink +?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE inumber=?",
                     ps -> {
                         ps.setInt(1, delta);
                         ps.setTimestamp(2, now);
                         ps.setTimestamp(3, now);
                         ps.setLong(4, inode.ino());
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
        _jdbc.update("UPDATE t_inodes SET inlink=inlink -?,imtime=?,ictime=?,igeneration=igeneration+1 WHERE inumber=?",
                     ps -> {
                         ps.setInt(1, delta);
                         ps.setTimestamp(2, now);
                         ps.setTimestamp(3, now);
                         ps.setLong(4, inode.ino());
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
    void createEntryInParent(FsInode parent, String name, FsInode inode)
    {
        _jdbc.update("INSERT INTO t_dirs (iparent,ichild,iname) VALUES(?,?,?)",
                     ps -> {
                         ps.setLong(1, parent.ino());
                         ps.setLong(2, inode.ino());
                         ps.setString(3, name);
                     });
    }

    private boolean removeEntryInParent(FsInode parent, String name, FsInode child) {
        return _jdbc.update("DELETE FROM t_dirs WHERE iname=? AND iparent=? AND ichild=?",
                            name, parent.ino(), child.ino()) > 0;
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
                "SELECT iparent FROM t_dirs WHERE ichild=?",
                ps -> ps.setLong(1, inode.ino()),
                rs -> rs.next() ? new FsInode(inode.getFs(), rs.getLong("iparent")) : null);
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
        /* Since we access t_inodes anyway and the cost of transferring the entire row
         * is negligible, we fill the stat cache as a side effect.
         */
        return _jdbc.query("SELECT * FROM t_inodes WHERE inumber=?",
                           ps -> ps.setLong(1, inode.ino()),
                           rs -> {
                               if (rs.next()) {
                                   inode.setStatCache(toStat(rs));
                                   return rs.getInt("iio") == 1;
                               } else {
                                   return false;
                               }
                           });
    }

    void setInodeIo(FsInode inode, boolean enable) {
        _jdbc.update("UPDATE t_inodes SET iio=? WHERE inumber=?",
                     ps -> {
                         ps.setInt(1, enable ? 1 : 0);
                         ps.setLong(2, inode.ino());
                     });
    }

    int write(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) {
        if (level == 0) {
            int n = _jdbc.queryForObject("SELECT count(*) FROM t_inodes_data WHERE inumber=?",
                                         Integer.class, inode.ino());
            if (n > 0) {
                // entry exist, update only
                _jdbc.update("UPDATE t_inodes_data SET ifiledata=? WHERE inumber=?",
                             ps -> {
                                 ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                                 ps.setLong(2, inode.ino());
                             });
            } else {
                // new entry
                _jdbc.update("INSERT INTO t_inodes_data (inumber,ifiledata) VALUES (?,?)",
                             ps -> {
                                 ps.setLong(1, inode.ino());
                                 ps.setBinaryStream(2, new ByteArrayInputStream(data, offset, len), len);
                             });
            }

            // correct file size
            _jdbc.update("UPDATE t_inodes SET isize=? WHERE inumber=?",
                         ps -> {
                             ps.setLong(1, len);
                             ps.setLong(2, inode.ino());
                         });
        } else {
            int n = _jdbc.queryForObject(
                    "SELECT count(*) FROM t_level_" + level + " WHERE inumber=?", Integer.class, inode.ino());
            if (n == 0) {
                // if level does not exist, create it
                Timestamp now = new Timestamp(System.currentTimeMillis());
                _jdbc.update("INSERT INTO t_level_" + level + "(inumber,imode,inlink,iuid,igid,isize,ictime,iatime,imtime,ifiledata) VALUES(?,?,1,?,?,?,?,?,?,?)",
                             ps -> {
                                 ps.setLong(1, inode.ino());
                                 ps.setInt(2, 644);
                                 ps.setInt(3, 0);
                                 ps.setInt(4, 0);
                                 ps.setLong(5, len);
                                 ps.setTimestamp(6, now);
                                 ps.setTimestamp(7, now);
                                 ps.setTimestamp(8, now);
                                 ps.setBinaryStream(9, new ByteArrayInputStream(data, offset, len), len);
                             });
            } else {
                _jdbc.update("UPDATE t_level_" + level + " SET ifiledata=?,isize=? WHERE ino=?",
                             ps -> {
                                 ps.setBinaryStream(1, new ByteArrayInputStream(data, offset, len), len);
                                 ps.setLong(2, len);
                                 ps.setLong(3, inode.ino());
                             });
            }
        }

        return len;
    }

    int read(FsInode inode, int level, long beginIndex, byte[] data, int offset, int len) {
        ResultSetExtractor<Integer> extractor = rs -> {
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
        };
        if (level == 0) {
            return _jdbc.query("SELECT ifiledata FROM t_inodes_data WHERE inumber=?", extractor, inode.ino());
        } else {
            return _jdbc.query("SELECT ifiledata FROM t_level_" + level + " WHERE inumber=?", extractor, inode.ino());
        }
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
                           "WHERE itype=? AND inumber=? AND istate=1 ORDER BY ipriority DESC",
                           ps -> {
                               ps.setInt(1, type);
                               ps.setLong(2, inode.ino());
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
    List<StorageLocatable> getInodeLocations(FsInode inode) {
        return _jdbc.query("SELECT itype,ilocation,ipriority,ictime,iatime FROM t_locationinfo " +
                           "WHERE inumber=? AND istate=1 ORDER BY ipriority DESC",
                           ps -> {
                               ps.setLong(1, inode.ino());
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
        _jdbc.update("INSERT INTO t_locationinfo (inumber,itype,ilocation,ipriority,ictime,iatime,istate) " +
                     "(SELECT * FROM (VALUES (?,?,?,?,?,?,?)) v WHERE NOT EXISTS " +
                     "(SELECT 1 FROM t_locationinfo WHERE inumber=? AND itype=? AND ilocation=?))",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setInt(2, type);
                         ps.setString(3, location);
                         ps.setInt(4, 10); // default priority
                         ps.setTimestamp(5, now);
                         ps.setTimestamp(6, now);
                         ps.setInt(7, 1); // online
                         ps.setLong(8, inode.ino());
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
        _jdbc.update("DELETE FROM t_locationinfo WHERE inumber=? AND itype=? AND ilocation=?",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setInt(2, type);
                         ps.setString(3, location);
                     });
    }

    String[] tags(FsInode inode) {
        List<String> tags = _jdbc.queryForList("SELECT itagname FROM t_tags where inumber=?",
                                               String.class, inode.ino());
        return tags.toArray(new String[tags.size()]);
    }

    Map<String,byte[]> getAllTags(FsInode inode) {
        Map<String,byte[]> tags = new HashMap<>();
        _jdbc.query("SELECT t.itagname, i.ivalue, i.isize " +
                    "FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid WHERE t.inumber=?",
                    ps -> {
                        ps.setLong(1, inode.ino());
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
        return _jdbc.query("SELECT itagid FROM t_tags WHERE inumber=? AND itagname=?",
                           ps -> {
                               ps.setLong(1, dir.ino());
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
            _jdbc.update("UPDATE t_tags SET itagid=?,isorign=? WHERE inumber=? AND itagname=?",
                         ps -> {
                             ps.setLong(1, tagId);
                             ps.setInt(2, isOrign ? 1 : 0);
                             ps.setLong(3, dir.ino());
                             ps.setString(4, tagName);
                         });
        } else {
            _jdbc.update("INSERT INTO t_tags (inumber, itagid, isorign, itagname) VALUES(?,?,1,?)",
                         ps -> {
                             ps.setLong(1, dir.ino());
                             ps.setLong(2, tagId);
                             ps.setString(3, tagName);
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
        _jdbc.update("DELETE FROM t_tags WHERE inumber=? AND itagname=?", dir.ino(), tag);
    }

    void removeTag(FsInode dir) {
        /* Get the tag IDs of the tag links to be removed.
         */
        List<Long> ids = _jdbc.queryForList("SELECT itagid FROM t_tags WHERE inumber=?", Long.class, dir.ino());
        if (!ids.isEmpty()) {
            /* Remove the links.
             */
            _jdbc.update("DELETE FROM t_tags WHERE inumber=?", dir.ino());

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
                           "WHERE t.inumber=? AND t.itagname=?",
                           ps -> {
                               ps.setLong(1, inode.ino());
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
                                            ret.setIno(dir.ino());
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
        return _jdbc.query("SELECT isorign FROM t_tags WHERE inumber=? AND itagname=?",
                           ps -> {
                               ps.setLong(1, dir.ino());
                               ps.setString(2, tagName);
                           },
                           rs -> rs.next() && rs.getInt("isorign") == 1);
    }

    void createTags(FsInode inode, int uid, int gid, int mode, Map<String, byte[]> tags) {
        if (!tags.isEmpty()) {
            Map<String, Long> ids = new HashMap<>();
            tags.forEach((key, value) -> ids.put(key, createTagInode(uid, gid, mode, value)));
            _jdbc.batchUpdate("INSERT INTO t_tags (inumber,itagid,isorign,itagname) VALUES(?,?,1,?)",
                              ids.entrySet(),
                              ids.size(),
                              (ps, tag) -> {
                                  ps.setLong(1, inode.ino());
                                  ps.setLong(2, tag.getValue());
                                  ps.setString(3, tag.getKey());
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
        _jdbc.update("INSERT INTO t_tags (inumber,itagid,isorign,itagname) (SELECT ?,itagid,0,itagname from t_tags WHERE inumber=?)",
                     destination.ino(), orign.ino());
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
                     "(SELECT 1 FROM t_storageinfo WHERE inumber=?))",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setString(2, storageInfo.hsmName());
                         ps.setString(3, storageInfo.storageGroup());
                         ps.setString(4, storageInfo.storageSubGroup());
                         ps.setLong(5, inode.ino());
                     });
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
                    "SELECT ihsmName, istorageGroup, istorageSubGroup FROM t_storageinfo WHERE inumber=?",
                    (rs, rowNum) -> {
                        String hsmName = rs.getString("ihsmName");
                        String storageGroup = rs.getString("istoragegroup");
                        String storageSubGroup = rs.getString("istoragesubgroup");
                        return new InodeStorageInformation(inode, hsmName, storageGroup, storageSubGroup);
                    },
                    inode.ino());
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
        _jdbc.update("INSERT INTO t_inodes_checksum (inumber,itype,isum) (SELECT * FROM (VALUES (?,?,?)) v WHERE NOT EXISTS " +
                     "(SELECT 1 FROM t_inodes_checksum WHERE inumber=?))",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setInt(2, type);
                         ps.setString(3, value);
                         ps.setLong(4, inode.ino());
                     });
    }

    /**
     *
     * @param inode
     */
    List<Checksum> getInodeChecksums(FsInode inode) {
        return _jdbc.query("SELECT isum, itype FROM t_inodes_checksum WHERE inumber=?",
                           ps -> ps.setLong(1, inode.ino()),
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
            _jdbc.update("DELETE FROM t_inodes_checksum WHERE inumber=? AND itype=?",
                         ps -> {
                             ps.setLong(1, inode.ino());
                             ps.setInt(2, type);
                         });
        } else {
            _jdbc.update("DELETE FROM t_inodes_checksum WHERE inumber=?", inode);
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
                    parentInode = new FsInode(parentInode.getFs(), _root);
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
                    parentInode = new FsInode(parentInode.getFs(), _root);
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
        return _jdbc.query("SELECT * FROM t_acl WHERE inumber =  ? ORDER BY ace_order",
                           ps -> ps.setLong(1, inode.ino()),
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
        _jdbc.batchUpdate("INSERT INTO t_acl (inumber,rs_type,type,flags,access_msk,who,who_id,ace_order) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", acl, acl.size(),
                          new ParameterizedPreparedStatementSetter<ACE>()
                          {
                              int order = 0;

                              @Override
                              public void setValues(PreparedStatement ps, ACE ace) throws SQLException
                              {
                                  ps.setLong(1, inode.ino());
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

    boolean deleteAcl(FsInode inode) {
        return _jdbc.update("DELETE FROM t_acl WHERE inumber = ?", inode.ino()) > 0;
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
    static FsSqlDriver getDriverInstance(String dialect, DataSource dataSource) throws ChimeraFsException
    {
        String dialectDriverClass = "org.dcache.chimera." + dialect + "FsSqlDriver";

        try {
            return (FsSqlDriver) Class.forName(dialectDriverClass).getDeclaredConstructor(DataSource.class).newInstance(dataSource);
        } catch (InvocationTargetException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), ChimeraFsException.class);
            throw new RuntimeException("Failed to instantiate Chimera driver: " + e.getMessage(), e);
        } catch (NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException("Failed to instantiate Chimera driver: " + e.getMessage(), e);
        } catch (ClassNotFoundException e) {
            _log.info(dialectDriverClass + " not found, using default FsSQLDriver.");
            return new FsSqlDriver(dataSource);
        }
    }

    private PreparedStatement generateAttributeUpdateStatement(Connection dbConnection, FsInode inode, Stat stat, int level)
	    throws SQLException {

        if (stat.isDefined(Stat.StatAttributes.ATIME) && stat.getDefinedAttributeses().size() == 1) {
            /*
             * ATIME only update. The CTIME must stay unchanged.
             */
            PreparedStatement preparedStatement = dbConnection.prepareStatement(
                    "UPDATE t_inodes SET iatime=?,igeneration=igeneration+1 WHERE inumber=?");
            preparedStatement.setTimestamp(1, new Timestamp(stat.getATime()));
            preparedStatement.setLong(2, inode.ino());
            return preparedStatement;
        }


        final String attrUpdatePrefix =
                (level == 0)
                ? "UPDATE t_inodes SET ictime=?,igeneration=igeneration+1"
                : ("UPDATE t_level_" + level + " SET ictime=?");
        final String attrUpdateSuffix =
                (level == 0 && stat.isDefined(Stat.StatAttributes.SIZE))
                ? " WHERE inumber=? AND itype = " + UnixPermission.S_IFREG
                : " WHERE inumber=?";

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
        preparedStatement.setLong(idx++, inode.ino());
        return preparedStatement;
    }
}
