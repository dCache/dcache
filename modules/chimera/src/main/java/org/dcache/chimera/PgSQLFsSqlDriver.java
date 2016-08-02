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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;

import javax.sql.DataSource;

import java.io.File;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.posix.Stat;

/**
 * PostgreSQL specific
 *
 *
 */
public class PgSQLFsSqlDriver extends FsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(PgSQLFsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    public PgSQLFsSqlDriver(DataSource dataSource) throws ChimeraFsException
    {
        super(dataSource);
        _log.info("Running PostgreSQL specific Driver");
    }

    @Override
    protected FsInode createInodeInParent(FsInode parent, String name, String id, int owner, int group, int mode, int type,
                                          int nlink, long size) {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        Long inumber =
                _jdbc.query("SELECT f_create_inode(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            cs -> {
                                cs.setLong(1, parent.ino());
                                cs.setString(2, name);
                                cs.setString(3, id);
                                cs.setInt(4, type);
                                cs.setInt(5, mode & UnixPermission.S_PERMS);
                                cs.setInt(6, nlink);
                                cs.setInt(7, owner);
                                cs.setInt(8, group);
                                cs.setLong(9, size);
                                cs.setInt(10, _ioMode);
                                cs.setTimestamp(11, now);
                            },
                            rs -> rs.next() ? rs.getLong(1) : null);
        if (inumber == null) {
            throw new IncorrectUpdateSemanticsDataAccessException("f_create_inode failed to return an inumber.");
        }

        Stat stat = new Stat();
        stat.setIno(inumber);
        stat.setId(id);
        stat.setCrTime(now.getTime());
        stat.setGeneration(0);
        stat.setSize(size);
        stat.setATime(now.getTime());
        stat.setCTime(now.getTime());
        stat.setMTime(now.getTime());
        stat.setUid(owner);
        stat.setGid(group);
        stat.setMode(mode & UnixPermission.S_PERMS | type);
        stat.setNlink(nlink);
        stat.setDev(17);
        stat.setRdev(13);

        return new FsInode(parent.getFs(), inumber, FsInodeType.INODE, 0, stat);
    }

    @Override
    boolean removeInodeIfUnlinked(FsInode inode)
    {
        return _jdbc.update("DELETE FROM t_inodes WHERE inumber=? AND inlink = 0", inode.ino()) > 0;
    }

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param inode
     * @return
     */
    @Override
    String inode2path(FsInode inode, FsInode startFrom) {
        if (inode.equals(startFrom)) {
            return "/";
        }
        return _jdbc.query("SELECT inumber2path(?)",
                           ps -> ps.setLong(1, inode.ino()),
                           rs -> rs.next() ? rs.getString(1) : null);
    }

    /**
     * Returns a normalized path string for the given path. The
     * normalized string uses the slash character as a path separator,
     * it does not have a leading slash (i.e. it is relative), and it
     * has no empty path elements.
     */
    private String normalizePath(String path) {
        File file = new File(path);
        List<String> elements = new ArrayList<>();
        do {
            String fileName = file.getName();
            if (!fileName.isEmpty()) {
                /*
                 * skip multiple '/'
                 */
                elements.add(fileName);
            }

            file = file.getParentFile();
        } while (file != null);

        StringBuilder normalizedPath = new StringBuilder();
        if (!elements.isEmpty()) {
            normalizedPath.append(elements.get(elements.size() - 1));
            for (int i = elements.size() - 2; i >= 0; i--) {
                normalizedPath.append('/').append(elements.get(i));
            }
        }

        return normalizedPath.toString();
    }

    /**
     * get inode of given path starting <i>root</i> inode.
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     */
    @Override
    FsInode path2inode(FsInode root, String path) throws ChimeraFsException
    {
        /* Ideally we would use the SQL array type for the second
         * parameter to inject the path elements, however there is no
         * easy way to do that with prepared statements. Hence we use
         * a slash delimited string instead. We cannot use
         * <code>path</code> as that uses the platform specific path
         * separator.
         */
        String normalizedPath = normalizePath(path);
        if (normalizedPath.isEmpty()) {
            return root;
        }

        return _jdbc.query("SELECT path2inumber(?, ?)",
                           ps -> {
                               ps.setLong(1, root.ino());
                               ps.setString(2, normalizedPath);
                           },
                           rs -> {
                               if (rs.next()) {
                                   long id = rs.getLong(1);
                                   if (!rs.wasNull()) {
                                       return new FsInode(root.getFs(), id);
                                   }
                               }
                               return null;
                           });
    }

    @Override
    List<FsInode> path2inodes(FsInode root, String path) throws ChimeraFsException
    {
        /* Ideally we would use the SQL array type for the second
         * parameter to inject the path elements, however there is no
         * easy way to do that with prepared statements. Hence we use
         * a slash delimited string instead. We cannot use
         * <code>path</code> as that uses the platform specific path
         * separator.
         */
        String normalizedPath = normalizePath(path);

        if (normalizedPath.isEmpty()) {
            return Collections.singletonList(root);
        }

        return _jdbc.query(
                "SELECT inumber,ipnfsid,isize,inlink,itype,imode,iuid,igid,iatime,ictime,imtime from path2inodes(?, ?)",
                ps -> {
                    ps.setLong(1, root.ino());
                    ps.setString(2, normalizedPath);
                },
                (rs, rowNum) -> {
                    FsInode inode = new FsInode(root.getFs(), rs.getLong("inumber"));
                    Stat stat = new Stat();
                    stat.setIno(rs.getLong("inumber"));
                    stat.setId(rs.getString("ipnfsid"));
                    stat.setSize(rs.getLong("isize"));
                    stat.setATime(rs.getTimestamp("iatime").getTime());
                    stat.setCTime(rs.getTimestamp("ictime").getTime());
                    stat.setMTime(rs.getTimestamp("imtime").getTime());
                    stat.setUid(rs.getInt("iuid"));
                    stat.setGid(rs.getInt("igid"));
                    stat.setMode(rs.getInt("imode") | rs.getInt("itype"));
                    stat.setNlink(rs.getInt("inlink"));
                    stat.setDev(17);
                    inode.setStatCache(stat);
                    return inode;
                });
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags) {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update("INSERT INTO t_acl (inumber,rs_type,type,flags,access_msk,who,who_id,ace_order) " +
                     "SELECT ?, ?, type, (flags | ?) # ?, access_msk, who, who_id, ace_order " +
                     "FROM t_acl WHERE inumber = ? AND ((flags & ?) > 0)",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setInt(2, type.getValue());
                         ps.setInt(3, msk);
                         ps.setInt(4, msk);
                         ps.setLong(5, source.ino());
                         ps.setInt(6, flgs);
                     });
    }

    @Override
    void createEntryInParent(FsInode parent, String name, FsInode inode) {
        int n = _jdbc.update("insert into t_dirs (iparent, iname, ichild) " +
                             "(select ? as iparent, ? as iname, ? as ichild " +
                             " where not exists (select 1 from t_dirs where iparent=? and iname=?))",
                             ps -> {
                                 ps.setLong(1, parent.ino());
                                 ps.setString(2, name);
                                 ps.setLong(3, inode.ino());
                                 ps.setLong(4, parent.ino());
                                 ps.setString(5, name);
                             });
        if (n == 0) {
            /*
             * no updates as such entry already exists.
             * To be compatible with others, throw corresponding
             * DataAccessException.
             */
            throw new DuplicateKeyException("Entry already exists");
        }
    }

    @Override
    void addInodeLocation(FsInode inode, int type, String location) {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("INSERT INTO t_locationinfo (inumber,itype,ilocation,ipriority,ictime,iatime,istate) " +
                     "(SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS " +
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
}
