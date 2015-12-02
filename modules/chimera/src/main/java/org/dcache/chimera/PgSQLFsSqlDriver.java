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

import javax.sql.DataSource;

import java.io.File;
import java.sql.CallableStatement;
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
class PgSQLFsSqlDriver extends FsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(PgSQLFsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected PgSQLFsSqlDriver(DataSource dataSource) {
        super(dataSource);
        _log.info("Running PostgreSQL specific Driver");
    }

    @Override
    FsInode createInodeInParent(FsInode parent, String name, FsInode inode, int owner, int group, int mode, int type,
                                int nlink, long size)
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        _jdbc.execute("{ call f_create_inode(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) }",
                      (CallableStatement cs) -> {
                          cs.setString(1, parent.toString());
                          cs.setString(2, name);
                          cs.setString(3, inode.toString());
                          cs.setInt(4, type);
                          cs.setInt(5, mode & UnixPermission.S_PERMS);
                          cs.setInt(6, nlink);
                          cs.setInt(7, owner);
                          cs.setInt(8, group);
                          cs.setLong(9, size);
                          cs.setInt(10, _ioMode);
                          cs.setTimestamp(11, now);
                          cs.execute();
                          return null;
                      });

        Stat stat = new Stat();
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
        stat.setIno((int) inode.id());
        stat.setDev(17);
        stat.setRdev(13);

        return new FsInode(inode.getFs(), inode.toString(), inode.type(), inode.getLevel(), stat);
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
        return _jdbc.query("SELECT inode2path(?)",
                           ps -> ps.setString(1, inode.toString()),
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
            if (fileName.length() != 0) {
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
    FsInode path2inode(FsInode root, String path) {
        /* Ideally we would use the SQL array type for the second
         * parameter to inject the path elements, however there is no
         * easy way to do that with prepared statements. Hence we use
         * a slash delimited string instead. We cannot use
         * <code>path</code> as that uses the platform specific path
         * separator.
         */
        String normalizedPath = normalizePath(path);
        if (normalizedPath.length() == 0) {
            return root;
        }

        return _jdbc.query("SELECT path2inode(?, ?)",
                           ps -> {
                               ps.setString(1, root.toString());
                               ps.setString(2, normalizedPath);
                           },
                           rs -> {
                               if (rs.next()) {
                                   String id = rs.getString(1);
                                   if (id != null) {
                                       return new FsInode(root.getFs(), id);
                                   }
                               }
                               return null;
                           });
    }

    @Override
    List<FsInode> path2inodes(FsInode root, String path)
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
                "SELECT ipnfsid,isize,inlink,itype,imode,iuid,igid,iatime,ictime,imtime from path2inodes(?, ?)",
                ps -> {
                    ps.setString(1, root.toString());
                    ps.setString(2, normalizedPath);
                },
                (rs, rowNum) -> {
                    FsInode inode = new FsInode(root.getFs(), rs.getString("ipnfsid"));
                    Stat stat = new Stat();
                    stat.setSize(rs.getLong("isize"));
                    stat.setATime(rs.getTimestamp("iatime").getTime());
                    stat.setCTime(rs.getTimestamp("ictime").getTime());
                    stat.setMTime(rs.getTimestamp("imtime").getTime());
                    stat.setUid(rs.getInt("iuid"));
                    stat.setGid(rs.getInt("igid"));
                    stat.setMode(rs.getInt("imode") | rs.getInt("itype"));
                    stat.setNlink(rs.getInt("inlink"));
                    stat.setIno((int) inode.id());
                    stat.setDev(17);
                    inode.setStatCache(stat);
                    return inode;
                });
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags)
    {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update("INSERT INTO t_acl " +
                     "SELECT ?, ?, type, (flags | ?) # ?, access_msk, who, who_id, ace_order " +
                     "FROM t_acl WHERE rs_id = ? AND ((flags & ?) > 0)",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type.getValue());
                         ps.setInt(3, msk);
                         ps.setInt(4, msk);
                         ps.setString(5, source.toString());
                         ps.setInt(6, flgs);
                     });
    }

    @Override
    void createEntryInParent(FsInode parent, String name, FsInode inode) {
        int n = _jdbc.update("insert into t_dirs (iparent, iname, ipnfsid) " +
                             "(select ? as iparent, ? as iname, ? as ipnfsid " +
                             " where not exists (select 1 from t_dirs where iparent=? and iname=?))",
                             ps -> {
                                 ps.setString(1, parent.toString());
                                 ps.setString(2, name);
                                 ps.setString(3, inode.toString());
                                 ps.setString(4, parent.toString());
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
    void addInodeLocation(FsInode inode, int type, String location)
    {
        Timestamp now = new Timestamp(System.currentTimeMillis());
        _jdbc.update("INSERT INTO t_locationinfo (SELECT ?,?,?,?,?,?,? WHERE NOT EXISTS " +
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
}
