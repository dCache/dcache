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
import java.io.File;
import java.net.SocketException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import javax.sql.DataSource;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;
import org.dcache.chimera.posix.Stat;
import org.dcache.chimera.store.InodeStorageInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.IncorrectUpdateSemanticsDataAccessException;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * PostgreSQL 9.5 and later specific
 */
public class PgSQL95FsSqlDriver extends FsSqlDriver {

    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(PgSQL95FsSqlDriver.class);

    /**
     * this is a utility class which is issues SQL queries on database
     */
    public PgSQL95FsSqlDriver(DataSource dataSource) throws ChimeraFsException {
        super(dataSource);
        LOGGER.info("Running PostgreSQL >= 9.5 specific Driver");
    }


    protected FsInode createInodeInParent(FsInode parent, String name, String id, int owner,
          int group, int mode, int type,
          int nlink, long size) {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        Long inumber =
              _jdbc.query("SELECT f_create_inode95(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
                        cs.setInt(10, FileState.CREATED.getValue());
                        cs.setTimestamp(11, now);
                    },
                    rs -> rs.next() ? rs.getLong(1) : null);

        if (inumber == null) {
            throw new IncorrectUpdateSemanticsDataAccessException(
                  "f_create_inode failed to return an inumber.");
        }

        if (inumber == -1L) {
            throw new DuplicateKeyException("File exists");
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
        stat.setState(FileState.CREATED);

        return new FsInode(parent.getFs(), inumber, FsInodeType.INODE, 0, stat);
    }

    @Override
    void createEntryInParent(FsInode parent, String name, FsInode inode) {
        int n = _jdbc.update(
              "INSERT INTO t_dirs (iparent, iname, ichild) VALUES(?,?,?) ON CONFLICT ON CONSTRAINT t_dirs_pkey DO NOTHING",
              ps -> {
                  ps.setLong(1, parent.ino());
                  ps.setString(2, name);
                  ps.setLong(3, inode.ino());
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
    boolean removeInodeIfUnlinked(FsInode inode) {
        return _jdbc.update("DELETE FROM t_inodes WHERE inumber=? AND inlink = 0", inode.ino()) > 0;
    }

    /**
     * return the path associated with inode, starting from root of the tree. in case of hard link,
     * one of the possible paths is returned
     *
     * @param inode
     * @return
     */
    @Override
    protected String inode2path(long inode, long startFrom) {
        if (inode == startFrom) {
            return "/";
        }
        return _jdbc.query("SELECT inumber2path(?)",
              ps -> ps.setLong(1, inode),
              rs -> rs.next() ? rs.getString(1) : null);
    }

    @Override
    public List<OriginTag> findTags(String name) {
        return _jdbc.query("SELECT inumber2path(inumber),ivalue"
                    + " FROM t_tags t JOIN t_tags_inodes i ON t.itagid = i.itagid"
                    + " WHERE itagname=? AND isorign=1",
              ps -> ps.setString(1, name),
              (rs, row) -> {
                  String inumber2Path = rs.getString(1);
                  String path = inumber2Path.isEmpty() ? "/"
                        : inumber2Path; // Work around bug in inumber2path
                  return new OriginTag(path, rs.getBytes(2));
              });
    }

    /**
     * Returns a normalized path string for the given path. The normalized string uses the slash
     * character as a path separator, it does not have a leading slash (i.e. it is relative), and it
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
     *
     * @param root staring point
     * @param path
     * @return inode or null if path does not exist.
     */
    @Override
    FsInode path2inode(FsInode root, String path) throws ChimeraFsException {
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
    List<FsInode> path2inodes(FsInode root, String path) throws ChimeraFsException {
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
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask,
          EnumSet<AceFlags> flags) {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update(
              "INSERT INTO t_acl (inumber,rs_type,type,flags,access_msk,who,who_id,ace_order) " +
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

    /**
     * adds a new location for the inode
     *
     * @param inode
     * @param type
     * @param location
     */
    void addInodeLocation(FsInode inode, int type, String location) {
        _jdbc.update(
              "INSERT INTO t_locationinfo (inumber,itype,ilocation,ipriority,ictime,iatime,istate) VALUES(?,?,?,?,?,?,?) "
                    +
                    "ON CONFLICT ON CONSTRAINT t_locationinfo_pkey DO NOTHING",
              ps -> {
                  Timestamp now = new Timestamp(System.currentTimeMillis());
                  ps.setLong(1, inode.ino());
                  ps.setInt(2, type);
                  ps.setString(3, location);
                  ps.setInt(4, 10); // default priority
                  ps.setTimestamp(5, now);
                  ps.setTimestamp(6, now);
                  ps.setInt(7, 1); // online
              });
    }


    /**
     * Attache a given label to  a given file system object.
     *
     * @param inode     file system object.
     * @param labelname label name.
     * @throws ChimeraFsException
     */
    void addLabel(FsInode inode, String labelname) throws ChimeraFsException {

        KeyHolder keyHolder = new GeneratedKeyHolder();
        try {

            int n = _jdbc.update(
                  con -> {
                      PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO t_labels ( labelname) VALUES (?)"
                                  + "ON CONFLICT ON CONSTRAINT labelname DO NOTHING",
                            Statement.RETURN_GENERATED_KEYS);
                      ps.setString(1, labelname);

                      return ps;
                  }, keyHolder);
            if (n != 0) {
                Long label_id = (Long) keyHolder.getKeys().get("label_id");

                _jdbc.update("INSERT INTO t_labels_ref (label_id, inumber) VALUES (?,?)",
                      label_id, inode.ino());

            } else {


                Long label_id = getLabel(labelname);

                _jdbc.update(
                      "INSERT INTO t_labels_ref (label_id, inumber) (SELECT * FROM (VALUES (?,?)) "
                            + "ON CONFLICT ON CONSTRAINT i_label_pkey DO NOTHING",

                      ps -> {
                          ps.setLong(1, label_id);
                          ps.setLong(2, inode.ino());
                          ps.setLong(3, label_id);
                          ps.setLong(4, inode.ino());

                      });


            }

        } catch (EmptyResultDataAccessException e) {
            throw new NoLabelChimeraException(labelname);
        }

    }

    @Override
    void copyTags(FsInode orign, FsInode destination) {
        _jdbc.queryForList(
                    "INSERT INTO t_tags (inumber,itagid,isorign,itagname) (SELECT ?,itagid,0,itagname FROM t_tags WHERE inumber=?) RETURNING itagid",
                    Long.class, destination.ino(), orign.ino()).
              forEach(tagId -> {
                  _jdbc.update("UPDATE t_tags_inodes SET inlink = inlink + 1 WHERE itagid=?",
                        tagId);
              });
    }

    @Override
    void removeTag(FsInode dir) {
        _jdbc.queryForList("DELETE FROM t_tags WHERE inumber=? RETURNING itagid", Long.class,
                    dir.ino())
              .forEach(tagId -> {
                  // shortcut: delete right away, if there is only one reference left
                  int n = _jdbc.update("DELETE FROM t_tags_inodes WHERE itagid=? AND inlink = 1",
                        tagId);
                  // if delete didn't happen, then just indicate that one reference in gone
                  if (n == 0) {
                      _jdbc.update("UPDATE t_tags_inodes SET inlink = inlink - 1 WHERE itagid=?",
                            tagId);
                  }
              });
    }

    @Override
    void removeTag(FsInode dir, String tag) {

        Long tagId = _jdbc.query("DELETE FROM t_tags WHERE inumber=? AND itagname=? RETURNING *",
              ps -> {
                  ps.setLong(1, dir.ino());
                  ps.setString(2, tag);
              },
              (ResultSet rs) -> rs.next() ? rs.getLong("itagid") : null);

        // TODO: explore a possibility to perform DELETE+UPDATE with single query
        if (tagId != null) {
            // shortcut: delete right away, if there is only one reference left
            int n = _jdbc.update("DELETE FROM t_tags_inodes WHERE itagid=? AND inlink = 1", tagId);
            // if delete didn't happen, then just indicate that one reference in gone
            if (n == 0) {
                _jdbc.update("UPDATE t_tags_inodes SET inlink = inlink - 1 WHERE itagid=?", tagId);
            }
        }
    }

    @Override
    void setStorageInfo(FsInode inode, InodeStorageInformation storageInfo) {
        _jdbc.update("INSERT INTO t_storageinfo VALUES (?,?,?,?) " +
                    "ON CONFLICT ON CONSTRAINT t_storageinfo_pkey DO NOTHING",
              ps -> {
                  ps.setLong(1, inode.ino());
                  ps.setString(2, storageInfo.hsmName());
                  ps.setString(3, storageInfo.storageGroup());
                  ps.setString(4, storageInfo.storageSubGroup());
              });
    }

    @Override
    void setInodeChecksum(FsInode inode, int type, String value) {
        _jdbc.update("INSERT INTO t_inodes_checksum (inumber,itype,isum) VALUES (?,?,?) " +
                    "ON CONFLICT ON CONSTRAINT t_inodes_checksum_pkey DO NOTHING",
              ps -> {
                  ps.setLong(1, inode.ino());
                  ps.setInt(2, type);
                  ps.setString(3, value);
              });
    }

    @Override
    void setXattr(FsInode inode, String attr, byte[] value, FileSystemProvider.SetXattrMode mode)
          throws ChimeraFsException {

        switch (mode) {
            case CREATE: {
                int n = _jdbc.update("INSERT INTO t_xattr (inumber, ikey, ivalue) VALUES (?,?,?)" +
                            "ON CONFLICT ON CONSTRAINT i_xattr_pkey DO NOTHING",
                      inode.ino(), attr, value);
                if (n == 0) {
                    throw new FileExistsChimeraFsException();
                }
                break;
            }
            case REPLACE: {
                int n = _jdbc.update(
                      "UPDATE t_xattr SET ivalue = ? WHERE  inumber = ? AND ikey = ?",
                      value, inode.ino(), attr);
                if (n == 0) {
                    throw new NoXdataChimeraException(attr);
                }
                break;
            }
            case EITHER: {
                _jdbc.update("INSERT INTO t_xattr (inumber, ikey, ivalue) VALUES (?,?,?)" +
                            "ON CONFLICT ON CONSTRAINT i_xattr_pkey DO UPDATE SET ivalue = EXCLUDED.ivalue",
                      inode.ino(), attr, value);
            }
        }

        // trigger generation update
        setInodeAttributes(inode, 0, new Stat());
    }

    /**
     * Update file system cache table.
     */

    @Override
    void updateFsStat() {
        try {
            _jdbc.update("UPDATE t_fstat SET iusedFiles = t.usedFiles, " +
                  "iusedSpace = t.usedSpace " +
                  "FROM (SELECT count(*) AS usedFiles, " +
                  "SUM(isize) AS usedSpace FROM t_inodes " +
                  "WHERE itype=32768) as t");
        } catch (DataAccessException e) {
            Throwable cause = Throwables.getRootCause(e);
            if (cause instanceof SocketException) {
                LOGGER.warn("FS stat update interrupted {}", e.getMessage());
            }
        }
    }
}
