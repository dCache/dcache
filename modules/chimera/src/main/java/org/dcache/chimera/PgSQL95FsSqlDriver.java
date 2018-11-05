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

import java.sql.Timestamp;
import org.dcache.chimera.posix.Stat;

import org.dcache.chimera.store.InodeStorageInformation;

/**
 * PostgreSQL 9.5 and later specific
 *
 *
 */
public class PgSQL95FsSqlDriver extends PgSQLFsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(PgSQL95FsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    public PgSQL95FsSqlDriver(DataSource dataSource) throws ChimeraFsException
    {
        super(dataSource);
        _log.info("Running PostgreSQL >= 9.5 specific Driver");
    }

        @Override
    protected FsInode createInodeInParent(FsInode parent, String name, String id, int owner, int group, int mode, int type,
                                          int nlink, long size) {
        Timestamp now = new Timestamp(System.currentTimeMillis());

        Long inumber =
                _jdbc.query("SELECT f_create_inode_95(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
        if (inumber == null || inumber == 0L) {
            throw new DuplicateKeyException("Entry already exists");
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
        int n = _jdbc.update("INSERT INTO t_dirs (iparent, iname, ichild) VALUES(?,?,?) ON CONFLICT ON CONSTRAINT t_dirs_pkey DO NOTHING",
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

    /**
      *
      * adds a new location for the inode
      *
      * @param inode
      * @param type
      * @param location
      */
    void addInodeLocation(FsInode inode, int type, String location) {
        _jdbc.update("INSERT INTO t_locationinfo (inumber,itype,ilocation,ipriority,ictime,iatime,istate) VALUES(?,?,?,?,?,?,?) " +
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
}
