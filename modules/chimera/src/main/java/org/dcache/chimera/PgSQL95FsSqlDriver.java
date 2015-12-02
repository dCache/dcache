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

import org.dcache.chimera.store.InodeStorageInformation;

/**
 * PostgreSQL 9.5 and later specific
 *
 *
 */
class PgSQL95FsSqlDriver extends PgSQLFsSqlDriver {

    /**
     * logger
     */
    private static final Logger _log = LoggerFactory.getLogger(PgSQL95FsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected PgSQL95FsSqlDriver(DataSource dataSource) {
        super(dataSource);
        _log.info("Running PostgreSQL >= 9.5 specific Driver");
    }


    @Override
    void createEntryInParent(FsInode parent, String name, FsInode inode) {
        int n = _jdbc.update("INSERT INTO t_dirs VALUES(?,?,?) ON CONFLICT ON CONSTRAINT t_dirs_pkey DO NOTHING",
                             ps -> {
                                 ps.setString(1, parent.toString());
                                 ps.setString(2, name);
                                 ps.setString(3, inode.toString());
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
        _jdbc.update("INSERT INTO t_locationinfo VALUES(?,?,?,?,?,?,?) " +
                     "ON CONFLICT ON CONSTRAINT t_locationinfo_pkey DO NOTHING",
                     ps -> {
                         Timestamp now = new Timestamp(System.currentTimeMillis());
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setString(3, location);
                         ps.setInt(4, 10); // default priority
                         ps.setTimestamp(5, now);
                         ps.setTimestamp(6, now);
                         ps.setInt(7, 1); // online
                     });
    }

    @Override
    void setStorageInfo(FsInode inode, InodeStorageInformation storageInfo)
    {
        _jdbc.update("INSERT INTO t_storageinfo VALUES (?,?,?,?) " +
                     "ON CONFLICT ON CONSTRAINT t_storageinfo_pkey DO NOTHING",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setString(2, storageInfo.hsmName());
                         ps.setString(3, storageInfo.storageGroup());
                         ps.setString(4, storageInfo.storageSubGroup());
                     });
    }

    @Override
    void setInodeChecksum(FsInode inode, int type, String value)
    {
        _jdbc.update("INSERT INTO t_inodes_checksum VALUES (?,?,?) " +
                     "ON CONFLICT ON CONSTRAINT t_inodes_checksum_pkey DO NOTHING",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type);
                         ps.setString(3, value);
                     });

    }
}
