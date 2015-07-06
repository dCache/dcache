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

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.dcache.acl.ACE;
import org.dcache.chimera.posix.Stat;
import org.dcache.commons.util.SqlHelper;

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
    protected PgSQL95FsSqlDriver() {
        _log.info("Running PostgreSQL >= 9.5 specific Driver");
    }


    private static final String sqlCreateEntryInParent = "INSERT INTO t_dirs VALUES(?,?,?) " +
            "ON CONFLICT ON CONSTRAINT t_dirs_pkey DO NOTHING";

    @Override
    void createEntryInParent(Connection dbConnection, FsInode parent, String name, FsInode inode) throws SQLException {
        PreparedStatement stInserIntoParent = null;
        try {

            stInserIntoParent = dbConnection.prepareStatement(sqlCreateEntryInParent);
            stInserIntoParent.setString(1, parent.toString());
            stInserIntoParent.setString(2, name);
            stInserIntoParent.setString(3, inode.toString());
            int n = stInserIntoParent.executeUpdate();
            if (n == 0) {
                /*
                 * no updates as such entry already exists.
                 * To be compatible with others, throw corresponding
                 * SQL exception.
                 */
                throw new SQLException("Entry already exists", DUPLICATE_KEY_ERROR);
            }

        } finally {
            SqlHelper.tryToClose(stInserIntoParent);
        }
    }

    private static final String sqlAddInodeLocation = "INSERT INTO t_locationinfo VALUES(?,?,?,?,?,?,?) " +
            "ON CONFLICT ON CONSTRAINT t_locationinfo_pkey DO NOTHING";

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

}
