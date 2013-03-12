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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.commons.util.SqlHelper;

/**
 * Oracle specific SQL driver
 *
 *
 */
class OracleFsSqlDriver extends FsSqlDriver {

    private static final Logger _log = LoggerFactory.getLogger(OracleFsSqlDriver.class);

    /**
     *  this is a utility class which is issues SQL queries on database
     *
     */
    protected OracleFsSqlDriver() {
        _log.info("Running Oracle specific Driver");
    }
    private static final String sqlInode2Path = "SELECT iname, LEVEL AS deep FROM (SELECT * FROM  t_dirs WHERE iname !='.' AND iname !='..') start with ipnfsid=? CONNECT BY  ipnfsid = PRIOR iparent ORDER BY deep DESC";

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param dbConnection
     * @param inode
     * @throws SQLException
     * @return
     */
    @Override
    String inode2path(Connection dbConnection, FsInode inode, FsInode startFrom, boolean inclusive) throws SQLException {

        String path = null;
        PreparedStatement ps = null;
        ResultSet result = null;

        try {

            ps = dbConnection.prepareStatement(sqlInode2Path);
            ps.setString(1, inode.toString());

            result = ps.executeQuery();

            StringBuilder sb = new StringBuilder();
            while (result.next()) {
                sb.append("/").append(result.getString("iname"));
            }

            path = sb.toString();

        } finally {
            SqlHelper.tryToClose(result);
            SqlHelper.tryToClose(ps);
        }

        return path;
    }
}
