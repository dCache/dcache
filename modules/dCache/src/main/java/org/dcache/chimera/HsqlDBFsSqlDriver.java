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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.dcache.commons.util.SqlHelper;

public class HsqlDBFsSqlDriver extends FsSqlDriver {

    /**
     *
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param dbConnection
     * @param orign
     * @param destination
     * @throws java.sql.SQLException
     */
    @Override
    void copyTags(Connection dbConnection, FsInode orign, FsInode destination) throws SQLException {

        String sqlCopyTag = "INSERT INTO t_tags ( SELECT ? , itagname, itagid, 0 from t_tags WHERE ipnfsid=?)";

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
}
