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
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dcache.commons.util.SqlHelper;

public class HsqlDBFsSqlDriver extends FsSqlDriver {

    private static final String srmGetTagsIdsOfPnfsid = "SELECT itagid FROM t_tags WHERE ipnfsid=?";
    private static final String sqlRemoveTag = "DELETE FROM t_tags WHERE ipnfsid=?";
    private static final String sqlRemoveTagInodes = "DELETE FROM t_tags_inodes i WHERE itagid = ? AND NOT EXISTS (SELECT 1 FROM t_tags t WHERE t.itagid=i.itagid LIMIT 1)";

    @Override
    void removeTag(Connection dbConnection, FsInode dir) throws SQLException {

        /* The sqlRemoveTagInodes statement above relies on concurrent transactions not deleting
         * other links to affected tag inodes. Otherwise we could come into a situation in which
         * two concurrent transactions remove two links to the same inode, yet none of them realize
         * that the inode is left without links (as there is another link).
         *
         * One way to ensure this would be to use repeatable read transaction isolation, but
         * PostgreSQL doesn't support changing the isolation level in the middle of a transaction.
         * Always running any operation that might call this method with repeatable read was deemed
         * unacceptible. Another solution would be to lock that tag inode at the beginning of
         * this method using SELECT FOR UPDATE. This would be fairly expensive way of solving
         * this race.
         *
         * For now we decide to ignore the race: It seems unlikely to run into and even
         * if one does, the consequence is merely an orphaned inode.
         */

        PreparedStatement ps1 = null, ps2 = null, ps3 = null;
        ResultSet rs = null;
        try {
            /* Get the tag IDs of the tag links to be removed.
             */
            ps1 = dbConnection.prepareStatement(srmGetTagsIdsOfPnfsid);
            ps1.setString(1, dir.toString());
            rs = ps1.executeQuery();

            if (rs.next()) {
                /* Remove the links.
                 */
                ps2 = dbConnection.prepareStatement(sqlRemoveTag);
                ps2.setString(1, dir.toString());
                ps2.executeUpdate();

                /* Remove any tag inode of of the tag links removed above, which
                 * are not referenced by any other links either.
                 *
                 * We ought to maintain the link count in the inode, but Chimera
                 * has not done so in the past. In the interest of avoiding costly
                 * schema corrections in patch level releases, the current solution
                 * queries for the existence of other links instead.
                 */
                ps3 = dbConnection.prepareStatement(sqlRemoveTagInodes);
                do {
                    ps3.setString(1, rs.getString(1));
                    ps3.addBatch();
                } while (rs.next());
                ps3.executeBatch();
            }
        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(ps1);
            SqlHelper.tryToClose(ps2);
            SqlHelper.tryToClose(ps3);
        }
    }

    /**
     *
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param dbConnection
     * @param orign
     * @param destination
     * @throws SQLException
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
