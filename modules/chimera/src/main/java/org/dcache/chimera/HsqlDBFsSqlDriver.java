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

import javax.sql.DataSource;

import java.util.EnumSet;
import java.util.List;

import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;

public class HsqlDBFsSqlDriver extends FsSqlDriver {

    protected HsqlDBFsSqlDriver(DataSource dataSource)
    {
        super(dataSource);
    }

    @Override
    void removeTag(FsInode dir)
    {
        /* Get the tag IDs of the tag links to be removed.
         */
        List<String> ids = _jdbc.queryForList("SELECT itagid FROM t_tags WHERE ipnfsid=?", String.class, dir.toString());
        if (!ids.isEmpty()) {
            /* Remove the links.
             */
            _jdbc.update("DELETE FROM t_tags WHERE ipnfsid=?", dir.toString());

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
            _jdbc.batchUpdate("DELETE FROM t_tags_inodes i WHERE itagid = ? " +
                              "AND NOT EXISTS (SELECT 1 FROM t_tags t WHERE t.itagid=i.itagid LIMIT 1)",
                              ids, ids.size(),
                              (ps, tagid) -> ps.setString(1, tagid));
        }
    }

    /**
     *
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param orign
     * @param destination
     */
    @Override
    void copyTags(FsInode orign, FsInode destination) {
        _jdbc.update("INSERT INTO t_tags ( SELECT ? , itagname, itagid, 0 from t_tags WHERE ipnfsid=?)",
                     destination.toString(), orign.toString());
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags)
    {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update("INSERT INTO t_acl " +
                     "SELECT ?, ?, type, BITANDNOT(flags, ?), access_msk, who, who_id, ace_order " +
                     "FROM t_acl WHERE rs_id = ? AND BITAND(flags, ?) > 0",
                     ps -> {
                         ps.setString(1, inode.toString());
                         ps.setInt(2, type.getValue());
                         ps.setInt(3, msk);
                         ps.setString(4, source.toString());
                         ps.setInt(5, flgs);
                     });
    }
}
