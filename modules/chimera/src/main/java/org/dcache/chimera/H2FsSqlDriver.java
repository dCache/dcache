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

import java.sql.SQLException;
import java.util.EnumSet;

import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;


/**
 * H2 database specific dialect.
 */
public class H2FsSqlDriver extends FsSqlDriver {

    protected H2FsSqlDriver(DataSource dataSource)
    {
        super(dataSource);
    }

    /**
     * copy all directory tags from origin directory to destination. New copy marked as inherited.
     *
     * @param orign
     * @param destination
     */
    @Override
    void copyTags(FsInode orign, FsInode destination) {
        _jdbc.update("INSERT INTO t_tags ( SELECT '" + destination.toString() + "' , itagname, itagid, 0 from t_tags WHERE ipnfsid=?)",
                     orign.toString());
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags)
    {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update("INSERT INTO t_acl " +
                     "SELECT ?, ?, type, BITXOR(BITOR(flags, ?), ?), access_msk, who, who_id, ace_order " +
                     "FROM t_acl WHERE rs_id = ? AND BITAND(flags, ?) > 0",
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
    public boolean isForeignKeyError(SQLException e) {
        return "23506".endsWith(e.getSQLState());
    }
}
