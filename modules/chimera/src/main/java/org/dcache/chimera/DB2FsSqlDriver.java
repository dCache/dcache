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

import javax.sql.DataSource;

import java.util.EnumSet;

import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;


/**
 * DB2 specific SQL driver
 */
class DB2FsSqlDriver extends FsSqlDriver {

    private static final Logger _log = LoggerFactory.getLogger(DB2FsSqlDriver.class);

    /**
     *  this is a utility class which issues SQL queries on database
     *
     */
    protected DB2FsSqlDriver(DataSource dataSource) {
        super(dataSource);
        _log.info("Running DB2 specific Driver");
    }

    @Override
    void copyTags(FsInode orign, FsInode destination) {
        // TODO: db2 needs some other solution
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
