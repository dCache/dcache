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
    protected OracleFsSqlDriver(DataSource dataSource) throws ChimeraFsException
    {
        super(dataSource);
        _log.info("Running Oracle specific Driver");
    }

    /**
     *
     * return the path associated with inode, starting from root of the tree.
     * in case of hard link, one of the possible paths is returned
     *
     * @param inode
     * @return
     */
    @Override
    String inode2path(FsInode inode, FsInode startFrom) {
        if (inode.equals(startFrom)) {
            return "/";
        }
        return _jdbc.query(
                "SELECT iname, LEVEL AS deep FROM (SELECT * FROM  t_dirs) start with ichild=? CONNECT BY  ichild = PRIOR iparent ORDER BY deep DESC",
                rs -> {
                    StringBuilder sb = new StringBuilder();
                    while (rs.next()) {
                        sb.append('/').append(rs.getString("iname"));
                    }
                    return sb.toString();
                },
                inode.ino());
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask, EnumSet<AceFlags> flags) {
        int msk = EnumSet.complementOf(mask).stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update("INSERT INTO t_acl (inumber,rs_type,type,flags,access_msk,who,who_id,ace_order) " +
                     "SELECT ?, ?, type, BITAND(flags, ?), access_msk, who, who_id, ace_order " +
                     "FROM t_acl WHERE inumber = ? AND BITAND(flags, ?) > 0",
                     ps -> {
                         ps.setLong(1, inode.ino());
                         ps.setInt(2, type.getValue());
                         ps.setInt(3, msk);
                         ps.setLong(4, source.ino());
                         ps.setInt(5, flgs);
                     });
    }
}
