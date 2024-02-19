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

import java.util.EnumSet;
import javax.sql.DataSource;
import org.dcache.acl.enums.AceFlags;
import org.dcache.acl.enums.RsType;

public class HsqlDBFsSqlDriver extends FsSqlDriver {

    public HsqlDBFsSqlDriver(DataSource dataSource) throws ChimeraFsException {
        super(dataSource);
    }

    @Override
    void copyAcl(FsInode source, FsInode inode, RsType type, EnumSet<AceFlags> mask,
          EnumSet<AceFlags> flags) {
        int msk = mask.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        int flgs = flags.stream().mapToInt(AceFlags::getValue).reduce(0, (a, b) -> a | b);
        _jdbc.update(
              "INSERT INTO t_acl (inumber,rs_type,type,flags,access_msk,who,who_id,ace_order) " +
                    "SELECT ?, ?, type, BITANDNOT(flags, ?), access_msk, who, who_id, ace_order " +
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
