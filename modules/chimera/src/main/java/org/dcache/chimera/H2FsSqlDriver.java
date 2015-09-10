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

import org.springframework.jdbc.JdbcUpdateAffectedIncorrectNumberOfRowsException;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import javax.sql.DataSource;

import java.io.ByteArrayInputStream;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
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

    @Override
    long createTagInode(int uid, int gid, int mode)
    {
        final String CREATE_TAG_INODE_WITHOUT_VALUE = "INSERT INTO t_tags_inodes (imode, inlink, iuid, igid, isize, " +
                                                      "ictime, iatime, imtime, ivalue) VALUES (?,1,?,?,0,?,?,?,NULL)";

        Timestamp now = new Timestamp(System.currentTimeMillis());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = _jdbc.update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            CREATE_TAG_INODE_WITHOUT_VALUE, Statement.RETURN_GENERATED_KEYS);
                    ps.setInt(1, mode | UnixPermission.S_IFREG);
                    ps.setInt(2, uid);
                    ps.setInt(3, gid);
                    ps.setTimestamp(4, now);
                    ps.setTimestamp(5, now);
                    ps.setTimestamp(6, now);
                    return ps;
                }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(CREATE_TAG_INODE_WITHOUT_VALUE, 1, rc);
        }
        /* H2 uses weird names for the column with the auto-generated key, so we cannot use the code
         * in the base class.
         */
        return (Long) keyHolder.getKey();
    }

    @Override
    long createTagInode(int uid, int gid, int mode, byte[] value)
    {
        final String CREATE_TAG_INODE_WITH_VALUE = "INSERT INTO t_tags_inodes (imode, inlink, iuid, igid, isize, " +
                                                   "ictime, iatime, imtime, ivalue) VALUES (?,1,?,?,?,?,?,?,?)";

        Timestamp now = new Timestamp(System.currentTimeMillis());
        KeyHolder keyHolder = new GeneratedKeyHolder();
        int rc = _jdbc.update(
                con -> {
                    PreparedStatement ps = con.prepareStatement(
                            CREATE_TAG_INODE_WITH_VALUE, Statement.RETURN_GENERATED_KEYS);
                    ps.setInt(1, mode | UnixPermission.S_IFREG);
                    ps.setInt(2, uid);
                    ps.setInt(3, gid);
                    ps.setLong(4, value.length);
                    ps.setTimestamp(5, now);
                    ps.setTimestamp(6, now);
                    ps.setTimestamp(7, now);
                    ps.setBinaryStream(8, new ByteArrayInputStream(value), value.length);
                    return ps;
                }, keyHolder);
        if (rc != 1) {
            throw new JdbcUpdateAffectedIncorrectNumberOfRowsException(CREATE_TAG_INODE_WITH_VALUE, 1, rc);
        }
        /* H2 uses weird names for the column with the auto-generated key, so we cannot use the code
         * in the base class.
         */
        return (Long) keyHolder.getKey();
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
