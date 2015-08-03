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
    public boolean isForeignKeyError(SQLException e) {
        return "23506".endsWith(e.getSQLState());
    }
}
