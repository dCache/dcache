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
import java.sql.SQLException;


/**
 * DB2 specific SQL driver
 */
class DB2FsSqlDriver extends FsSqlDriver {

    private static final Logger _log = LoggerFactory.getLogger(DB2FsSqlDriver.class);

    /**
     *  this is a utility class which issues SQL queries on database
     *
     */
    protected DB2FsSqlDriver() {
        _log.info("Running DB2 specific Driver");
    }

    @Override
    void copyTags(Connection dbConnection, FsInode orign, FsInode destination) throws SQLException {
        // TODO: db2 needs some other solution
    }
}
