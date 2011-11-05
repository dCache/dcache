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

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.dcache.chimera.posix.Stat;
import org.dcache.commons.util.SqlHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryStreamImpl implements DirectoryStreamB<HimeraDirectoryEntry>,
        Iterator<HimeraDirectoryEntry> {

    private final static Logger _log = LoggerFactory.getLogger(DirectoryStreamImpl.class);
    private final Connection _con;
    private final PreparedStatement _st;
    private final ResultSet _listResultSet;
    private final FsInode _parent;
    private boolean _hasPendingElement;

    DirectoryStreamImpl(FsInode parent, Connection con, PreparedStatement st, ResultSet result) {
        _st = st;
        _con = con;
        _listResultSet = result;
        _parent = parent;
        _hasPendingElement = false;
    }

    @Override
    public Iterator<HimeraDirectoryEntry> iterator() {
        return this;
    }

    @Override
    public void close() throws IOException {
        SqlHelper.tryToClose(_listResultSet);
        SqlHelper.tryToClose(_st);
        try {
            _con.close();
        } catch (SQLException e) {
            _log.error("failed to close DB connection: " + e.getMessage());
            throw new IOException(e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {

        /*
         * To be complient with the semantics: calling hasNext() several times
         * shold not move cursor.
         *
         * Cache the result of the last call as long as next() was't called.
         */
        if (_hasPendingElement) {
            return true;
        }
        try {
            boolean hasNext = _listResultSet.next();
            if (hasNext) {
                _hasPendingElement = true;
            }
            return hasNext;
        } catch (SQLException ex) {
            _log.error("failed check for next entry: " + ex.getMessage());
            return false;
        }
    }

    @Override
    public HimeraDirectoryEntry next() {

        try {
            Stat stat = new Stat();
            stat.setSize(_listResultSet.getLong("isize"));
            stat.setATime(_listResultSet.getTimestamp("iatime").getTime());
            stat.setCTime(_listResultSet.getTimestamp("ictime").getTime());
            stat.setMTime(_listResultSet.getTimestamp("imtime").getTime());
            stat.setUid(_listResultSet.getInt("iuid"));
            stat.setGid(_listResultSet.getInt("igid"));
            stat.setMode(_listResultSet.getInt("imode") | _listResultSet.getInt("itype"));
            stat.setNlink(_listResultSet.getInt("inlink"));
            FsInode inode = new FsInode(_parent.getFs(), _listResultSet.getString("ipnfsid"));
            inode.setParent(_parent);
            stat.setIno((int) inode.id());
            stat.setDev(17);

            inode.setStatCache(stat);
            _hasPendingElement = false;
            return new HimeraDirectoryEntry(_listResultSet.getString("iname"), inode, stat);
        } catch (SQLException e) {
            _log.error("failed to fetch next entry: " + e.getMessage());
            throw new NoSuchElementException("Got SQL exception: " + e.getMessage());
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
