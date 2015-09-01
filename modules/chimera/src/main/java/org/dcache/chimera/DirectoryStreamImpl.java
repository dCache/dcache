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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.dcache.chimera.posix.Stat;

public class DirectoryStreamImpl implements DirectoryStreamB<HimeraDirectoryEntry>,
        Iterator<HimeraDirectoryEntry> {

    private static final Logger _log = LoggerFactory.getLogger(DirectoryStreamImpl.class);

    private static final String QUERY =
            "SELECT i.ipnfsid, d.iname, i.isize, i.inlink, i.imode, i.itype, " +
            "i.iuid, i.igid, i.iatime, i.ictime, i.imtime, i.icrtime, i.igeneration " +
            "FROM t_inodes i JOIN t_dirs d ON i.ipnfsid = d.ipnfsid WHERE iparent=? " +
            "UNION ALL " +
            "SELECT i.ipnfsid, '.', i.isize, i.inlink, i.imode, i.itype, " +
            "i.iuid, i.igid, i.iatime, i.ictime, i.imtime, i.icrtime, i.igeneration " +
            "FROM t_inodes i WHERE i.ipnfsid=? " +
            "UNION ALL " +
            "SELECT i.ipnfsid, '..', i.isize, i.inlink, i.imode, i.itype, " +
            "i.iuid, i.igid, i.iatime, i.ictime, i.imtime, i.icrtime, i.igeneration " +
            "FROM t_inodes i JOIN t_dirs d ON i.ipnfsid = d.iparent WHERE d.ipnfsid=?";

    private final ResultSet _resultSet;
    private final FsInode _dir;
    private final JdbcTemplate _jdbc;
    private final Connection _connection;
    private final PreparedStatement _statement;
    private boolean _hasPendingElement;

    DirectoryStreamImpl(FsInode dir, JdbcTemplate jdbc) {
        _dir = dir;
        _hasPendingElement = false;
        _jdbc = jdbc;

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs;
        try {
            connection = DataSourceUtils.getConnection(_jdbc.getDataSource());
            ps = connection.prepareStatement(QUERY);
            ps.setFetchSize(50);
            ps.setString(1, dir.toString());
            ps.setString(2, dir.toString());
            ps.setString(3, dir.toString());
            rs = ps.executeQuery();
        } catch (SQLException ex) {
            JdbcUtils.closeStatement(ps);
            DataSourceUtils.releaseConnection(connection, _jdbc.getDataSource());
            throw _jdbc.getExceptionTranslator().translate("StatementExecution", QUERY, ex);
        }
        _connection = connection;
        _resultSet = rs;
        _statement = ps;
    }

    @Override
    public void close() throws IOException
    {
        try {
            JdbcUtils.closeResultSet(_resultSet);
            JdbcUtils.closeStatement(_statement);
            DataSourceUtils.releaseConnection(_connection, _jdbc.getDataSource());
        } catch (DataAccessException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public Iterator<HimeraDirectoryEntry> iterator() {
        return this;
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
            boolean hasNext = _resultSet.next();
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
            stat.setSize(_resultSet.getLong("isize"));
            stat.setATime(_resultSet.getTimestamp("iatime").getTime());
            stat.setCTime(_resultSet.getTimestamp("ictime").getTime());
            stat.setMTime(_resultSet.getTimestamp("imtime").getTime());
            stat.setCrTime(_resultSet.getTimestamp("icrtime").getTime());
            stat.setUid(_resultSet.getInt("iuid"));
            stat.setGid(_resultSet.getInt("igid"));
            stat.setMode(_resultSet.getInt("imode") | _resultSet.getInt("itype"));
            stat.setNlink(_resultSet.getInt("inlink"));
            stat.setGeneration(_resultSet.getInt("igeneration"));
            FsInode inode = new FsInode(_dir.getFs(), _resultSet.getString("ipnfsid"));
            inode.setParent(_dir);
            stat.setIno((int) inode.id());
            stat.setDev(17);
            stat.setRdev(13);

            inode.setStatCache(stat);
            _hasPendingElement = false;
            return new HimeraDirectoryEntry(_resultSet.getString("iname"), inode, stat);
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
