
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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.jdbc.support.JdbcUtils;

public class VirtualDirectoryStreamImpl {

    private static final String QUERY =

          "SELECT i.*, d.iname || '-' || d.iparent  as filename, i.inumber as fileid  FROM t_inodes i JOIN t_dirs d ON  i.inumber = d.ichild WHERE d.ichild IN"
                +

                "(SELECT   ichild  FROM t_dirs," +
                " ( SELECT label_id FROM t_labels  WHERE labelname= ?) as nested " +

                "WHERE ichild IN ( select inumber from  t_labels_ref   where label_id = nested.label_id))";


    private final ResultSet _resultSet;
    private final JdbcTemplate _jdbc;
    private final Connection _connection;
    private final PreparedStatement _statement;

    VirtualDirectoryStreamImpl(String labelname, JdbcTemplate jdbc) {

        _jdbc = jdbc;

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs;
        try {
            connection = DataSourceUtils.getConnection(_jdbc.getDataSource());
            ps = connection.prepareStatement(QUERY);
            ps.setFetchSize(50);
            ps.setString(1, labelname);
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

    public void close() throws IOException {
        try {
            JdbcUtils.closeResultSet(_resultSet);
            JdbcUtils.closeStatement(_statement);
            DataSourceUtils.releaseConnection(_connection, _jdbc.getDataSource());
        } catch (DataAccessException e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public ResultSet next() throws SQLException {
        return _resultSet.next() ? _resultSet : null;
    }
}
