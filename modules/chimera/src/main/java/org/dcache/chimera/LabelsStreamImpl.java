/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2022 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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

public class LabelsStreamImpl {

    private static final String QUERY =
          "SELECT labelname FROM t_labels ";

    private final ResultSet _resultSet;
    private final JdbcTemplate _jdbc;
    private final Connection _connection;
    private final PreparedStatement _statement;

    LabelsStreamImpl(JdbcTemplate jdbc) {
        _jdbc = jdbc;

        Connection connection = null;
        PreparedStatement ps = null;
        ResultSet rs;
        try {
            connection = DataSourceUtils.getConnection(_jdbc.getDataSource());
            ps = connection.prepareStatement(QUERY);
            ps.setFetchSize(50);
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
