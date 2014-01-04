package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcConnectionPool
{
    private final static Logger _logSql =
        LoggerFactory.getLogger(JdbcConnectionPool.class);

    private final DataSource dataSource;

    public JdbcConnectionPool(DataSource dataSource)
    {
        this.dataSource = checkNotNull(dataSource);
    }

    /**
     * we use getConnection and return connection to assure that the
     * same connection is not used to do more then one thing at a time
     */
    public  Connection getConnection() throws SQLException
    {
        Connection con = dataSource.getConnection();
        con.setAutoCommit(false);
        return con;
    }

    public void returnFailedConnection(Connection con)
    {
        try {
            con.rollback();
        } catch (SQLException e) {
        }

        try {
            con.close();
        } catch (SQLException e) {
            _logSql.debug("returnFailedConnection() exception: ", e);

        }
    }

    /**
     * should be called every time the connection is not in use anymore
     * @param con Connection that is not in use anymore
     */
    public void returnConnection(Connection con)
    {
        try {
            con.commit();
        } catch (SQLException e) {
        }
        try {
            con.close();
        } catch (SQLException e) {
            _logSql.debug("returnConnection() exception: ", e);
        }
    }
}
