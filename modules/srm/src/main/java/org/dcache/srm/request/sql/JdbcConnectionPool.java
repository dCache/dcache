package org.dcache.srm.request.sql;

import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcConnectionPool
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(JdbcConnectionPool.class);

    private static final int MAX_CONNECTIONS = 50;

    private static final List<JdbcConnectionPool> pools = new ArrayList<>();

    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    private final DataSource dataSource;

    public static final synchronized JdbcConnectionPool getPool(String jdbcUrl,
                                                                String jdbcClass,
                                                                String user,
                                                                String pass) throws SQLException
    {
        for (JdbcConnectionPool pool : pools) {
            if (pool.jdbcClass.equals(jdbcClass) &&
                    pool.jdbcUrl.equals(jdbcUrl) &&
                    pool.pass.equals(pass) &&
                    pool.user.equals(user)) {
                return pool;
            }
        }
        JdbcConnectionPool pool = new JdbcConnectionPool(jdbcUrl, jdbcClass, user, pass);
        pools.add(pool);
        return pool;
    }

    protected JdbcConnectionPool(String jdbcUrl,
                                 String jdbcClass,
                                 String user,
                                 String pass) throws SQLException
    {
        this.jdbcUrl = checkNotNull(jdbcUrl);
        this.jdbcClass = checkNotNull(jdbcClass);
        this.user = checkNotNull(user);
        this.pass = checkNotNull(pass);

        try {
            Class.forName(jdbcClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException("Cannot initialize JDBC driver: " + jdbcClass);
        }

        final BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setIdleConnectionTestPeriodInMinutes(60);
        ds.setIdleMaxAgeInMinutes(240);
        ds.setMaxConnectionsPerPartition(MAX_CONNECTIONS);
        ds.setPartitionCount(1);
        ds.setAcquireIncrement(5);
        ds.setStatementsCacheSize(100);
        ds.setReleaseHelperThreads(3);

        dataSource = ds;

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                ds.close();
            }
        });
    }

    public Connection getConnection() throws SQLException
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
            LOGGER.debug("returnFailedConnection() exception: ", e);
        }
    }

    /**
     * should be called every time the connection is not in use anymore
     *
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
            LOGGER.debug("returnConnection() exception: ", e);
        }
    }
}

