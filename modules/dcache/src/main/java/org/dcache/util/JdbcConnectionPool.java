package org.dcache.util;

import com.jolbox.bonecp.BoneCPDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcConnectionPool
{
    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    private final DataSource dataSource;
    private static Logger _logSql =
        LoggerFactory.getLogger(JdbcConnectionPool.class);

    private static final Set<JdbcConnectionPool> pools =
        new HashSet<>();

    /**
     * DataSource should not be closed.
     *
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @throws SQLException if the underlying jdbc code throws exception
     * @return DataSource
     */
    public synchronized static final DataSource
        getDataSource(String jdbcUrl,
    String jdbcClass,
    String user,
                      String pass) throws SQLException
    {
        return getPool(jdbcUrl, jdbcClass, user, pass).dataSource;
    }

    /**
     * Gets existing or creates a new JdbcConnectionPool.
     *
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @throws SQLException if the underlying jdbc code throws exception
     * @return JdbcConnectionPool  with indicated parameters
     */
    public synchronized static final JdbcConnectionPool
        getPool(String jdbcUrl,
    String jdbcClass,
    String user,
                String pass)
        throws SQLException
    {
        if (pass == null) {
            pass="";
    }

        long starttimestamp = System.currentTimeMillis();
        for (JdbcConnectionPool pool: pools) {
            if(pool.jdbcClass.equals(jdbcClass) &&
            pool.jdbcUrl.equals(jdbcUrl) &&
            pool.pass.equals(pass) &&
            pool.user.equals(user) ) {
                return pool;
            }
        }

        JdbcConnectionPool pool =
            new JdbcConnectionPool(jdbcUrl, jdbcClass, user, pass);

        pools.add(pool);
        long elapsed = System.currentTimeMillis()-starttimestamp;
        if( _logSql.isDebugEnabled() ) {
            _logSql.debug( "getPool() took "+elapsed+" ms");
        }

        return pool;
    }

    /**
     * Creates a new instance of JdbcConnectionPool.
     *
     * @param jdbcUrl URL of the Database
     * @param jdbcClass JDBC Driver class name
     * @param user Database user
     * @param pass Database password
     * @throws SQLException
     */
    protected JdbcConnectionPool(  String jdbcUrl,
    String jdbcClass,
    String user,
                                 String pass)
        throws SQLException
        {
        checkNotNull(jdbcUrl);
        checkNotNull(jdbcClass);
        checkNotNull(user);
        checkNotNull(pass);

        try {
            Class.forName(jdbcClass);
        } catch (ClassNotFoundException e) {
            throw new SQLException("can not initialize jdbc driver : "+jdbcClass);
        }

        this.jdbcUrl = jdbcUrl;
        this.jdbcClass = jdbcClass;
        this.user = user;
        this.pass = pass;

        final BoneCPDataSource ds = new BoneCPDataSource();
        ds.setJdbcUrl(jdbcUrl);
        ds.setUsername(user);
        ds.setPassword(pass);
        ds.setIdleConnectionTestPeriodInMinutes(60);
        ds.setIdleMaxAgeInMinutes(240);
        ds.setMaxConnectionsPerPartition(10);
        ds.setPartitionCount(3);
        ds.setAcquireIncrement(5);
        ds.setStatementsCacheSize(100);

        dataSource = ds;

        Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run()
                {
                    ds.close();
                }
            });
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

    public boolean equals(Object o)
    {
        if( this == o) {
            return true;
        }

        if (!(o instanceof JdbcConnectionPool)) {
            return false;
        }

        JdbcConnectionPool pool = (JdbcConnectionPool)o;
        return pool.jdbcClass.equals(jdbcClass) &&
        pool.jdbcUrl.equals(jdbcUrl) &&
        pool.pass.equals(pass) &&
            pool.user.equals(user);
    }

    public int hashCode()
    {
        return
            jdbcClass.hashCode() ^
            jdbcUrl.hashCode()      ^
            pass.hashCode()         ^
            user.hashCode();
    }
    }
