package org.dcache.srm.request.sql;

import com.jolbox.bonecp.BoneCPDataSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;

import java.util.ArrayList;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JdbcConnectionPool
{
    private static final int MAX_CONNECTIONS = 50;

    private static final List<JdbcConnectionPool> pools = new ArrayList<>();

    private final String jdbcUrl;
    private final String jdbcClass;
    private final String user;
    private final String pass;
    private final DataSource dataSource;
    private final PlatformTransactionManager tx;

    public static final synchronized JdbcConnectionPool getPool(String jdbcUrl,
                                                                String jdbcClass,
                                                                String user,
                                                                String pass)
            throws ClassNotFoundException
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
                                 String pass)
            throws ClassNotFoundException
    {
        this.jdbcUrl = checkNotNull(jdbcUrl);
        this.jdbcClass = checkNotNull(jdbcClass);
        this.user = checkNotNull(user);
        this.pass = checkNotNull(pass);

        Class.forName(jdbcClass);

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
        tx = new DataSourceTransactionManager(dataSource);

        Runtime.getRuntime().addShutdownHook(new Thread()
        {
            @Override
            public void run()
            {
                ds.close();
            }
        });
    }

    public JdbcTemplate newJdbcTemplate()
    {
        return new JdbcTemplate(dataSource);
    }

    public TransactionTemplate newTransactionTemplate()
    {
        return new TransactionTemplate(tx);
    }
}

