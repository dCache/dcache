package org.dcache.services.pinmanager;

import java.util.concurrent.TimeUnit;
import java.sql.SQLException;
import java.sql.Connection;

import org.apache.log4j.Logger;

import org.dcache.util.JdbcConnectionPool;

/**
 * Abstraction over a database operation.
 *
 * This class handles the common operations of obtaining a connection
 * from a connection pool, returning it, logging errors and retry upon
 * failure.
 *
 * The operation itself must be performed in a subclass.
 *
 * Failures are handled by sleeping for 30 seconds and retrying the
 * operation. Short of shutting down the cell, there is not much else
 * we can do in case we have problems with the database.
 */
abstract class AbstractDBTask implements Runnable
{
    private final JdbcConnectionPool _pool;

    private static Logger _logger =
        Logger.getLogger("logger.org.dcache.db.sql");

    public AbstractDBTask(JdbcConnectionPool pool)
    {
        _pool = pool;
    }

    public void run()
    {
        try {
            for (;;) {
                try {
                    Connection con = _pool.getConnection();
                    try {
                        run(con);
                        _pool.returnConnection(con);
                        con = null;
                    } finally {
                        if (con != null) {
                            _pool.returnFailedConnection(con);
                        }
                    }
                    _logger.info("done");
                    break;
                } catch (SQLException e) {
                    _logger.error(e);
                }
                TimeUnit.SECONDS.sleep(30);
            }
        } catch (InterruptedException e) {
            _logger.fatal("Background database task was interrupted. The operation was not completed.");
            Thread.currentThread().interrupt();
        }
    }

    protected abstract void run(Connection con) throws SQLException;
}
