package org.dcache.services.pinmanager;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Arrays;

import org.apache.log4j.Logger;

import diskCacheV111.services.JdbcConnectionPool;

class PreparedUpdateTask extends AbstractDBTask
{
    private String _statement;
    private Object[] _args;

    private static Logger _logger =
        Logger.getLogger("logger.org.dcache.db.sql");

    public PreparedUpdateTask(JdbcConnectionPool pool,
                              String statement, Object ... args)
    {
        super(pool);
        _statement = statement;
        _args = args;
    }

    public void run(Connection con) throws SQLException
    {
        if (_logger.isDebugEnabled()) {
            _logger.debug("Executing '" + _statement +
                          "' with arguments " + Arrays.toString(_args));
        }

        PreparedStatement s = con.prepareStatement(_statement);
        for (int i = 0; i < _args.length; i++)
            s.setObject(i + 1, _args[i]);
        int count = s.executeUpdate();
        con.commit();

        _logger.debug("Updated " + count + " records");
    }
}
