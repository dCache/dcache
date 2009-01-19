/**
 * $Id$
 */
package diskCacheV111.namespace.provider;

import diskCacheV111.util.PnfsIdUtil;
import org.apache.log4j.Logger;
import org.dcache.util.JdbcConnectionPool;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;

public class DbTrash implements Trash
{
    private final DataSource _dataSource;
    private static final Logger _logger =  Logger.getLogger("logger.org.dcache.namespace." + DbTrash.class.getName());


    public DbTrash(String jdbcUrl, String jdbcClass, String user, String password)
            throws SQLException
    {
        Connection conn = null;
        if (jdbcUrl == null || jdbcClass == null || user == null || password == null) {
            throw new IllegalArgumentException("Not enough arguments to initalize trash database");
        } else {
            try {
                if (_logger.isDebugEnabled()) {
                    _logger.debug(MessageFormat.format("Init database args: {0}, {1}, {2}, ********", jdbcUrl, jdbcClass, user));
                }
                _dataSource = JdbcConnectionPool.getDataSource(jdbcUrl, jdbcClass, user, password);
                conn = _dataSource.getConnection();
            } finally {
                try { if (null!=conn) conn.close();  } catch (SQLException e) { }
            }
        }
    }


    public boolean isFound(String pnfsid) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rset = null;
        String sql = "SELECT * FROM deleted WHERE pnfsid='" + pnfsid + "'";
        boolean rval = false;
        //
        try {
            if (_logger.isDebugEnabled()) {
                _logger.debug("About to execute '" + sql + "'");
            }
            conn = _dataSource.getConnection();
            pstmt = conn.prepareStatement("SELECT * FROM deleted WHERE pnfsid=?");
            pstmt.setBytes(1, PnfsIdUtil.toBinPnfsId(pnfsid));
            rset = pstmt.executeQuery();
            rval = rset.next();
        } catch (SQLException exception) {
            _logger.error("Trash database access error" + exception);
        } finally {
            try { if (null!=rset) rset.close();  } catch (SQLException e) { }
            try { if (null!=pstmt) pstmt.close();  } catch (SQLException e) { }
            try { if (null!=conn) conn.close();  } catch (SQLException e) { }
        }
        if (_logger.isDebugEnabled()) {
            _logger.debug("SQL '"+sql+"' returned " + rval);
        }
        return rval;
    }

}
