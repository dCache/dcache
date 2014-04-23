// $Id$

package diskCacheV111.replicaManager;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import diskCacheV111.repository.CacheRepositoryEntryInfo;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAdapter;

import org.dcache.chimera.JdbcFs;

import static org.dcache.commons.util.SqlHelper.tryToClose;

//import uk.org.primrose.GeneralException;
//import uk.org.primrose.vendor.standalone.*;
//import uk.org.primrose.pool.core.PoolLoader;


/**
 * This class works with replicas database
 */
public class ReplicaDbV1 implements ReplicaDb1 {
    private final static String _cvsId     = "$Id$";

    private final static Logger _log =
        LoggerFactory.getLogger(ReplicaDbV1.class);

    private CellAdapter         _cell;
    private static DataSource   DATASOURCE;
    private final static String ERRCODE_UNIQUE_VIOLATION = "23505";

    /**
     * Class constructor
     * @throws SQLException
     */
    public ReplicaDbV1(CellAdapter cell)
    {
        _cell = cell;
    }

    /*
     * Report SQL exception ex for the sql statement sql in method m.
     */
    private void reportSQLException(String m, SQLException ex, String sql) {
        int iErr = ex.getErrorCode();
        String sState = ex.getSQLState();

        _log.warn("SQL exception in method " + m + ": '" + ex + "', errCode=" + iErr + " SQLState=" + sState + " SQLStatement=[" + sql
                + "]");
    }

    private void ignoredSQLException(String m, SQLException ex, String sql) {
        int iErr = ex.getErrorCode();
        String sState = ex.getSQLState();
        String exMsg = ex.getMessage().substring(5);

        _log.info("Ignore SQL exception in method " + m + ": '" + exMsg + "', errCode=" + iErr + " SQLState=" + sState
                + " SQLStatement=[" + sql + "]");
    }

    /**
     * Add record (poolname, pnfsid) to the table 'replicas'
     */
    @Override
    public synchronized void addPool(PnfsId pnfsId, String poolName) {
//1     final String sql = "INSERT INTO replicas VALUES ('" + poolName + "','" + pnfsId.toString() + "',now())";
        Connection conn = null;
//1     Statement  stmt = null;
        PreparedStatement pstmt = null;
        final String sql1 = "INSERT INTO replicas (pool,pnfsid,datestamp,poolid,bitmask,countable,excluded) SELECT ?, ?, now(), pools.poolid, ?, ?, ? FROM pools WHERE pools.pool=?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
//1         stmt = (_conn == null) ? conn.createStatement() : _stmt;
//1         stmt.executeUpdate(sql);
            pstmt = conn.prepareStatement(sql1);
            pstmt.setString (1, poolName);
            pstmt.setString (2, pnfsId.toString());
            pstmt.setInt    (3, 1);
            pstmt.setBoolean(4, true);
            pstmt.setBoolean(5, false);
            pstmt.setString (6, poolName);
            pstmt.executeUpdate();
        } catch (SQLException ex) {
            String exState = ex.getSQLState();
            if (exState.equals(ERRCODE_UNIQUE_VIOLATION) ) { // "ERROR: duplicate key value violates unique constraint" - or similar
                String s = ex.getMessage().substring(5);
                _log.info("WARNING" + s + "; caused by duplicate message, ignore for now. pnfsid=" + pnfsId.toString() + " pool="
                        + poolName);
//1             ignoredSQLException("addPool()", (SQLException) ex, sql);
                ignoredSQLException("addPool()", ex, sql1);
            } else {
                _log.warn("Database access error", ex);
            }
        } finally {
            tryToClose(pstmt);
            tryToClose(conn);
        }
    }

    /**
     * Add records (poolname, pnfsid) to the table 'replicas'
     */
    public synchronized void addPnfsToPool(List<CacheRepositoryEntryInfo> fileList, String poolName) {
        Connection conn = null;
        Statement  stmt = null;
        PreparedStatement pstmt = null;
//      final String sql = "INSERT INTO replicas VALUES ('" + poolName + "',?,now())";
        final String sql = MessageFormat.format(
                "INSERT INTO replicas (pool,pnfsid,datestamp,poolid,bitmask,countable,excluded) SELECT ''{0}'', ?, now(), pools.poolid, ?, ?, ? FROM pools WHERE pools.pool=''{0}''",
                poolName);
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            pstmt = conn.prepareStatement(sql);
            for (CacheRepositoryEntryInfo info:  fileList) {        // Now put
                                                                    // all
                                                                    // pnfsids
                                                                    // into
                                                                    // replicas
                                                                    // table
                String pnfsId = info.getPnfsId().toString();
                int bitmask = info.getBitMask();
                boolean countable =
                        info.isPrecious() &&
//                        info.isCached() &&
                        !info.isReceivingFromClient() &&
                        !info.isReceivingFromStore() &&
//                        info.isSendingToStore() &&
                        !info.isBad() &&
                        !info.isRemoved() &&
                        !info.isDestroyed();
//                        info.isSticky();

                try {
                    pstmt.setString(1, pnfsId);
                    pstmt.setInt    (2, bitmask);
                    pstmt.setBoolean(3, countable);
                    pstmt.setBoolean(4, false);
                    pstmt.executeUpdate();
                } catch (SQLException ex) {
                    String exState = ex.getSQLState();
                    if (exState.equals(ERRCODE_UNIQUE_VIOLATION) ) { // "ERROR: duplicate key value violates unique constraint" - or similar
                        String s = ex.getMessage().substring(5);
                        _log.info("WARNING" + s + "; caused by duplicate message, ignore for now. pnfsid=" + pnfsId + " pool=" + poolName);
                        ignoredSQLException("addPool()", ex, sql);

                    } else {
                        _log.warn("Database access error", ex);
                    }
                }
            }
            //
//            stmt.execute("COMMIT");
        } catch (SQLException e) {
            _log.warn("addPnfsToPool: prepareStatement error", e);
        } finally {
            tryToClose(pstmt);
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Remove record (poolname, pnfsid) from the table 'replicas'
     */
    @Override
    public void removePool(PnfsId pnfsId, String poolName) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        String sql = "DELETE FROM replicas WHERE pool = ? and pnfsId = ?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, poolName);
            stmt.setString(2, pnfsId.toString());
            stmt.executeUpdate();
        } catch (SQLException ex) {
            _log.warn("WARNING: Database access error, can not delete pnfsId='" + pnfsId.toString() + "' " + "at pool = '" + poolName
                    + "' from replicas DB table");
            reportSQLException("removePool()", ex, sql);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }

    }

    /**
     * Get the number of pools for given pnfsid depreciated - will not work with
     * newer postgres release
     */
    @Override
    public int countPools(PnfsId pnfsId) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        ResultSet  rset = null;
        String sql = "SELECT pool FROM replicas WHERE pnfsId = ?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt =  conn.prepareStatement(sql);
            stmt.setString(1, pnfsId.toString());
            rset = stmt.executeQuery();
            // getFetchSize() is depreciated and will not work with newer
            // postgres release
            return rset.getFetchSize();
        } catch (SQLException ex) {
            _log.warn("Database access error", ex);
            reportSQLException("countPools()", ex, sql);
            return -1;
        } finally {
            tryToClose(rset);
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Remove all the records with given pnfsid from the table
     */
    @Override
    public void clearPools(PnfsId pnfsId) {
        Connection conn = null;
        PreparedStatement statement = null;
        String sqlDeleteFromReplicas = "DELETE FROM replicas WHERE pnfsId = ?";
        String sqlDeleteFromExcluded = "DELETE FROM excluded WHERE pnfsId = ?";
        String sqlDeleteFromFiles = "DELETE FROM files    WHERE pnfsId = ?";
        // If the file has been removed from PNFS, we also have to clean up "files" and "excluded" tables
        try {
            conn = DATASOURCE.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sqlDeleteFromReplicas);
            statement.setString(1, pnfsId.toString());
            statement.executeUpdate();
            statement = conn.prepareStatement(sqlDeleteFromExcluded);
            statement.setString(1, pnfsId.toString());
            statement.executeUpdate();
            statement = conn.prepareStatement(sqlDeleteFromFiles);
            statement.setString(1, pnfsId.toString());
            statement.executeUpdate();
            conn.commit();
        } catch (Exception ex) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                _log.error(e.toString());
            }
            _log.warn("Database access error", ex);
        } finally {
            tryToClose(statement);
            tryToClose(conn);
        }
    }

    /**
     * Abstract class for DB access
     */
    protected abstract class DbIterator<T> implements Iterator<T> {

        protected Connection conn;
        protected Statement  stmt;
        protected ResultSet  rset;

        public DbIterator() throws SQLException {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
        }

        @Override
        public boolean hasNext() {
            try {
                return rset.next();
            } catch (Exception ex) {
                _log.warn("Can't step to the next element of the result set", ex);
            }
            return false;
        }

        @Override
        public T next() {
            try {
                return (T) rset.getObject(1);
            } catch (Exception ex) {
                _log.warn("Can't get the next element of the result set", ex);
            }
            return null;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("No remove");
        }

        public void close() {
            tryToClose(rset);
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Private class for PNFSIDs access
     */
    private class PnfsIdIterator extends DbIterator<String> {

        private PnfsIdIterator() throws SQLException {
            final String sql = "SELECT pnfsId FROM replicas GROUP BY pnfsid";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(sql);
        }

        private PnfsIdIterator(String poolName) throws SQLException {
            String sql = "SELECT pnfsId FROM replicas WHERE  pool = ?";
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setString(1, poolName);
            stmt = statement;
            rset = statement.executeQuery();
        }

        private PnfsIdIterator(long timestamp) throws SQLException {
            String sql = "SELECT pnfsId FROM actions WHERE \"timestamp\" < ?";
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setLong(1, timestamp);
            stmt = statement;
            rset = statement.executeQuery();
        }
    }

    /**
     * Returns all PNFSIDs from the DB
     */
    @Override
    public Iterator<String> getPnfsIds() throws SQLException
    {
        return new PnfsIdIterator();
    }

    /**
     * Returns all PNFSIDs for the given pool from the DB
     */
    public Iterator<String> getPnfsIds(String poolName) throws SQLException
    {
        return new PnfsIdIterator(poolName);
    }

    /**
     * Private class for PNFSIDs access
     */
    private class PoolsIterator extends DbIterator<String> {

        /**
         * Returns all pools from the DB
         *
         * @throws SQLException
         */
        private PoolsIterator() throws SQLException {
            final String sql = "SELECT * FROM pools";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(sql);
        }

        /**
         * Returns all pools for given pnfsid from the DB
         *
         * @throws SQLException
         */
        private PoolsIterator(PnfsId pnfsId) throws SQLException {
            String sql = "SELECT pool FROM replicas WHERE pnfsId = ?";
            PreparedStatement statement = conn.prepareStatement(sql);
            statement.setString(1, pnfsId.toString());
            stmt = statement;
            rset = statement.executeQuery();
        }
    }

    /**
     * Returns all pools from DB
     */
    @Override
    public Iterator<String> getPools() {
        try {
            return new PoolsIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<String>().iterator(); // Empty set
    }

    /**
     * Returns all pools for a given pnfsid
     */
    @Override
    public Iterator<String> getPools(PnfsId pnfsId) {
        try {
            return new PoolsIterator(pnfsId);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<String>().iterator(); // Empty set
    }

    private class PoolsWritableIterator extends DbIterator<String> {
        /**
         * Returns Writable pools from the DB
         *
         * @throws SQLException
         */
        private PoolsWritableIterator() throws SQLException {
            final String query = "select * from pools WHERE status='"+ ONLINE+"'";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(query);
        }
    }

    /**
     * Returns all writable pools from DB
     */
    @Override
    public Iterator<String> getPoolsWritable() {
        try {
            return new PoolsWritableIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet().iterator(); // Empty set
    }

    private class PoolsReadableIterator extends DbIterator<String> {
        /**
         * Returns Readable pools from the DB
         *
         * @throws SQLException
         */
        private PoolsReadableIterator() throws SQLException {
            String query = "select * from pools WHERE " + "(  status='" + ONLINE + "' " + "OR status='" + DRAINOFF + "' "
                    + "OR status='" + OFFLINE_PREPARE + "')";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(query);
        }
    }

    /**
     * Returns all Readable pools from DB
     */
    @Override
    public Iterator<String> getPoolsReadable() {
        try {
            return new PoolsReadableIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet().iterator(); // Empty set
    }

    /**
     * Clears the tables
     */
    @Override
    public void clearAll() {
        Connection conn = null;
        Statement  stmt = null;
        try {
            conn = DATASOURCE.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE replicas");
            stmt.executeUpdate("TRUNCATE TABLE pools");
            stmt.executeUpdate("TRUNCATE TABLE deficient");
            stmt.executeUpdate("TRUNCATE TABLE redundant");
            stmt.executeUpdate("TRUNCATE TABLE excluded");
            conn.commit();
        } catch (Exception ex) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
                _log.error(e1.toString());
            }
            _log.warn("Can't clear the tables");
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Clears pools and replicas tables for the pool 'pool' in argument
     *
     * @throws SQLException
     *                 if can not remove pool from DB
     */
    @Override
    public void clearPool(String poolName) {
        Connection conn = null;
        PreparedStatement statement = null;
        String sqlDeleteReplicas = "DELETE FROM replicas WHERE pool=?";
        String sqlDeletePools = "DELETE FROM pools    WHERE pool=?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            statement = conn.prepareStatement(sqlDeleteReplicas);
            statement.setString(1, poolName);
            statement.executeUpdate();
            statement = conn.prepareStatement(sqlDeletePools);
            statement.setString(1, poolName);
            statement.executeUpdate();
            conn.commit();
        } catch (SQLException ex) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
                _log.error(ex.toString());
            }
            _log.warn("Can't remove pool '" + poolName + "' from the DB");
        } finally {
            tryToClose(statement);
            tryToClose(conn);
        }
    }

    /**
     * Private class to get the PNFSIDs with number of replicas > maximum
     */
    private class getRedundantIterator extends DbIterator<Object[]> {

        private getRedundantIterator(int maxcnt) throws SQLException {
/*
            String sql = "SELECT * FROM (SELECT pnfsid, sum(CASE WHEN pools.status='"
                    + ONLINE + "' THEN 1 ELSE 0 END) "
                    + "FROM      replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp "
                    + "WHERE sum > " + maxcnt
                    + " AND pnfsid NOT IN (SELECT pnfsid FROM actions) ORDER BY sum DESC";
*/
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE redundant");
            final String sql;
            sql = "INSERT  INTO redundant"
                    +"  SELECT pnfsid, count(*)"
                    +"  FROM replicas, pools"
                    +"  WHERE"
                    +"        replicas.poolid=pools.poolid"
                    +"        AND pools.status='" + ONLINE + "'"
                    +"       AND replicas.countable"
                    +"  GROUP BY pnfsid"
                    +"  HAVING count(*) > " + maxcnt;
            stmt.executeUpdate(sql);
            stmt.executeUpdate("DELETE FROM redundant WHERE pnfsid IN (SELECT pnfsid FROM actions)");
            stmt.executeUpdate("DELETE FROM redundant WHERE pnfsid IN (SELECT pnfsid FROM excluded)");
            conn.commit();
            conn.setAutoCommit(true);
            //
            rset = stmt.executeQuery("SELECT * FROM redundant ORDER BY \"count\" DESC");
        }

        @Override
        public Object[] next() {
            try {
                return new Object[] { rset.getObject(1), rset.getObject(2) };
            } catch (Exception ex) {
                _log.warn("Can't get the next element of the result set", ex);
            }
            return null;
        }
    }

    /**
     * Returns all pnfsids with counters > 4
     */
    @Override
    public Iterator<Object[]> getRedundant(int maxcnt) {
        try {
            return new getRedundantIterator(maxcnt);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<Object[]>().iterator(); // Empty set
    }

    /**
     * Private class to get the PNFSIDs with number of replicas < minimum
     */
    private class getDeficientIterator extends DbIterator<Object[]> {

        private getDeficientIterator(int mincnt) throws SQLException {
/*
            final String sql;
            sql = "SELECT * FROM (SELECT pnfsid,"
                    + "sum(CASE WHEN pools.status='" + ONLINE
                    + "' OR pools.status='" + OFFLINE
                    + "' OR pools.status='" + OFFLINE_PREPARE
                    + "' THEN 1 ELSE 0 END) "
                    + "FROM      replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp "
                    + "WHERE sum > 0 and sum < " + mincnt
                    + " AND pnfsid NOT IN (SELECT pnfsid FROM actions) ORDER BY sum ASC";
*/
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE deficient");
            final String sql;
            sql = "INSERT INTO deficient (pnfsid, \"count\")"+
                    "  SELECT pnfsid, count(*)"+
                    "  FROM replicas, pools"+
                    "  WHERE"+
            //      "--      replicas.pool=pools.pool"+
                    "        replicas.poolid=pools.poolid"+
                    "        AND pools.status IN ('"+ONLINE+"','"+OFFLINE+"','"+OFFLINE_PREPARE+"')"+
            //      "--      AND pools.countable"+
                    "       AND replicas.countable"+
                    "  GROUP BY pnfsid"+
                    "  HAVING count(*) < " + mincnt;
            stmt.executeUpdate(sql);
            stmt.executeUpdate("DELETE FROM deficient WHERE pnfsid IN (SELECT pnfsid FROM actions)");
            stmt.executeUpdate("DELETE FROM deficient WHERE pnfsid IN (SELECT pnfsid FROM excluded)");
            conn.commit();
            conn.setAutoCommit(true);
            //
            rset = stmt.executeQuery("SELECT * FROM deficient ORDER BY \"count\" ASC");
        }

        @Override
        public Object[] next() {
            try {
                return new Object[] { rset.getObject(1), rset.getObject(2) };
            } catch (Exception ex) {
                _log.warn("Can't get the next element of the result set", ex);
            }
            return null;
        }
    }

    /**
     * Returns all pnfsids with counters = 1
     */
    @Override
    public Iterator<Object[]> getDeficient(int mincnt) {
        try {
            return new getDeficientIterator(mincnt);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<Object[]>().iterator(); // Empty set
    }

    /**
     * Private class to get the PNFSIDs with number of replicas = 0
     */
    private class getMissingIterator extends DbIterator<String> {

        private getMissingIterator() throws SQLException {
            String sql = "SELECT pnfsid FROM (SELECT pnfsid, " + "sum(CASE " + "WHEN pools.status='" + ONLINE
                    + "' OR pools.status='" + OFFLINE + "' OR pools.status='" + OFFLINE_PREPARE + "' THEN 1 "
                    + "WHEN pools.status='reduce' THEN -1 ELSE 0 "
                    + "END) FROM replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp WHERE sum=0";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(sql);
        }
    }

    /**
     * Returns all pnfsids with counters = 0
     */
    @Override
    public Iterator<String> getMissing() {
        try {
            return new getMissingIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<String>().iterator(); // Empty set
    }

    @Override
    public void removePoolStatus(String poolName) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        String sql = "DELETE FROM pools WHERE pool=?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt =  conn.prepareStatement(sql);
            stmt.setString(1, poolName);
            stmt.executeUpdate();
        } catch (Exception ex) {
            _log.warn("Can't remove pool '" + poolName + "' from the DB", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Set the value of pool status.
     *
     * @param poolName
     *                Pool name.
     * @param poolStatus
     *                Value to assign to pool status.
     */
    @Override
    public void setPoolStatus(String poolName, String poolStatus) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        String sql_i = "insert into pools (pool,status,datestamp) values (?,?,now())";
        String sql_u = "update pools set status=?, datestamp=now() where pool=?";

        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt =  conn.prepareStatement(sql_i);
            stmt.setString(1, poolName);
            stmt.setString(2, poolStatus);
            stmt.executeUpdate();
        } catch (SQLException ex) {
            _log.debug(ex.toString(), ex);
            try {
                stmt = conn.prepareStatement(sql_u);
                stmt.setString(1, poolStatus);
                stmt.setString(2, poolName);
                stmt.executeUpdate();
            } catch (SQLException ex2) {
                _log.warn("setPoolStatus() ERROR: Can't add/update pool '" + poolName + "'" + " status in 'pools' table in DB", ex2);
            }
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Get the value of pool status.
     *
     * @return value of pool status.
     */
    @Override
    public String getPoolStatus(String poolName) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        ResultSet  rset = null;
        String sql = "SELECT status FROM pools WHERE pool=?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt =  conn.prepareStatement(sql);
            stmt.setString(1, poolName);
            rset = stmt.executeQuery();
            rset.next();
            return rset.getString(1);
        } catch (SQLException ex) {
            reportSQLException("getPoolStatus()", ex, sql);

            _log.warn("DB: Can't get status for pool '" + poolName + "' from pools table, return 'UNKNOWN'");
            return "UNKNOWN";
        } finally {
            tryToClose(rset);
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Add transaction into DB
     */
    @Override
    public void addTransaction(PnfsId pnfsId, long timestamp, int count) {
        Connection conn = null;
        Statement  stmt = null;
        String op;
        if (count > 0) {
            op = "replicate";
        } else if (count < 0) {
            op = "reduce";
        } else {
            op = "exclude";
        }

        final String sql = MessageFormat.format("INSERT INTO actions VALUES (''{0}'',''{1}'',''{2}'',''{3}'',now(),{4,number,#})",
                op, "s", pnfsId.toString(), "d", timestamp);

        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception ex) {
            _log.warn("Can't add transaction to the DB", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Add transaction into DB
     * @param pnfsId
     * @param timestamp
     * @param errcode
     * @param errmsg
     */
    public void addExcluded(PnfsId pnfsId, long timestamp, String errcode, String errmsg) {
        Connection conn = null;
        Statement  stmt = null;

        final String sql = MessageFormat.format("INSERT INTO excluded VALUES (''{0}'',''{1}'',now(),{2,number,#},10000,{3,number,#},''{4}'',''{5}'')",
                "s", pnfsId.toString(), timestamp, 0xFFFF, errcode, errmsg);

        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception ex) {
            _log.warn("Can't add transaction to the DB", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Remove transaction from DB
     */
    @Override
    public void removeTransaction(PnfsId pnfsId) {
        Connection conn = null;
        Statement  stmt = null;
        final String sql = MessageFormat.format("DELETE FROM actions WHERE pnfsId = ''{0}''", pnfsId.toString());
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception ex) {
            _log.warn("Can't remove transaction from the DB", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Release excluded files from "excluded" table with timestamp older than "timesatamp"
     */
    public int releaseExcluded(long timestamp) {
        Connection conn = null;
        Statement  stmt = null;
        final String sql = "DELETE FROM excluded WHERE \"timestamp\" < " + timestamp;
        int count = 0;
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            count = stmt.executeUpdate(sql);
        } catch (Exception ex) {
            _log.warn("Can't delete old records from the 'excluded' table", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }

        return count;
    }


    /**
     * Clear transactions from DB
     */
    @Override
    public void clearTransactions() {
        Connection conn = null;
        Statement  stmt = null;
        final String sql = "DELETE FROM actions WHERE \"action\" IN ('replicate', 'reduce')";
//      final String sql = "TRUNCATE actions";

        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        } catch (Exception ex) {
            _log.warn("Can't clear transactions from the DB", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Return the timestamp for a given PNFSID
     */
    @Override
    public long getTimestamp(PnfsId pnfsId) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        ResultSet  rset = null;
        String sql = "SELECT \"timestamp\" FROM actions WHERE pnfsId = ?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt = conn.prepareStatement(sql);
            stmt.setString(1, pnfsId.toString());
            rset = stmt.executeQuery();
            rset.next();
            return rset.getLong(1);
        } catch (Exception ex) {
            _log.warn("Can't get data from the DB", ex);
            return -1;
        } finally {
            tryToClose(rset);
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    /**
     * Return the list of PNFSIDs which are older than 'timestamp'
     */
    public Iterator<String> getPnfsIds(long timestamp) {
        try {
            return new PnfsIdIterator(timestamp);
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<String>().iterator(); // Empty set
    }

    @Override
    public void removePool(String poolName) {
        Connection conn = null;
        PreparedStatement statement = null;
        String sql = "DELETE FROM replicas WHERE pool=?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            statement =  conn.prepareStatement(sql);
            statement.setString(1, poolName);
            statement.executeUpdate();
            conn.commit();
        } catch (Exception ex) {
            try { conn.rollback(); } catch (SQLException e1) { }
            _log.warn("Can't remove pool '" + poolName + "' from the DB");
        } finally {
            tryToClose(statement);
            tryToClose(conn);
        }
    }

    /**
     * Private class to get the PNFSIDs which are in the drainoff pools only
     */
    private class getDrainingIterator extends DbIterator<String> {

        private getDrainingIterator() throws SQLException {
/*
            String sql = "SELECT rd.pnfsid " +
            "FROM ONLY replicas rd, pools pd " +
            "WHERE rd.pool = pd.pool AND pd.status = '" + DRAINOFF + "' " +
            "GROUP BY rd.pnfsid " +
            "EXCEPT " +
            "SELECT r.pnfsid " +
            "FROM (" +
            "       SELECT rr.pnfsid FROM ONLY replicas rr, pools pp " +
            "       WHERE rr.pool = pp.pool  AND pp.status = '" + DRAINOFF + "' " +
            "       GROUP BY rr.pnfsid" +
            "     ) r, " +
            "     ONLY replicas r1, " + "     pools p1 " + "WHERE r.pnfsid  = r1.pnfsid" +
            " AND  p1.pool   = r1.pool" +
            " AND  ( p1.status = '" + ONLINE + "' " +
            "     OR r.pnfsid IN (SELECT pnfsid FROM actions) ) " +
            "GROUP BY r.pnfsid";
*/
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            conn.setAutoCommit(false);
            stmt = conn.createStatement();
            stmt.executeUpdate("TRUNCATE TABLE drainoff");
            String sql;
            sql = "INSERT INTO drainoff"
                    +"        SELECT rd.pnfsid"
                    +"        FROM replicas rd, pools pd"
                    +"            WHERE rd.poolid = pd.poolid AND pd.status = '"+DRAINOFF+"'"
                    +"        GROUP BY rd.pnfsid";
            stmt.executeUpdate(sql);
            sql = "DELETE FROM drainoff WHERE pnfsid IN"
                    +"        (SELECT pnfsid"
                    +"         FROM replicas rd, pools pd"
                    +"            WHERE rd.poolid = pd.poolid AND pd.status = '"+ONLINE+"'"
                    +"         GROUP BY pnfsid"
                    +"         UNION ALL"
                    +"         SELECT pnfsid FROM actions"
                    +"         UNION ALL"
                    +"         SELECT pnfsid FROM excluded"
                    +"        )";
            stmt.executeUpdate(sql);
            conn.commit();
            conn.setAutoCommit(true);
            //
            rset = stmt.executeQuery("SELECT * FROM drainoff ORDER BY pnfsid");
        }
    }

    /**
     * Get the list of PNFSIDs which are in the drainoff pools only
     */
    @Override
    public Iterator<String> getInDrainoffOnly() {
        try {
            return new getDrainingIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet<String>().iterator(); // Empty set
    }

    /**
     * Private class to get the PNFSIDs which are in the offline pools only
     */
    private class getOfflineIterator extends DbIterator<String> {

        private getOfflineIterator() throws SQLException {
            String sql = "SELECT ro.pnfsid " + "FROM replicas ro, pools po "
                    + "WHERE ro.pool = po.pool AND po.status = '"  + OFFLINE_PREPARE + "' "
                    + "GROUP BY ro.pnfsid "
                    + "EXCEPT "
                    + "SELECT r.pnfsid " + "FROM ("
                    + "       SELECT rr.pnfsid FROM replicas rr, pools pp "
                    + "       WHERE rr.pool = pp.pool  AND pp.status = '" + OFFLINE_PREPARE + "' "
                    + "       GROUP BY rr.pnfsid"
                    + "     ) r, " + "     replicas r1, " + "     pools p1 "
                    + "WHERE r.pnfsid = r1.pnfsid"
                    + " AND  p1.pool  = r1.pool" + " AND  ( p1.status = '" + ONLINE + "' "
                    + "     OR r.pnfsid IN (SELECT pnfsid FROM actions) ) " + "GROUP BY r.pnfsid";
            stmt = conn.createStatement();
            rset = stmt.executeQuery(sql);
        }
    }

    /**
     * Get the list of PNFSIDs which are in the offline pools only
     */
    @Override
    public Iterator<String> getInOfflineOnly() {
        try {
            return new getOfflineIterator();
        } catch (SQLException e) {
            // TODO Auto-generated catch block
            _log.error(e.toString(), e);
        }
        return new HashSet().iterator(); // Empty set
    }

    /**
     *
     */
    @Override
    public void setHeartBeat(String name, String desc) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        final String sql_i = "insert into heartbeat values (?,?,now())";
        final String sql_u = "update heartbeat set description=?, datestamp=now() where process=?";

        try {
            conn = DATASOURCE.getConnection();
            try {
                conn.setAutoCommit(true);
                stmt =  conn.prepareStatement(sql_i);
                stmt.setString(1, name);
                stmt.setString(2, desc);
                stmt.executeUpdate();
            } catch (Exception ex) {
                _log.debug(ex.toString());
                try {
                    stmt = conn.prepareStatement(sql_u);
                    stmt.setString(1, desc);
                    stmt.setString(2, name);
                    stmt.executeUpdate();
                } catch (Exception ex2) {
                    _log.warn("setHeartBeat() ERROR: Can't add/update process '" + name + "' status in 'heartbeat' table in DB", ex2);
                }
            } finally {
                tryToClose(stmt);
                tryToClose(conn);
            }
        } catch (SQLException e) {
            _log.error(e.toString());
        }
    }

    /**
     *
     */
    @Override
    public void removeHeartBeat(String name) {
        Connection conn = null;
        PreparedStatement  stmt = null;
        final String sql = "DELETE FROM heartbeat WHERE process = ?";
        try {
            conn = DATASOURCE.getConnection();
            conn.setAutoCommit(true);
            stmt =  conn.prepareStatement(sql);
            stmt.setString(1, name);
            stmt.executeUpdate();
        } catch (Exception ex) {
            _log.warn("Database access error", ex);
        } finally {
            tryToClose(stmt);
            tryToClose(conn);
        }
    }

    //////////////////////////////// End Of Interface ///////////////////////////////////////

    public static void printClassName(Object obj) {
        System.out.println("The class of " + obj + " is " + obj.getClass().getName());
    }

    /**
     * Setup method to create connection to the database and the datasource
     *
     * @param connectURI
     * @param user
     * @param password
     */
    public final static void setup(String connectURI, String user, String password) {
        HikariConfig config = new HikariConfig();
        config.setDataSource(new DriverManagerDataSource(connectURI, user, password));
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(30);
        final HikariDataSource ds = new HikariDataSource(config);

        Runtime.getRuntime().addShutdownHook(new Thread("replica-hikaricp-shutdown-hook") {
            @Override
            public void run()
            {
                ds.shutdown();
            }
        });

        DATASOURCE = ds;
    }


    public static void main(String[] args) throws SQLException
    {
        System.out.println("Test ReplicaDbV1, cvsId=" + _cvsId);

        setup("jdbc:postgresql://localhost:5432/replicas", "enstore", "NoPassword");
        ReplicaDbV1 db = new ReplicaDbV1(null);

        System.out.println("List pnfsId's in all pools");
        for (Iterator<String> i = db.getPnfsIds(); i.hasNext();) {
            System.out.println(i.next());
        }

        for (Iterator<String> p = db.getPools(); p.hasNext();) {
            String pool = p.next();
            System.out.println("Pool : " + pool);
            for (Iterator<String> j = db.getPnfsIds(pool); j.hasNext();) {
                System.out.println(j.next());
            }
        }
//        for ( Iterator j = db.pnfsIds("pool1") ; j.hasNext() ; ) {
//            System.out.println( j.next().toString());
//        }
//        for ( Iterator j = db.pnfsIds("pool2") ; j.hasNext() ; ) {
//            System.out.println( j.next().toString());
//        }
//        for ( Iterator j = db.pnfsIds("pool3") ; j.hasNext() ; ) {
//            System.out.println( j.next().toString());
//        }
//        for ( Iterator j = db.pnfsIds("pool4") ; j.hasNext() ; ) {
//            System.out.println( j.next().toString());
//        }

        PnfsId pnfsId = new PnfsId("1234");

        System.out.println("WARNING: db.countPools(...) is depreciated and will not work with newer postgres release ");

        db.addPool(pnfsId, "pool1");
        db.addPool(pnfsId, "pool2");
        db.addPool(pnfsId, "pool3");
        System.out.println("pools: " + db.countPools(pnfsId));

        db.removePool(pnfsId, "pool1");
        System.out.println("pools: " + db.countPools(pnfsId));

        db.addPool(pnfsId, "pool1");
        System.out.println("pools: " + db.countPools(pnfsId));

        db.clearPools(pnfsId);
        System.out.println("pools: " + db.countPools(pnfsId));

        // This has to generate an error: Cannot insert a duplicate key into
        // unique index replicas_index
//        for (int i = 0; i < 5; i++) {
//            db.addPool( new PnfsId("2000"+i) , "pool1" ) ;
//        }

//        for (int i = 0; i < 5; i++) {
//            db.addPool( new PnfsId("2000"+i) , "pool3" ) ;
//        }

        db.addPool(pnfsId, "pool2");
        // db.addPool( pnfsId , "pool2" ) ; // This has to generate an error:
        // Cannot insert a duplicate key into unique index replicas_index
        System.out.println("pools: " + db.countPools(pnfsId));
        for (Iterator<String> i = db.getPools(pnfsId); i.hasNext();) {
            System.out.println(" pnfsid : " + pnfsId + ", pool : " + i.next());
        }

        db.removePool(pnfsId, "pool2");
        System.out.println("pools: " + db.countPools(pnfsId));
        for (Iterator<String> i = db.getPools(pnfsId); i.hasNext();) {
            System.out.println(" pnfsid : " + pnfsId + ", pool : " + i.next());
        }

        for (Iterator<String> i = db.getMissing(); i.hasNext();) {
            System.out.println(" Missing pnfsid : " + i.next());
        }

        for (Iterator<Object[]> i = db.getDeficient(2); i.hasNext();) {
            Object[] r = (i.next());
            // System.out.println(" Length : "+r.length);
            System.out.println(" Deficient pnfsid : " + r[0] + ": " + r[1]);
            // printClassName(i.next());
            // printClassName(new int[] {1,2,3});
        }

        for (Iterator<Object[]> i = db.getRedundant(3); i.hasNext();) {
            Object[] r = (i.next());
            System.out.println(" Redundant pnfsid : " + r[0] + ": " + r[1]);
        }

        System.out.println("pool1: Status : '" + db.getPoolStatus("pool1") + "'");
        System.out.println("pool11111111111: Status : '" + db.getPoolStatus("pool11111111111") + "'");

        db.setPoolStatus("pool9", "offline");
        System.out.println("pool9: Status : '" + db.getPoolStatus("pool9") + "'");

        db.clearPool("pool9");

        System.exit(0);
    }
}
