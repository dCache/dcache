/**
 * @author David Melkumyan, DESY Zeuthen
 * @created September 2007
 */
package org.dcache.acl.util.sql;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author David Melkumyan, DESY Zeuthen
 *
 */
public class SQLHandler {
    private static final Logger logger = LoggerFactory.getLogger("logger.org.dcache.authorization." + SQLHandler.class.getName());

    public final static String PROPERTY_USER = "user";

    public final static String PROPERTY_PASSWORD = "password";

    public final static String PROPERTY_RECONNECT = "autoReconnect";

    public final static String PROPERTY_SSL = "ssl";

    private static SQLHandler _SINGLETON;
    static {
        _SINGLETON = new SQLHandler();
    }

    public static SQLHandler instance() {
        return _SINGLETON;
    }

    public static void refresh() {
        _SINGLETON = new SQLHandler();
    }

    private SQLHandler() {
        super();
    }

//	public static Connection getConnection(String driver, String url, String user, String pswd) throws SQLException, Exception {
//		Properties props = new Properties();
//		props.setProperty(PROPERTY_USER, user);
//		props.setProperty(PROPERTY_PASSWORD, pswd);
//		props.setProperty(PROPERTY_RECONNECT, "true");
//
//		logger.debug("Creating new instance of the driver: " + driver);
//
//		final Driver d = (Driver) Class.forName(driver).newInstance();
//		if ( d.acceptsURL(url) == false )
//			throw new RuntimeException("Driver not accept the URL: " + url);
//
//		Connection conn = d.connect(url, props);
//
//		return (conn != null && noWarnings(conn) ? conn : null);
//	}
//
//	public static DataSource getDataSource(String driver, String url, String user, String pswd) throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
//		Properties props = new Properties();
//		props.setProperty(PROPERTY_USER, user);
//		props.setProperty(PROPERTY_PASSWORD, pswd);
//		props.setProperty(PROPERTY_RECONNECT, "true");
//
//		logger.debug("Creating new instance of the driver: " + driver);
//
//		final Driver d = (Driver) Class.forName(driver).newInstance();
//		if ( d.acceptsURL(url) == false )
//			throw new RuntimeException("Driver not accept the URL: " + url);
//
//		DataSource unpooled = DataSources.unpooledDataSource(url, user, pswd);
//		return DataSources.pooledDataSource(unpooled);
//	}

    public static int executeUpdate(final Connection conn, String query) throws SQLException {
        int cnt = -1;

        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            conn.setAutoCommit(false);
            cnt = stmt.executeUpdate(query);
            conn.setAutoCommit(true);

        } catch (SQLException e) {
            conn.rollback();
            throw new SQLException(getMessage(e));

        } finally {
            attemptClose(rs);
            attemptClose(stmt);
        }
        return cnt;
    }

    public static String[] getStringIDs(final Connection conn, String query) throws SQLException {
        String[] rsIds;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(query);
            int cnt = 0;
            if ( rs.last() )
                cnt = rs.getRow();
            else
                // rows == 0
                return null;

            rsIds = new String[cnt];
            int i = cnt;
            do {
                rsIds[--i] = rs.getString(1);
            } while (rs.previous());

            return rsIds;

        } finally {
            attemptClose(rs);
            attemptClose(stmt);
        }
    }

    public static Integer getIntegerID(final Connection conn, String query) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
            rs = stmt.executeQuery(query);
            int cnt = 0;
            if ( rs.last() )
                cnt = rs.getRow();
            else
                // rows == 0
                return null;

            int id = rs.getInt(1);
            if ( cnt == 1 )
                return Integer.valueOf(id);
            else
                // rows > 1
                throw new SQLException("Not a unique ID: " + id);

        } finally {
            attemptClose(rs);
            attemptClose(stmt);
        }
    }

    public static Integer getIntegerID(final Connection conn, String query, String value) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setString(1, value);
            rs = pstmt.executeQuery(query);
            int cnt = 0;
            if ( rs.last() )
                cnt = rs.getRow();
            else
                // rows == 0
                return null;

            int id = rs.getInt(1);
            if ( cnt == 1 )
                return Integer.valueOf(id);
            else
                // rows > 1
                throw new SQLException("Not a unique ID: " + id);

        } finally {
            attemptClose(rs);
            attemptClose(pstmt);
        }

    }

    /**
     * check whether record is exist in ResultSet
     *
     * @param conn
     *            Database connestion
     * @param query
     *            SQL query
     * @return Returns true if a ResultSet is not an empty
     */
    public static boolean isRecordExist(final Connection conn, String query) throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);
            return (rs != null && rs.last());

        } finally {
            attemptClose(rs);
            attemptClose(stmt);
        }
    }

    public static boolean isRecordExist(final Connection conn, String query, String value) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setString(1, value);
            rs = pstmt.executeQuery(query);
            return (rs != null && rs.last());

        } finally {
            attemptClose(rs);
            attemptClose(pstmt);
        }
    }

    public static boolean isRecordExist(final Connection conn, String query, int value) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, value);
            rs = pstmt.executeQuery(query);
            return (rs != null && rs.last());

        } finally {
            attemptClose(rs);
            attemptClose(pstmt);
        }
    }

    public static boolean isRecordExist(final Connection conn, String query, int value1, int value2) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = conn.prepareStatement(query);
            pstmt.setInt(1, value1);
            pstmt.setInt(1, value2);
            rs = pstmt.executeQuery(query);
            return (rs != null && rs.last());

        } finally {
            attemptClose(rs);
            attemptClose(pstmt);
        }
    }

    public static void attemptClose(ResultSet o) {
        try {
            if ( o != null )
                o.close();
        } catch (Exception ignore) {
        }
    }

    public static void attemptClose(Statement o) {
        try {
            if ( o != null )
                o.close();
        } catch (Exception ignore) {
        }
    }

    public static void attemptClose(PreparedStatement o) {
        try {
            if ( o != null )
                o.close();
        } catch (Exception ignore) {
        }
    }

    public static void attemptClose(Connection o) {
        try {
            if ( o != null )
                o.close();
        } catch (Exception ignore) {
        }
    }

    public static void attemptRollback(Connection o) {
        try {
            if ( o != null )
                o.rollback();
        } catch (Exception ignore) {
        }
    }

    public static String getMessage(java.sql.BatchUpdateException bue) {
        if ( bue == null )
            throw new NullPointerException("BatchUpdateException is null.");

        StringBuilder sb = new StringBuilder("BatchUpdateException caught: ");
        sb.append("Message = ").append(bue.getMessage());
        sb.append(", SQLState = ").append(bue.getSQLState());
        sb.append(", Vendor = ").append(bue.getErrorCode());

        java.sql.SQLException sqle;
        while ((sqle = bue.getNextException()) != null) {
            sb.append("Message = ").append(sqle.getMessage());
            sb.append(", SQLState = ").append(sqle.getSQLState());
            sb.append(", Vendor = ").append(sqle.getErrorCode());
        }
        return sb.toString();
    }

    public static String getMessage(SQLException e) {
        if ( e == null )
            throw new NullPointerException("SQLException is null.");

        StringBuilder sb = new StringBuilder("SQLException caught: ");
        while (e != null) {
            sb.append("Message = ").append(e.getMessage());
            sb.append(", SQLState = ").append(e.getSQLState());
            sb.append(", Vendor = ").append(e.getErrorCode());
            e = e.getNextException();
        }
        return sb.toString();
    }

    public static String getMessage(SQLWarning e) {
        if ( e == null )
            throw new NullPointerException("SQLWarning is null.");

        StringBuilder sb = new StringBuilder("SQLWarning caught: ");
        while (e != null) {
            sb.append("Message = ").append(e.getMessage());
            sb.append(", SQLState = ").append(e.getSQLState());
            sb.append(", Vendor = ").append(e.getErrorCode());
            e = e.getNextWarning();
        }
        return sb.toString();
    }

    public static SQLWarning getWarnings(Object sqlObj) throws NullPointerException {
        SQLWarning warn = null;
        try {
            if ( sqlObj == null )
                throw new NullPointerException("Argument sqlObj is NULL.");

            if ( sqlObj instanceof Connection )
                warn = ((Connection) sqlObj).getWarnings();

            else if ( sqlObj instanceof Statement )
                warn = ((Statement) sqlObj).getWarnings();

            else if ( sqlObj instanceof PreparedStatement )
                warn = ((PreparedStatement) sqlObj).getWarnings();

        } catch (SQLException e) {
            e.printStackTrace();
        }
        return warn;
    }

    public static boolean noWarnings(Object sqlObj) throws NullPointerException {
        SQLWarning warn = getWarnings(sqlObj);
        if ( warn != null ) {
            logger.warn(getMessage(warn));
            return false;

        } else
            return true;
    }

    public static String getConnStatus(Connection conn) throws SQLException, Exception {
        String status = null;

        if ( conn == null )
            throw new NullPointerException("Connection is NULL.");

        DatabaseMetaData dma = conn.getMetaData();
        if ( dma != null ) {
            StringBuilder sb = new StringBuilder("SQL Connection Status: ");
            sb.append(dma.getURL()).append(", Driver: ").append(dma.getDriverName());
            sb.append(", Version: ").append(dma.getDriverVersion());
            status = sb.toString();
        }

        return status;
    }

    public static PreparedStatement prepareStatement(Connection conn, String sql) throws SQLException {
        return prepareStatement(conn, sql, ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public static PreparedStatement prepareScrollableStatement(Connection conn, String sql) throws SQLException {
        return prepareStatement(conn, sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
    }

    public static PreparedStatement prepareStatement(Connection conn, String sql, int rsType, int rsConn) throws SQLException {
        if ( conn == null )
            throw new NullPointerException("Connection is null.");

        if ( logger.isDebugEnabled() )
            logger.debug("Preparing statement: rsType = " + rsType + ", rsConn = " + rsConn + ", sql = " + sql);

        final PreparedStatement pstmt = conn.prepareStatement(sql, rsType, rsConn);
        if ( noWarnings(pstmt) )
            return pstmt;

        return null;
    }

}