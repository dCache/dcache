package org.dcache.services.pinmanager;

import java.util.List;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.io.PrintWriter;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.Pgpass;

import diskCacheV111.services.JdbcConnectionPool;

class PinRequestDatabase
{
    // keep the names spelled in the lower case to make postgress
    // driver to work correctly

    private static final String TABLE_PINREQUEST = "pinrequestsv2";
    private static final String TABLE_NEXTPINREQUESTID = "nextpinrequestid";
    private static final String TABLE_OLDPINREQUEST = "pinrequestsv1";
    private static final String TABLE_OLDPINS = "pins";

    private static final long NEXT_LONG_STEP = 1000;

    /**
     * we are going to use the currentTimeMillis as the next
     * PinRequestId so that it can be used as an identifier for the
     * request and the creation time stamp if the PinRequestId already
     * exists, we will increment by one until we get a unique one
     */

        // Expiration is of type long,
        // its value is time in milliseconds since the midnight of 1970 GMT (i think)
        // which has the same meaning as the value returned by
        // System.currentTimeMillis()
        // working with TIMESTAMP and with
        // java.sql.Date and java.sql.Time proved to be upredicatble and
        // too complex to work with.

    private static final String CreatePinRequestTable =
        "CREATE TABLE " + TABLE_PINREQUEST + " ( " +
        " PinRequestId numeric PRIMARY KEY," +
        " PnfsId VARCHAR," + //forein key
        " Expiration numeric, " +
        " RequestId numeric" +
        ");";
    private static final String CreateNextPinRequestIdTable =
	"CREATE TABLE " + TABLE_NEXTPINREQUESTID + "(NEXTLONG BIGINT)";
    private static final String insertNextPinRequestId =
        "INSERT INTO " + TABLE_NEXTPINREQUESTID + " VALUES (0)";

    private static final String InsertIntoPinRequestTable =
        "INSERT INTO " + TABLE_PINREQUEST
        + " (PinRequestId, PnfsId, Expiration, RequestId) VALUES (?,?,?,?)";
    private static final String UpdatePinRequestTable =
        "UPDATE " + TABLE_PINREQUEST
        + " SET Expiration=? WHERE PinRequestId=?";
    private static final String DeleteFromPinRequests =
        "DELETE FROM " + TABLE_PINREQUEST + " WHERE PinRequestId=?";

    private static final String SelectNextPinRequestId =
        "SELECT NEXTLONG FROM " + TABLE_NEXTPINREQUESTID;

    /**
     * In the begining we examine the whole database to see is there
     * is a list of outstanding pins which need to be expired or timed
     * for experation.
     */
    private static final String SelectEverything =
        "SELECT PinRequestId, PnfsId, Expiration, RequestId FROM "
        + TABLE_PINREQUEST;

    private static final String SelectNextPinRequestIdForUpdate =
        "SELECT NEXTLONG FROM " + TABLE_NEXTPINREQUESTID + " FOR UPDATE";
    private static final String IncreasePinRequestId =
        "UPDATE " + TABLE_NEXTPINREQUESTID +
        " SET NEXTLONG=NEXTLONG+" + NEXT_LONG_STEP;

    private final String _jdbcUrl;
    private final String _jdbcClass;
    private final String _user;
    private final String _pass;
    private final PinManager _manager;

    /**
     * Connection pool for talking to the database.
     */
    private final JdbcConnectionPool _pool;

    /**
     * Executor used for background database updates.
     */
    private final ExecutorService _tasks =
        Executors.newSingleThreadExecutor();

    private long nextLongBase;
    private long nextLongIncrement = NEXT_LONG_STEP;

    private long nextRequestId;
    long _nextLongBase = 0;

    public PinRequestDatabase(PinManager manager,
                              String url, String driver,
                              String user, String password,
                              String passwordfile)
        throws SQLException
    {
        if (passwordfile != null && passwordfile.trim().length() > 0) {
            Pgpass pgpass = new Pgpass(passwordfile);
            password = pgpass.getPgpass(url, user);
        }

        _jdbcUrl = url;
        _jdbcClass = driver;
        _manager = manager;
        _user = user;
        _pass = password;

        // Load JDBC driver
        try {
            Class.forName(_jdbcClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find JDBC driver", e);
        }

        _pool = JdbcConnectionPool.getPool(_jdbcUrl, _jdbcClass, _user, _pass);

        prepareTables();
        readRequests();
    }

    protected void debug(String s)
    {
        _manager.debug(s);
    }

    protected void info(String s)
    {
        _manager.info(s);
    }

    protected void warn(String s)
    {
        _manager.warn(s);
    }

    protected void error(String s)
    {
        _manager.error(s);
    }

    protected void fatal(String s)
    {
        _manager.error(s);
    }

    private void createTable(Connection con, String name,
                             String ... statements)
        throws SQLException
    {
        DatabaseMetaData md = con.getMetaData();
        ResultSet tableRs = md.getTables(null, null, name, null);
        if (!tableRs.next()) {
            try {
                for (String statement : statements) {
                    Statement s = con.createStatement();
                    debug(statement);
                    int result = s.executeUpdate(statement);
                    s.close();
                }
            } catch (SQLException e) {
                warn("SQL Exception (relation could already exist): "
                     + e.getMessage());
            }
        }
    }

    private void migrateTables(Connection con)
        throws SQLException
    {
        DatabaseMetaData md = con.getMetaData();

        // Check if old style pin requests table is present
        try {
            ResultSet tableRs =
                md.getTables(null, null, TABLE_OLDPINREQUEST , null );
            if (tableRs.next()) {
                // it is still there
                // copy everything into the new table
                String SelectEverythingFromOldPinRewquestTable =
                    "SELECT PinRequestId, PnfsId, Expiration FROM "
                    + TABLE_OLDPINREQUEST;
                Statement stmt = con.createStatement();
                debug(SelectEverythingFromOldPinRewquestTable);

                PreparedStatement stmt1 =
                    con.prepareStatement(InsertIntoPinRequestTable);
                ResultSet rs =
                    stmt.executeQuery(SelectEverythingFromOldPinRewquestTable);
                while (rs.next()) {
                    stmt1.setString(1, rs.getString(1));
                    stmt1.setString(2, rs.getString(2));
                    stmt1.setLong(3, rs.getLong(3));
                    stmt1.setNull(4, java.sql.Types.NUMERIC);
                    stmt1.executeUpdate();
                }
                stmt1.close();
                stmt.close();
                stmt = con.createStatement();
                debug("DROP TABLE " + TABLE_OLDPINREQUEST);
                stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINREQUEST);
                stmt.close();

            }
        } catch (SQLException e) {
            warn("Failed to read values from old pinrequest table: "
                 + e.getMessage());
        }

        try {
            //check if old pins table is still there
            ResultSet tableRs = md.getTables(null, null, TABLE_OLDPINS, null);
            if (tableRs.next()) {
                Statement stmt = con.createStatement();
                debug("DROP TABLE " + TABLE_OLDPINS);
                stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINS);
                stmt.close();
            }
        } catch (SQLException e) {
            warn("Failed to read values from old pinrequest table: "
                 + e.getMessage());
        }
    }

    private void prepareTables() throws SQLException
    {
        Connection con = _pool.getConnection();
        con.setAutoCommit(true);
        try {
            createTable(con, TABLE_PINREQUEST, CreatePinRequestTable);
            createTable(con, TABLE_NEXTPINREQUESTID,
                        CreateNextPinRequestIdTable, insertNextPinRequestId);
            migrateTables(con);

            Statement s = con.createStatement();
            debug(SelectNextPinRequestId);
            ResultSet rs = s.executeQuery(SelectNextPinRequestId);
            if (rs.next()) {
                nextRequestId = rs.getLong(1);
                nextRequestId++;
            } else {
                error("Cannot read nextRequestId, using system time instead.");
                nextRequestId = System.currentTimeMillis();
            }
            s.close();

            // to support our transactions
            con.setAutoCommit(false);
            _pool.returnConnection(con);
            con = null;
        } catch (SQLException e) {
            error("Failed to prepare tables: " + e.toString());
            throw e;
        } finally {
            if (con != null)
                _pool.returnFailedConnection(con);
        }
    }

    /**
     * This method reads the pin requests from the database and either
     * expires them or starts timers, depending on the expiration time
     * of each pin.
     */
    public void readRequests() throws SQLException
    {
        long currentTimestamp = System.currentTimeMillis();
        Connection con = null;
        try {
            con = _pool.getConnection();
            Statement stmt = con.createStatement();
            debug(SelectEverything);
            ResultSet rs = stmt.executeQuery(SelectEverything);
            while (rs.next()) {
                long pinId = rs.getLong(1);
                PnfsId pnfsId = new PnfsId(rs.getString(2));
                long expiration = rs.getLong(3);
                long clientId = rs.getLong(4);

                /* To avoid expiring lots of pins before all requests
                 * have been read, we put all expiration dates at
                 * least 1 minute into the future.
                 *
                 * If reading the requests should take longer, this is
                 * not fatal, as the Pin class is able to handle such
                 * a situation.
                 */
                if (expiration <= currentTimestamp + 60 * 1000) {
                    expiration = currentTimestamp + 60 * 1000;
                }

                PinRequest request =
                    new PinRequest(pinId, 
                    expiration, 
                    clientId,
                    null);
                _manager.getPin(pnfsId).recover(request);
            }
            rs.close();
            stmt.close();
            _pool.returnConnection(con);
            con = null;
        } catch(SQLException e) {
            error("Recovery of old requests failed: " + e.toString());
            throw e;
        } finally {
            if (con != null) {
                _pool.returnFailedConnection(con);
            }
        }
    }

    public synchronized long nextLong()
    {
        if (nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongIncrement = 0;
            Connection _con = null;
            try {
                _con = _pool.getConnection();
                PreparedStatement s =
                    _con.prepareStatement(SelectNextPinRequestIdForUpdate);
                debug(SelectNextPinRequestIdForUpdate);
                ResultSet set = s.executeQuery();
                if (!set.next()) {
                    s.close();
                    throw new SQLException("Table " + TABLE_NEXTPINREQUESTID + " is empty.");
                }
                nextLongBase = set.getLong("NEXTLONG");
                s.close();
                debug("nextLongBase=" + nextLongBase);
                s = _con.prepareStatement(IncreasePinRequestId);
                debug(IncreasePinRequestId);
                int i = s.executeUpdate();
                s.close();
                _con.commit();
            } catch (SQLException e) {
                error("Failed to obtain ID sequence: " + e.toString());
                try {
                    _con.rollback();
                } catch (SQLException e1) {

                }
                _pool.returnFailedConnection(_con);
                _con = null;
                nextLongBase = _nextLongBase;
            } finally {
                if (_con != null) {
                    _pool.returnConnection(_con);

                }
            }
            _nextLongBase = nextLongBase + NEXT_LONG_STEP;
        }

        long nextLong = nextLongBase + (nextLongIncrement++);
        debug("nextLong=" + nextLong);
        return nextLong;
    }

    protected void submitUpdateStatement(String statement, Object ... args)
    {
        _tasks.submit(new PreparedUpdateTask(_pool, statement, args));
    }

    public PinRequest createRequest(PnfsId pnfsId,
                                    long expiration, 
                                    long clientId,
                                    String clientHost)
    {
        long pinId = nextLong();
        PinRequest request = new PinRequest(pinId, 
            expiration, 
            clientId,
            clientHost);
        submitUpdateStatement(InsertIntoPinRequestTable,
                              pinId, pnfsId.toString(),
                              expiration, clientId);
        return request;
    }

    public void updateRequest(PinRequest request)
    {
        submitUpdateStatement(UpdatePinRequestTable,
                              request.getExpiration(),
                              request.getPinRequestId());
    }

    public void deleteRequest(PinRequest request)
    {
        submitUpdateStatement(DeleteFromPinRequests,
                              request.getPinRequestId());
    }

    public void getInfo(PrintWriter writer)
    {
        writer.println("\tjdbcClass=" + _jdbcClass);
        writer.println("\tjdbcUrl=" + _jdbcUrl);
        writer.println("\tjdbcUser=" + _user);
    }
}