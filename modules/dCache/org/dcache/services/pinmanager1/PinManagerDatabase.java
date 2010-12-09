package org.dcache.services.pinmanager1;

import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.Iterator;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.Pgpass;
import org.dcache.commons.util.SqlHelper;
import org.dcache.util.JdbcConnectionPool;
import org.dcache.auth.AuthorizationRecord;
import org.dcache.auth.persistence.AuthRecordPersistenceManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class PinManagerDatabase
{
    private static final Logger _logger = LoggerFactory.getLogger(PinManagerDatabase.class);
    // keep the names spelled in the lower case to make postgress
    // driver to work correctly
    private static final String PinManagerPinsTableName="pinsv3";
    private static final String PinManagerRequestsTableName = "pinrequestsv3";
    private static final String PinManagerSchemaVersionTableName = "pinmanagerschemaversion";
    private static final String PinManagerNextRequestIdTableName = "nextpinrequestid";

    private static final String CreatePinManagerSchemaVersionTable =
            "CREATE TABLE "+PinManagerSchemaVersionTableName+
            " ( version numeric )";


    private static final int currentSchemaVersion = 4;
    private int previousSchemaVersion;

    private static final String TABLE_PINREQUEST_V2 = "pinrequestsv2";
    private static final String TABLE_OLDPINREQUEST = "pinrequestsv1";
    private static final String TABLE_OLDPINS = "pins";

    private static final long NEXT_LONG_STEP = 1000;

        // Expiration is of type long,
        // its value is time in milliseconds since the midnight of 1970 GMT (i think)
        // which has the same meaning as the value returned by
        // System.currentTimeMillis()
        // working with TIMESTAMP and with
        // java.sql.Date and java.sql.Time proved to be upredicatble and
        // too complex to work with.

    private static final String CreatePinManagerPinsTable =
        "CREATE TABLE " + PinManagerPinsTableName + " ( " +
        " Id numeric PRIMARY KEY," +
        " PnfsId VARCHAR," +
        " Creation numeric, " +
        " Expiration numeric, " +
        " Pool VARCHAR, " +
        " StateTranstionTime numeric, "+
        " State numeric" +
        ")";

    private static final String CreatePinManagerRequestsTable =
        "CREATE TABLE " + PinManagerRequestsTableName + " ( " +
        " Id numeric PRIMARY KEY," +
        " PinId numeric," +
        " SRMId numeric, "+
        " Creation numeric, " +
        " Expiration numeric ," +
        " AuthRecId numeric ," +
        " CONSTRAINT fk_"+PinManagerRequestsTableName+
        "_L FOREIGN KEY (PinId) REFERENCES "+
        PinManagerPinsTableName +" (Id) "+
        " ON DELETE RESTRICT"+
        ")";
    private static final String CreateNextPinRequestIdTable =
	"CREATE TABLE " + PinManagerNextRequestIdTableName + "(NEXTLONG BIGINT)";
    private static final String insertNextPinRequestId =
        "INSERT INTO " + PinManagerNextRequestIdTableName + " VALUES (0)";

    private static final String SelectNextPinRequestId =
        "SELECT NEXTLONG FROM " + PinManagerNextRequestIdTableName;

    /**
     * In the begining we examine the whole database to see is there
     * is a list of outstanding pins which need to be expired or timed
     * for experation.
     */
    private static final String SelectAllV2Requests =
        "SELECT PinRequestId, PnfsId, Expiration, RequestId FROM "
        + TABLE_PINREQUEST_V2;

    private static final String SelectNextPinRequestIdForUpdate =
        "SELECT NEXTLONG FROM " + PinManagerNextRequestIdTableName + " FOR UPDATE";
    private static final String IncreasePinRequestId =
        "UPDATE " + PinManagerNextRequestIdTableName +
        " SET NEXTLONG=NEXTLONG+" + NEXT_LONG_STEP;

    private static final String InsertIntoPinsTable =
        "INSERT INTO " + PinManagerPinsTableName
        + " (Id, PnfsId, Creation,Expiration,Pool, StateTranstionTime, State ) VALUES (?,?,?,?,?,?,?)";

    private static final String InsertIntoPinRequestsTable =
        "INSERT INTO " + PinManagerRequestsTableName
        + " (Id, SRMId, PinId,Creation, Expiration, AuthRecId ) VALUES (?,?,?,?,?,?)";

    private static final String deletePin =
                "DELETE FROM "+ PinManagerPinsTableName +
                " WHERE  id =?";
    private static final String deletePinRequest =
                "DELETE FROM "+ PinManagerRequestsTableName +
                " WHERE  id =?";

    private static final String AddAuthRecIdToPinRequestsTable =
        "ALTER TABLE " + PinManagerRequestsTableName
        + " ADD COLUMN AuthRecId numeric";

    private final String _jdbcUrl;
    private final String _jdbcClass;
    private final String _user;
    private final String _pass;

    /**
     * Connection pool for talking to the database.
     */
    private final JdbcConnectionPool jdbc_pool;

    /**
     * Executor used for background database updates.
     */
    private final ExecutorService _tasks =
        Executors.newSingleThreadExecutor();

    private long nextLongBase;
    private long nextLongIncrement = NEXT_LONG_STEP;

    private long nextRequestId;
    long _nextLongBase = 0;

    private AuthRecordPersistenceManager authRecordPM;

    public PinManagerDatabase(String url, String driver,
                              String user, String password,
                              String passwordfile,
                              int maxActive,
                              long maxWaitSeconds,
                              int maxIdle
        )
        throws SQLException, java.io.IOException
    {
        if (passwordfile != null && passwordfile.trim().length() > 0) {
            Pgpass pgpass = new Pgpass(passwordfile);
            password = pgpass.getPgpass(url, user);
        }

        _jdbcUrl = url;
        _jdbcClass = driver;
        _user = user;
        _pass = password;

        // Load JDBC driver
        try {
            Class.forName(_jdbcClass);
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("Could not find JDBC driver", e);
        }

        jdbc_pool = JdbcConnectionPool.getPool(
            _jdbcUrl,
            _jdbcClass,
            _user,
            _pass,
            maxActive,
            JdbcConnectionPool.WHEN_EXHAUSTED_BLOCK,
            maxWaitSeconds*1000L,
            maxIdle,
            true
            );

        prepareTables();
       // readRequests();
        authRecordPM = new AuthRecordPersistenceManager(
            _jdbcUrl,
            _jdbcClass,
            _user,
            _pass);
    }


   private void updateSchemaVersion (Map<String,Boolean> created, Connection con)
        throws SQLException {

        if(!created.get(PinManagerSchemaVersionTableName)) {
            String select = "SELECT * FROM "+
                PinManagerSchemaVersionTableName ;
            Statement s1 = con.createStatement();
             _logger.debug("updateSchemaVersion trying "+select);
             ResultSet schema = s1.executeQuery(select);
             if(schema.next()) {
                 previousSchemaVersion  = schema.getInt("version");
                 String update  = "UPDATE "+
                     PinManagerSchemaVersionTableName +
                     " SET version = "+currentSchemaVersion ;
                Statement s2 = con.createStatement();
                 _logger.debug("dbInit trying "+update);
                int result = s2.executeUpdate(update);
                if(result != 1) {
                    _logger.error("update of schema version gave result="+result);
                }
                s2.close();
             } else {
                 // nothing is found in the schema version table,
                 // pretend it was just created
                 created.put(PinManagerSchemaVersionTableName, Boolean.TRUE);
             }
             s1.close();
       }  else {


           //  schema table was just created, so we need other considerations
            // for schema migration
           DatabaseMetaData md = con.getMetaData();

          // Check if old style pin requests table is present
            ResultSet tableRs =
                md.getTables(null, null, TABLE_OLDPINREQUEST , null );
            if (tableRs.next()) {
                previousSchemaVersion = 1;
            } else {

                tableRs.close();
                tableRs =
                   md.getTables(null, null, TABLE_PINREQUEST_V2 , null );
                 if (tableRs.next()) {
                    previousSchemaVersion = 2;
                 }
            }
            tableRs.close();

            String insert = "INSERT INTO "+PinManagerSchemaVersionTableName +
                " VALUES ( "+currentSchemaVersion+" )";
                Statement s1 = con.createStatement();
             _logger.debug("updateSchemaVersion trying "+insert);
            int result = s1.executeUpdate(insert);
            s1.close();

        }
        _logger.debug(" previouos schema version is "+ previousSchemaVersion+
            " current schema version is "+currentSchemaVersion);

        if(previousSchemaVersion == currentSchemaVersion) {
            return;
        }

        if(previousSchemaVersion == 1) {
            try {
                updateSchemaToVersion3From1(con);
            }
            catch (SQLException sqle) {
                _logger.error("updateSchemaToVersion3From1 failed, schema might have been updated already:");
                _logger.error(sqle.getMessage());
            }
            previousSchemaVersion = 3;
        }
        if(previousSchemaVersion == 2) {
            try {
                updateSchemaToVersion3from2(con);
            }
            catch (SQLException sqle) {
                _logger.error("updateSchemaToVersion3 failed, schema might have been updated already:");
                _logger.error(sqle.toString());
            }
            previousSchemaVersion = 3;
        }
        if(previousSchemaVersion == 3) {
            try {
                updateSchemaToVersion4from3(con);
            }
            catch (SQLException sqle) {
                _logger.error("updateSchemaToVersion3 failed, schema might have been updated already:");
                _logger.error(sqle.toString());
            }
            previousSchemaVersion = 3;

        }
    }

    private void updateSchemaToVersion3From1(Connection con)
        throws SQLException
    {
            String SelectEverythingFromOldPinRewquestTable =
                "SELECT PinRequestId, PnfsId, Expiration FROM "
                + TABLE_OLDPINREQUEST;
            Statement stmt = con.createStatement();
            _logger.debug(SelectEverythingFromOldPinRewquestTable);

            PreparedStatement pinsInsertStmt =
                con.prepareStatement(InsertIntoPinsTable);
            PreparedStatement pinReqsInsertStmt =
                con.prepareStatement(InsertIntoPinRequestsTable);
            ResultSet rs =
                stmt.executeQuery(SelectEverythingFromOldPinRewquestTable);
            while (rs.next()) {
                long pinRequestId = rs.getLong(1);
                String pnfsId = rs.getString(2);
                long expiration = rs.getLong(3);
                try {
                    pinsInsertStmt.setLong(1, pinRequestId);
                    pinsInsertStmt.setString(2, pnfsId);
                    pinsInsertStmt.setLong(3, 0);
                    pinsInsertStmt.setLong(4, expiration);
                    pinsInsertStmt.setString(5,"unknown");
                    pinsInsertStmt.setLong(6,0);
                    pinsInsertStmt.setInt(7, PinManagerPinState.PINNED.getStateId());
                    pinsInsertStmt.executeUpdate();
                }
                catch (SQLException sqle) {
                    //ignore as there possible duplications
                }
                pinReqsInsertStmt.setLong(1,pinRequestId);
                pinReqsInsertStmt.setLong(2,0);
                pinReqsInsertStmt.setLong(3,pinRequestId);
                pinReqsInsertStmt.setLong(4,0);
                pinReqsInsertStmt.setLong(5,expiration);
                pinReqsInsertStmt.setNull(6,Types.NUMERIC);
                pinReqsInsertStmt.executeUpdate();

            }
            pinsInsertStmt.close();
            pinReqsInsertStmt.close();
            stmt.close();
            stmt = con.createStatement();
            _logger.debug("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.close();



        try {
            //check if old pins table is still there
            stmt = con.createStatement();
            _logger.debug("DROP TABLE " + TABLE_OLDPINS);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINS);
            stmt.close();
            stmt = con.createStatement();
            _logger.debug("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.close();
        } catch (SQLException e) {
            _logger.warn("Failed to drop old pinrequest table: "
                 + e.getMessage());
        }
    }

        private void updateSchemaToVersion3from2(Connection con) throws SQLException {
            PreparedStatement pinsInsertStmt =
                con.prepareStatement(InsertIntoPinsTable);
            PreparedStatement pinReqsInsertStmt =
                con.prepareStatement(InsertIntoPinRequestsTable);
            Statement stmt = con.createStatement();
            _logger.debug(SelectAllV2Requests);
            ResultSet rs = stmt.executeQuery(SelectAllV2Requests);
            while (rs.next()) {
                long pinId = rs.getLong(1);
                String pnfsId = rs.getString(2);
                long expiration = rs.getLong(3);
                long clientId = rs.getLong(4);
                try {
                    pinsInsertStmt.setLong(1, pinId);
                    pinsInsertStmt.setString(2, pnfsId);
                    pinsInsertStmt.setLong(3, 0);
                    pinsInsertStmt.setLong(4, expiration);
                    pinsInsertStmt.setString(5,"unknown");
                    pinsInsertStmt.setLong(6,0);
                    pinsInsertStmt.setInt(7, PinManagerPinState.PINNED.getStateId());
                    pinsInsertStmt.executeUpdate();

                    // pin and request ids are the same,
                    // they were one and the same in the
                    // previous version
                    long requestId=pinId;
                    pinReqsInsertStmt.setLong(1,requestId);
                    pinReqsInsertStmt.setLong(2,clientId);
                    pinReqsInsertStmt.setLong(3,pinId);
                    pinReqsInsertStmt.setLong(4,0);
                    pinReqsInsertStmt.setLong(5,expiration);
                    pinReqsInsertStmt.setNull(6,Types.NUMERIC);
                    pinReqsInsertStmt.executeUpdate();
                    }
                catch (SQLException sqle) {
                    continue;
                    //ignore as there possible duplications
                }
                /* To avoid expiring lots of pins before all requests
                 * have been read, we put all expiration dates at
                 * least 1 minute into the future.
                 *
                 * If reading the requests should take longer, this is
                 * not fatal, as the Pin class is able to handle such
                 * a situation.
                 */
            }
            rs.close();
            stmt.close();
            try {
                //check if old pins table is still there
                stmt = con.createStatement();
                _logger.debug("DROP TABLE " + TABLE_PINREQUEST_V2);
                stmt.executeUpdate("DROP TABLE " + TABLE_PINREQUEST_V2);
                stmt.close();
            } catch (SQLException e) {
                _logger.warn("Failed to drop PinRequests V2 table: "
                     + e.getMessage());
            }

    }

    private void updateSchemaToVersion4from3(Connection con) throws SQLException {
            PreparedStatement alterPinRequests =
                con.prepareStatement(AddAuthRecIdToPinRequestsTable);
            alterPinRequests.execute();

    }


   private void createIndecies() {
    try {
       PinManagerDatabase.createIndexes(jdbc_pool,PinManagerPinsTableName,"PnfsId");
    } catch (SQLException sqle) { _logger.debug("index creation failed "+ sqle); }
    try {
       PinManagerDatabase.createIndexes(jdbc_pool,PinManagerPinsTableName,"State");
    } catch (SQLException sqle) { _logger.debug("index creation failed "+ sqle); }
    try {
       PinManagerDatabase.createIndexes(jdbc_pool,PinManagerRequestsTableName,"PinId");
    } catch (SQLException sqle) { _logger.debug("index creation failed "+ sqle); }
    try {
       PinManagerDatabase.createIndexes(jdbc_pool,PinManagerRequestsTableName,"Expiration");
    } catch (SQLException sqle) { _logger.debug("index creation failed "+ sqle); }
    try {
       PinManagerDatabase.createIndexes(jdbc_pool,PinManagerRequestsTableName,"SRMId");
    } catch (SQLException sqle) { _logger.debug("index creation failed "+ sqle); }

   }

   private void prepareTables() throws SQLException {
        Connection _con = jdbc_pool.getConnection();
        try {


            //connect
            _con.setAutoCommit(true);
            //get database info
            DatabaseMetaData md = _con.getMetaData();
            // SpaceManagerNextIdTableName
            // LinkGroupTableName
            // SpaceTableName
            // SpaceFileTableName

            String tables[] = new String[] {
                PinManagerSchemaVersionTableName,
                PinManagerNextRequestIdTableName,
                PinManagerPinsTableName,
                PinManagerRequestsTableName
            };
            String createTables[] =
                    new String[] {
                CreatePinManagerSchemaVersionTable,
                CreateNextPinRequestIdTable,
                CreatePinManagerPinsTable,
                CreatePinManagerRequestsTable
            };
            Map<String,Boolean> created = new Hashtable<String,Boolean>();
            for (int i =0; i<tables.length;++i) {

                created.put(tables[i], Boolean.FALSE);

                ResultSet tableRs = md.getTables(null, null, tables[i] , null );


                if(!tableRs.next()) {
                    try {
                        Statement s = _con.createStatement();
                        _logger.debug("dbinit trying "+createTables[i]);
                        int result = s.executeUpdate(createTables[i]);
                        s.close();
                       created.put(tables[i], Boolean.TRUE);
                    } catch(SQLException sqle) {

                        _logger.error("SQL Exception (relation "+tables[i]+" could already exist)");
                        _logger.error(sqle.toString());

                    }
                }
            }

            updateSchemaVersion(created,_con);

            // need to initialize the NextToken value
            String select = "SELECT * FROM "+PinManagerNextRequestIdTableName;
            Statement s = _con.createStatement();
            ResultSet set = s.executeQuery(select);
            if(!set.next()) {
                Statement s1 = _con.createStatement();
                _logger.debug("dbInit trying "+insertNextPinRequestId);
                int result = s1.executeUpdate(insertNextPinRequestId);
                s1.close();
            } else {
                _logger.debug("dbInit set.next() returned nonnull");
            }
            s.close();

        } catch (SQLException sqe) {
            _logger.error(sqe.toString());
            throw sqe;
        } catch (Exception ex) {
            _logger.error(ex.toString());
            throw new SQLException(ex.toString());
        }
        finally {
            // to support our transactions
            _con.setAutoCommit(false);
            jdbc_pool.returnConnection(_con);

        }
        createIndecies();
   }


    private synchronized long nextLong(Connection _con)
    {
        if (nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongIncrement = 0;

            PreparedStatement stSelectNextPinRequestIdForUpdate = null;
            PreparedStatement stIncreasePinRequestId = null;
            ResultSet rsSelectNextPinRequestIdForUpdate = null;

            try {
                stSelectNextPinRequestIdForUpdate =
                    _con.prepareStatement(SelectNextPinRequestIdForUpdate);
                _logger.debug(SelectNextPinRequestIdForUpdate);
                rsSelectNextPinRequestIdForUpdate = stSelectNextPinRequestIdForUpdate.executeQuery();
                if (!rsSelectNextPinRequestIdForUpdate.next()) {
                    throw new SQLException("Table " + PinManagerNextRequestIdTableName + " is empty.");
                }
                nextLongBase = rsSelectNextPinRequestIdForUpdate.getLong("NEXTLONG");
                _logger.debug("nextLongBase=" + nextLongBase);
                stIncreasePinRequestId = _con.prepareStatement(IncreasePinRequestId);
                _logger.debug(IncreasePinRequestId);
                stIncreasePinRequestId.executeUpdate();
            } catch (SQLException e) {
                _logger.error("Failed to obtain ID sequence: " + e.toString());
                nextLongBase = _nextLongBase;
            }finally{
                SqlHelper.tryToClose(rsSelectNextPinRequestIdForUpdate);
                SqlHelper.tryToClose(stIncreasePinRequestId);
                SqlHelper.tryToClose(stSelectNextPinRequestIdForUpdate);
            }
            _nextLongBase = nextLongBase + NEXT_LONG_STEP;
        }

        long nextLong = nextLongBase + (nextLongIncrement++);
        _logger.debug("nextLong=" + nextLong);
        return nextLong;
    }

    private void insertPin(Connection _con,
        long id,
        PnfsId pnfsId,
        long expirationTime,
        String pool,
        PinManagerPinState state) throws SQLException {
        long creationTime=System.currentTimeMillis();

        PreparedStatement pinsInsertStmt = _con.prepareStatement(InsertIntoPinsTable);
        try {
            pinsInsertStmt.setLong(1, id);
            pinsInsertStmt.setString(2, pnfsId.toIdString());
            pinsInsertStmt.setLong(3, creationTime);
            pinsInsertStmt.setLong(4, expirationTime);
            pinsInsertStmt.setString(5,pool);
            pinsInsertStmt.setLong(6,creationTime);
            pinsInsertStmt.setInt(7, state.getStateId());

            int inserRowCount = pinsInsertStmt.executeUpdate();
            if(inserRowCount !=1 ){
                throw new SQLException("insert returned row count ="+inserRowCount);
            }
        }finally{
            SqlHelper.tryToClose( pinsInsertStmt );
        }
    }


    private void deletePin( Connection _con,
        long id) throws SQLException {
        _logger.debug("executing statement: "+deletePin);
        PreparedStatement deletePinStmt = _con.prepareStatement(deletePin);
        try {
            deletePinStmt.setLong(1,id);
            int deleteRowCount = deletePinStmt.executeUpdate();
            if(deleteRowCount !=1 ){
                throw new SQLException("delete returned row count ="+deleteRowCount);
            }
        }finally{
            SqlHelper.tryToClose(deletePinStmt);
        }
    }


    public void deletePin(
        long id) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            deletePin(_con,id);
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    private void deletePinRequest( Connection _con,
        long id) throws SQLException {
        _logger.debug("executing statement: "+deletePinRequest);
        PreparedStatement deletePinReqStmt = _con.prepareStatement(deletePinRequest);
        try {
            deletePinReqStmt.setLong(1,id);
            int deleteRowCount = deletePinReqStmt.executeUpdate();
            if(deleteRowCount !=1 ){
                throw new SQLException("delete returned row count ="+deleteRowCount);
            }
        }finally{
            SqlHelper.tryToClose(deletePinReqStmt);
        }
    }

    public void deletePinRequest(
        long id) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            deletePinRequest(_con,id);
            _con.commit();
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    private void insertPinRequest(Connection con,
        long id,
        long srmRequestId,
        long pinId,
        long expirationTime,
        Long authRecId
        ) throws SQLException {
        long creationTime=System.currentTimeMillis();
        _logger.debug("insertPinRequest()  executing statement:"+
            InsertIntoPinRequestsTable);
        _logger.debug("?="+id+" ?="+pinId+" ?="+creationTime+" ?="+
            expirationTime+" ?="+authRecId);

        PreparedStatement pinReqsInsertStmt = con.prepareStatement(InsertIntoPinRequestsTable);
        try {
            pinReqsInsertStmt.setLong(1,id);
            pinReqsInsertStmt.setLong(2,srmRequestId);
            pinReqsInsertStmt.setLong(3,pinId);
            pinReqsInsertStmt.setLong(4,creationTime);
            pinReqsInsertStmt.setLong(5,expirationTime);
            if(authRecId == null) {
                pinReqsInsertStmt.setNull(6,Types.NUMERIC);
            }
            else {
                pinReqsInsertStmt.setLong(6,authRecId);
            }
            _logger.debug("running insert=");
            int inserRowCount = pinReqsInsertStmt.executeUpdate();
            _logger.debug("inserRowCount="+inserRowCount);
            if(inserRowCount !=1 ){
                throw new SQLException("insert returned row count ="+inserRowCount);
            }
        }finally{
            SqlHelper.tryToClose(pinReqsInsertStmt);
        }
    }


    public void updatePin(
        long id,
        Long expirationTime,
        String pool,
        PinManagerPinState state) throws PinDBException {
        boolean found = false;
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            updatePin(
                    _con,
                    id,
                    expirationTime,
                    pool,
                    state);

        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    private void updatePin(
        Connection _con,
        long id,
        Long expirationTime,
        String pool,
        PinManagerPinState state) throws SQLException {

       boolean setField = false;
        String updatePin = "UPDATE "+PinManagerPinsTableName +
                " SET ";
        if(expirationTime != null )  {
            updatePin += "Expiration ="+expirationTime;
            setField =true;
        }
        if(pool != null) {
            if(setField) {
                updatePin += ",";
            }
            setField =true;
            updatePin += " Pool ='"+pool+"' ";
        }
        if(state != null) {
            if(setField) {
                updatePin += ",";
            }
            setField =true;
            long time = System.currentTimeMillis();
            updatePin +="StateTranstionTime= " +time+ ", ";
            updatePin += " State ="+state.getStateId()+" ";
        }
        updatePin += " WHERE id = "+id;
        _logger.debug("executing statement: "+updatePin);
        Statement sqlStatement =
                _con.createStatement();
        try {
            sqlStatement.executeUpdate(updatePin);
        }finally{
            SqlHelper.tryToClose(sqlStatement);
        }
    }

    public void updatePinRequest(
        long id,
        long expiration) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            updatePinRequest(
                    _con,
                    id,
                    expiration);

        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

   private static final String updatePinRequest = "UPDATE  "+ PinManagerRequestsTableName +
                " SET  Expiration =?"+
                " WHERE  id = ?";
    private void updatePinRequest(Connection _con,long id,long expiration)
    throws SQLException {
        _logger.debug("executing statement: "+updatePinRequest+"; ?="+expiration+" ?="+id);
        PreparedStatement sqlStatement =
                _con.prepareStatement(updatePinRequest);
        try {
            sqlStatement.setLong(1,expiration);
            sqlStatement.setLong(2,id);
            sqlStatement.executeUpdate();
        }finally{
            SqlHelper.tryToClose(sqlStatement);
        }

    }

   /**
    * @return number of records updated
    */
    public void movePinRequest(
        long requestid,
        long newPinId) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            if( 1 !=  movePinRequest(
                    _con,
                    requestid,
                    newPinId)) {
                throw new PinDBException(" pin request update failed in database");
            }

        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }


   private static final String movePinRequest = "UPDATE  "+ PinManagerRequestsTableName +
                " SET  PinId =?"+
                " WHERE  id = ?";
   /**
    * @return number of records updated
    */
    private int movePinRequest(Connection _con,long requestid,long newPinId)
    throws SQLException {
        _logger.debug("executing statement: "+movePinRequest+"; ?="+newPinId+" ?="+requestid);
        PreparedStatement sqlStatement =
                _con.prepareStatement(movePinRequest);
        try {
            sqlStatement.setLong(1,newPinId);
            sqlStatement.setLong(2,requestid);
            return sqlStatement.executeUpdate();
        }finally{
            SqlHelper.tryToClose(sqlStatement);
        }

    }

    private static final String selectPin =
            "SELECT * FROM "+ PinManagerPinsTableName +
            " WHERE  id = ?";
    private Pin getPin(Connection _con, long id) throws SQLException{
            _logger.debug("executing statement: "+selectPin);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPin);
            ResultSet set = null;
            try {
                sqlStatement.setLong(1,id);
                set = sqlStatement.executeQuery();
                if(!set.next()) {
                    throw new SQLException("pin with id="+id+" is not found");
                }
                Pin pin = extractPinFromResultSet( set );
                return pin;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }
    }

    private static final String selectPinForUpdate =
        "SELECT * FROM "+ PinManagerPinsTableName +
        " WHERE  id = ? FOR UPDATE";
    private Pin getPinForUpdate(Connection _con, long id) throws SQLException{
            _logger.debug("executing statement: "+selectPinForUpdate+" ?="+id);

            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinForUpdate);
            ResultSet set = null;
            try {
                sqlStatement.setLong(1,id);
                set = sqlStatement.executeQuery();
                if(!set.next()) {
                    throw new SQLException("pin with id="+id+" is not found");
                }
                Pin pin = extractPinFromResultSet( set );
                return pin;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }
    }

   private static final String selectPinRequest =
                    "SELECT * FROM "+ PinManagerRequestsTableName +
                    " WHERE  id = ?";

    private PinRequest getPinRequest(Connection _con, long id) throws SQLException{
            _logger.debug("executing statement: "+selectPinRequest+" ?="+id);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinRequest);
            ResultSet set = null;
            try{
                sqlStatement.setLong(1,id);
                set = sqlStatement.executeQuery();
                if(!set.next()) {
                    throw new SQLException("pin with id="+id+" is not found");
                }
                PinRequest pinRequest = extractPinRequestFromResultSet( set );
                return pinRequest;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }

    }

       private static final String selectActivePinsByPnfsId =
            "SELECT * FROM "+ PinManagerPinsTableName +
            " WHERE  PnfsId = ?"+
            " OR state = "+PinManagerPinState.INITIAL.getStateId()+
            " OR state = "+PinManagerPinState.PINNING.getStateId()+
            " FOR UPDATE";;


   private Pin getAndLockActivePinWithRequestsByPnfsId(Connection _con, PnfsId pnfsId) throws SQLException{
            _logger.debug("executing statement: "+selectActivePinsByPnfsId+" ?="+pnfsId);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectActivePinsByPnfsId);
            ResultSet set = null;
            try{
                sqlStatement.setString(1,pnfsId.toIdString());
                set = sqlStatement.executeQuery();
                if(set.next()) {
                    Pin pin = extractPinFromResultSet( set );
                    pin.setRequests(getPinRequestsByPin(_con,pin));
                    //our logic assumes that only one pin is active
                    assert !set.next();
                    return pin;
                }
                return null;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }

    }

   private static final String selectPinRequestByPnfsIdandSrmRequestId =
                    "SELECT "+PinManagerRequestsTableName+".id " +
                    "FROM "+ PinManagerRequestsTableName +", "+
                     PinManagerPinsTableName+

                    " WHERE  "+PinManagerRequestsTableName+".PinId = " +
                          PinManagerPinsTableName+".id AND "+
                    PinManagerPinsTableName+".PnfsId = ? AND "+
                    PinManagerRequestsTableName+ ".SRMId = ?";

    private long getPinRequestIdByByPnfsIdandSrmRequestId(Connection _con,
        PnfsId pnfsId, long SrmId) throws SQLException{
            _logger.debug("executing statement: "+
                selectPinRequestByPnfsIdandSrmRequestId+" ?="+pnfsId+
                " ?="+SrmId);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinRequestByPnfsIdandSrmRequestId);
            ResultSet set = null;
            try {
                sqlStatement.setString(1,pnfsId.toIdString());
                sqlStatement.setLong(2,SrmId);
                set = sqlStatement.executeQuery();
                if(!set.next()) {
                    throw new SQLException("pin request with pnfsid="+pnfsId+
                        " and SrmRequestId="+ SrmId+" is not found");
                }
                long id  = set.getLong( 1 );
                return id;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }
    }

    private static final String selectPinRequestsByPin =
                "SELECT * FROM "+ PinManagerRequestsTableName +
                " WHERE  PinId = ?";

        public Set<PinRequest> getPinRequestsByPin(Connection _con, Pin pin) throws SQLException{
            _logger.debug("executing statement: "+selectPinRequestsByPin+" ? "+pin.getId());
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinRequestsByPin);
            ResultSet set = null;
            try{
                sqlStatement.setLong(1,pin.getId());
                set = sqlStatement.executeQuery();
                Set<PinRequest> requests =
                        new HashSet<PinRequest>();
                while(set.next()) {
                    PinRequest pinRequest =  extractPinRequestFromResultSet( set );
                    pinRequest.setPin(pin);
                    requests.add(pinRequest);
                }

                return requests;
            }finally{
                SqlHelper.tryToClose(set);
                SqlHelper.tryToClose(sqlStatement);
            }
    }

   private static final String selectPinByPnfsIdForUpdate =
                    "SELECT * FROM "+ PinManagerPinsTableName +

                    " WHERE  PnfsId = ?"+
                    " AND  ( state = "+PinManagerPinState.PINNED.getStateId()+
                    " OR state = "+PinManagerPinState.INITIAL.getStateId()+
                  //  " OR state = "+PinManagerPinState.PNFSINFOWAITING.getStateId()+
                    " OR state = "+PinManagerPinState.PINNING.getStateId()+
                    " )"+ " FOR UPDATE " ;

    private Pin lockPinForInsertOfNewPinRequest(Connection _con, PnfsId pnfsId) throws SQLException{
           _logger.debug("executing statement: "+selectPinByPnfsIdForUpdate);
           PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinByPnfsIdForUpdate);
           ResultSet set = null;
           try {
           sqlStatement.setString(1,pnfsId.toString());
            set = sqlStatement.executeQuery();
            if(!set.next()) {
                return null;
            }
            Pin pin = extractPinFromResultSet( set );
            return pin;
           }finally{
               SqlHelper.tryToClose(set);
               SqlHelper.tryToClose(sqlStatement);
           }
    }


    public Pin getPin(long id)  throws PinDBException{
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            Pin pin =  getPin(_con,id);
            pin.setRequests(getPinRequestsByPin(_con,pin));
            return pin;

        } catch(SQLException sqle) {
            _logger.error("getPin: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }
 private static final String selectAllPins =
            "SELECT * FROM "+ PinManagerPinsTableName;
   private void allPinsToStringBuilder(Connection _con, StringBuilder sb) throws SQLException
    {
           _logger.debug("executing statement: "+selectAllPins);
         PreparedStatement sqlStatement =
                _con.prepareStatement(selectAllPins);
         ResultSet set = null;
         try {
            set = sqlStatement.executeQuery();
            int pcount = 0;
            int preqcount = 0;
            while(set.next()) {
                pcount++;
                Pin pin = extractPinFromResultSet( set );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                preqcount += pin.getRequestsNum();
                sb.append(pin.toString()).append('\n');
            }

            if(pcount == 0)  {
                sb.append("no files are pinned");
            } else {
                sb.append("total number of pins: ").append(pcount);
                sb.append("\n total number of pin requests:").append(preqcount);
            }

         }finally{
             SqlHelper.tryToClose(set);
             SqlHelper.tryToClose(sqlStatement);
         }

    }

   private static final String selectAllPinsWithPnfsid =
            "SELECT * FROM "+ PinManagerPinsTableName+" WHERE PnfsId =?";

   private void allPinsByPnfsIdToStringBuilder(Connection _con, PnfsId pnfsId,
       StringBuilder sb) throws SQLException
    {
        _logger.debug("executing statement: "+selectAllPinsWithPnfsid);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectAllPinsWithPnfsid);
        ResultSet set = null;
        try {
            sqlStatement.setString(1,pnfsId.toIdString());
            set = sqlStatement.executeQuery();
            int pcount = 0;
            int preqcount = 0;
            while(set.next()) {
                pcount++;
                Pin pin = extractPinFromResultSet( set );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                preqcount += pin.getRequestsNum();
                sb.append(pin.toString()).append('\n');
            }

            if(pcount == 0)  {
                sb.append("no files are pinned");
            } else {
                sb.append("total number of pins: ").append(pcount);
                sb.append("\n total number of pin requests:").append(preqcount);
            }
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }
    }

    public Set<Pin> allPinsByPnfsId(PnfsId pnfsId)  throws PinDBException{
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            Set<Pin> pins =  allPinsByPnfsId(_con,pnfsId);
            return pins;

        } catch(SQLException sqle) {
            _logger.error("getPin: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

    private Set<Pin>  allPinsByPnfsId(Connection _con, PnfsId pnfsId)
        throws SQLException
    {
        Set<Pin> pins = new HashSet<Pin>();
        _logger.debug("executing statement: "+selectAllPinsWithPnfsid);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectAllPinsWithPnfsid);
        ResultSet set = null;
        try {
            sqlStatement.setString(1,pnfsId.toIdString());
            set = sqlStatement.executeQuery();
            while(set.next()) {
                Pin pin = extractPinFromResultSet( set );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                pins.add(pin);
            }
            return pins;
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }

    }

    private static final String selectPinsByState =
            "SELECT * FROM "+ PinManagerPinsTableName+
            " WHERE State = ?";
    private Collection<Pin> getPinsByState(Connection _con,PinManagerPinState state) throws SQLException
    {
        Collection<Pin> pins = new HashSet<Pin>();

        PreparedStatement sqlStatement =
                _con.prepareStatement(selectPinsByState);
        ResultSet set = null;
        try{
            sqlStatement.setInt(1,state.getStateId());
            set = sqlStatement.executeQuery();
            while(set.next()) {
                Pin pin = extractPinFromResultSet( set );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                pins.add(pin);
            }
            return pins;
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }
    }


   private static final String selectUnpinnedPins =
            "SELECT * FROM "+ PinManagerPinsTableName+
            " WHERE State != "+PinManagerPinState.PINNED.getStateId();

    private Collection<Pin> getAllPinsThatAreNotPinned(Connection _con) throws SQLException
    {
        Collection<Pin> pins = new HashSet<Pin>();

        PreparedStatement sqlStatement =
                _con.prepareStatement(selectUnpinnedPins);
        ResultSet set = null;
        try {
            set = sqlStatement.executeQuery();
            while(set.next()) {
                Pin pin = extractPinFromResultSet( set );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                pins.add(pin);
            }
            return pins;
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }
    }

    public void allPinsToStringBuilder(StringBuilder sb) throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
             allPinsToStringBuilder(_con,sb);
        } catch(SQLException sqle) {
            _logger.error("getAllPinsThatAreNotPinned: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

    public void allPinsByPnfsIdToStringBuilder(StringBuilder sb, PnfsId pnfsId) throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
             allPinsByPnfsIdToStringBuilder(_con,pnfsId,sb);
        } catch(SQLException sqle) {
            _logger.error("allPinsByPnfsIdToStringBuilder: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }


    public Collection<Pin> getAllPinsThatAreNotPinned() throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            return getAllPinsThatAreNotPinned(_con);
        } catch(SQLException sqle) {
            _logger.error("getAllPinsThatAreNotPinned: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

   public Collection<Pin> getPinsByState(PinManagerPinState state) throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            return getPinsByState(_con,state);
        } catch(SQLException sqle) {
            _logger.error("getPinsByState: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

   private static final String selectExpiredPinRequest =
        "SELECT * FROM "+ PinManagerRequestsTableName +
        " WHERE  Expiration != -1 AND " +
        " Expiration < ?";

   private Set<PinRequest> getExpiredPinRequests(Connection _con) throws SQLException{
        long currentTimeMissis = System.currentTimeMillis();
        _logger.debug("executing statement: "+selectExpiredPinRequest+" ?="+currentTimeMissis);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectExpiredPinRequest);
        ResultSet set = null;

        try {
            sqlStatement.setLong(1,currentTimeMissis);
            set = sqlStatement.executeQuery();
            Set<PinRequest> requests =
                    new HashSet<PinRequest>();
            while(set.next()) {
                PinRequest pinRequest =  extractPinRequestFromResultSet( set );
                Pin pin = getPin(_con,pinRequest.getPinId());
                pinRequest.setPin(pin);
                requests.add(pinRequest);
            }
            return requests;
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }
    }

    public Set<PinRequest> getExpiredPinRequests() throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            return getExpiredPinRequests(_con);
        } catch(SQLException sqle) {
            _logger.error("getExpiredPinRequests: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

   private static final String selectIDsOfExpiredPinWithoutRequest =
       " SELECT AllPinIds.Id FROM \n"+
        "    ( SELECT "+
            PinManagerPinsTableName+".Id as Id, "+
            PinManagerPinsTableName+".Expiration as Expiration, "+
            PinManagerRequestsTableName+".PinId as PinId\n"+

       "      FROM "+ PinManagerPinsTableName +
            " LEFT OUTER JOIN " +PinManagerRequestsTableName +'\n'+
       "      ON "+ PinManagerPinsTableName+".Id = "+
        PinManagerRequestsTableName+".PinId \n"+
       "      GROUP BY "+
            PinManagerPinsTableName+".Id, "+
            PinManagerPinsTableName+".Expiration, "+
            PinManagerRequestsTableName+".PinId "+
       " ) AS AllPinIds \n"+
        " WHERE  AllPinIds.PinId IS NULL AND "  +
        " AllPinIds.Expiration != -1 AND " +
        " AllPinIds.Expiration < ?";


  private Set<Pin> getExpiredPinsWithoutRequests(Connection _con) throws SQLException{
        long currentTimeMissis = System.currentTimeMillis();
        _logger.debug("executing statement: "+selectIDsOfExpiredPinWithoutRequest+" ?="+currentTimeMissis);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectIDsOfExpiredPinWithoutRequest);
        ResultSet set = null;
        try{
            sqlStatement.setLong(1,currentTimeMissis);
            set = sqlStatement.executeQuery();
            Set<Pin> pins =
                    new HashSet<Pin>();
            while(set.next()) {
                Pin pin =  getPin(_con,set.getLong(1) );
                pin.setRequests(getPinRequestsByPin(_con,pin));
                assert pin.getRequests().isEmpty() ;
                pins.add(pin);
            }
            return pins;
        }finally{
            SqlHelper.tryToClose(set);
            SqlHelper.tryToClose(sqlStatement);
        }
    }

    public Set<Pin> getExpiredPinsWithoutRequests() throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            return getExpiredPinsWithoutRequests(_con);
        } catch(SQLException sqle) {
            _logger.error("getExpiredPinRequests: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }


    private Pin extractPinFromResultSet( ResultSet set )
        throws java.sql.SQLException
    {
       return new Pin(
                        set.getLong( "id" ),
                        new PnfsId(set.getString("PnfsId")),
                        set.getLong("Creation"),
                        set.getLong("Expiration"),
                        set.getString("Pool"),
                        set.getLong("StateTranstionTime"),
                        PinManagerPinState.getState(set.getInt("State")));
    }
     private PinRequest extractPinRequestFromResultSet( ResultSet set )
        throws java.sql.SQLException
    {
         AuthorizationRecord ar = null;
         long authRecId = set.getLong("AuthRecId");
         if(authRecId == 0 && set.wasNull() ) {
             _logger.debug("authRecId is NULL");
         } else {
             ar = authRecordPM.find(authRecId);
         }

         return new PinRequest(
                        set.getLong( "id" ),
                        set.getLong("SRMId"),
                        set.getLong("PinId"),
                        set.getLong("Creation"),
                        set.getLong("Expiration"),
                        ar
                        );
    }

    public PinRequest insertPinRequestIntoNewOrExistingPin(
        PnfsId pnfsId,
        long lifetime,
        long srmRequestId,
        AuthorizationRecord authRec) throws PinDBException {
        long expirationTime = lifetime == -1?
            -1:
            System.currentTimeMillis() + lifetime;
        if(authRec != null) {
            authRecordPM.persist(authRec);
        }
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            Pin pin = lockPinForInsertOfNewPinRequest(_con,pnfsId);
            if(pin == null) {
                long id = nextLong(_con);
                insertPin(_con,
                    id,
                    pnfsId,
                    expirationTime,
                    null,
                    PinManagerPinState.INITIAL);
                pin = lockPinForInsertOfNewPinRequest(_con,pnfsId);
            }

            if( pin == null)
            {
                throw new PinDBException(1,"Could not get or lock pin");
            }

            long requestId =  nextLong(_con);
            insertPinRequest(_con,requestId,srmRequestId,pin.getId(),
                    expirationTime,
                    authRec==null?null:authRec.getId());
            pin.setRequests(getPinRequestsByPin(_con,pin));
            PinRequest pinRequest = null;
            for(PinRequest aPinRequest : pin.getRequests()) {
                if(aPinRequest.getId() == requestId) {
                    pinRequest= aPinRequest;
                    break;
                }
            }
            assert pinRequest != null;
            return pinRequest;
        } catch(SQLException sqle) {
            _logger.error(sqle.toString());
            throw new PinDBException("insertPinRequestIntoNewOrExistingPin failed: "+sqle);
        }
    }

    public Pin newPinForPinMove(
        PnfsId pnfsId,
        String newPool,
        long expirationTime) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            long id = nextLong(_con);
            insertPin(_con,
                id,
                pnfsId,
                expirationTime,
                newPool,
                PinManagerPinState.MOVING);
            return getPin( _con, id);
        } catch(SQLException sqle) {
            _logger.error(sqle.toString());
            throw new PinDBException("newPinForPinMove failed: "+sqle);
        }
    }

    public Pin newPinForRepin(
        PnfsId pnfsId,
        long expirationTime) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }

        try {
            long id = nextLong(_con);
            insertPin(_con,
                id,
                pnfsId,
                expirationTime,
                null,
                PinManagerPinState.PINNING);
            return getPin( _con, id);
        } catch(SQLException sqle) {
            _logger.error(sqle.toString());
            throw new PinDBException("newPinForRepin failed: "+sqle);
        }
    }


    public Pin getPinForUpdateByRequestId(long requestId)
    throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            PinRequest request = getPinRequest(_con,requestId);
            long pinId = request.getPinId();
            Pin pin = getPinForUpdate(_con, pinId);
            pin.setRequests(getPinRequestsByPin(_con,pin));
            return pin;
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    public Pin getAndLockActivePinWithRequestsByPnfstId(PnfsId pnfsId)
    throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            return getAndLockActivePinWithRequestsByPnfsId(_con,pnfsId);
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    public long getPinRequestIdByByPnfsIdandSrmRequestId(PnfsId pnfsId, long srmrequestId)
    throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            long requestId =
                getPinRequestIdByByPnfsIdandSrmRequestId(_con,pnfsId,srmrequestId);
            return requestId;
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }


    public Pin getPinForUpdate(long pinId)
    throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
            Pin pin = getPinForUpdate(_con, pinId);
            pin.setRequests(getPinRequestsByPin(_con,pin));
            return pin;
        } catch (SQLException sqle) {
            throw new PinDBException(sqle.toString());
        }
    }

    public void getInfo(PrintWriter writer)
    {
        writer.println("\tjdbcClass=" + _jdbcClass);
        writer.println("\tjdbcUrl=" + _jdbcUrl);
        writer.println("\tjdbcUser=" + _user);
    }

    //use this for testing
    public static final void main(String[] args) throws Exception {
        if(args == null || args.length <4) {
            System.err.println("Usage: java org.dcache.services.pinmanager1.PinManagerDatabase "+
                    " jdbcUrl jdbcClass user pass ");
            System.exit(1);
        }
        PinManagerDatabase db = new PinManagerDatabase(args[0],
                args[1],
                args[2],
                args[3],
                null,
                10,
                10L,
                10);
        long id = db.nextLong(db.jdbc_pool.getConnection());
        Pin pin = db.getPin(id);
        System.out.println(pin.toString());
        db.updatePin(id,null,"pool1",PinManagerPinState.PINNING);
        pin = db.getPin(id);
        System.out.println(pin.toString());
        db.updatePin(id,null,"pool1",PinManagerPinState.PINNED);
        pin = db.getPin(id);
        System.out.println(pin.toString());
        db.deletePin(id);
        pin = db.getPin(id);
        System.out.println(pin.toString());

    }

    private static final ThreadLocal<Connection> threadLocalConnection =
        new ThreadLocal<Connection> () ;

    private static final Connection getThreadLocalConnection() {
        return threadLocalConnection.get();

    }

   private static final void clearThreadLocalConnectionl() {
        threadLocalConnection.remove();
    }

   private static final void setThreadLocalConnection(Connection con) {
       threadLocalConnection.set(con);
   }

   public void initDBConnection() throws PinDBException {
       if( getThreadLocalConnection() != null ) {
           throw new PinDBException(1,"DB is already initialized in this thread!!!");
       }
       try {
        threadLocalConnection.set(jdbc_pool.getConnection());
       } catch (SQLException sqle) {
           throw new PinDBException(sqle.toString());
       }
   }

   public void commitDBOperations() throws PinDBException {
       Connection _con = getThreadLocalConnection() ;
       if( _con == null ) {
           return;
       }

       try {
           _con.commit();
       } catch(SQLException sqle) {
            _logger.error("commitDBOperations failed with ");
            _logger.error(sqle.toString());
            _logger.error("ROLLBACK TRANSACTION");
            try {
                _con.rollback();
            } catch (SQLException sqle1) {
                _logger.error(sqle1.toString());
            }
            throw new PinDBException(2,"commitDBOperations failed with "+
                sqle.toString()) ;
       } finally {
           jdbc_pool.returnConnection(_con);
           clearThreadLocalConnectionl();
       }
   }

   public void rollbackDBOperations() throws PinDBException {
       Connection _con = getThreadLocalConnection() ;
       if( _con == null ) {
           return;
       }

       try {
           _con.rollback();
            jdbc_pool.returnConnection(_con);
            _con = null;
       } catch(SQLException sqle) {
            _logger.error("rollbackDBOperations failed with ");
            _logger.error(sqle.toString());
            _logger.error("ROLLBACK TRANSACTION");
            jdbc_pool.returnFailedConnection(_con);
            _con = null;
            throw new PinDBException(2,"rollbackDBOperations failed with "+
                sqle.toString()) ;
       } finally {
            clearThreadLocalConnectionl();
            if(_con != null) {
                jdbc_pool.returnConnection(_con);
            }
       }
   }


     private static void  createIndexes(JdbcConnectionPool connectionPool,
            String name,
            String ... columns)
		throws SQLException {
		Connection connection = null;
		try {
				connection = connectionPool.getConnection();
				DatabaseMetaData md = connection.getMetaData();
				ResultSet set       = md.getIndexInfo(null,
								      null,
								      name,
								      false,
								      false);
				HashSet<String> listOfColumnsToBeIndexed = new HashSet<String>();
				for (String column : columns) {
					listOfColumnsToBeIndexed.add(column.toLowerCase());
				}
				while(set.next()) {
					String s = set.getString("column_name").toLowerCase();
					if (listOfColumnsToBeIndexed.contains(s)) {
						listOfColumnsToBeIndexed.remove(s);
					}
				}
				for (Iterator<String> i=listOfColumnsToBeIndexed.iterator();i.hasNext();) {
					String column = i.next();
					String indexName=name.toLowerCase()+"_"+column+"_idx";
					String createIndexStatementText = "CREATE INDEX "+indexName+" ON "+name+" ("+column+")";
					Statement s = connection.createStatement();
					int result = s.executeUpdate(createIndexStatementText);
					connection.commit();
					s.close();
				}
		}
		catch (SQLException e) {
			connectionPool.returnFailedConnection(connection);
			connection = null;
			throw e;
		}
		finally {
			if(connection != null) {
				connectionPool.returnConnection(connection);
				connection = null;
			}
		}
	}


}
