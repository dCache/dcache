package org.dcache.services.pinmanager1;

import com.sun.corba.se.spi.orbutil.fsm.State;
import java.util.Collection;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.Arrays;
import java.io.PrintWriter;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.DatabaseMetaData;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.Pgpass;

import org.dcache.util.JdbcConnectionPool;

class PinManagerDatabase
{
    // keep the names spelled in the lower case to make postgress
    // driver to work correctly
    private static final String PinManagerPinsTableName="pinsv3";
    private static final String PinManagerRequestsTableName = "pinrequestsv3";
    private static final String PinManagerSchemaVersionTableName = "pinmanagerschemaversion";
    private static final String PinManagerNextRequestIdTableName = "nextpinrequestid";
    
    private static final String CreatePinManagerSchemaVersionTable =
            "CREATE TABLE "+PinManagerSchemaVersionTableName+
            " ( version numeric )";
    
    
    private static final int currentSchemaVersion = 3;
    private int previousSchemaVersion;

    private static final String TABLE_PINREQUEST_V2 = "pinrequestsv2";
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
        " CONSTRAINT fk_"+PinManagerRequestsTableName+
        "_L FOREIGN KEY (PinId) REFERENCES "+
        PinManagerPinsTableName +" (Id) "+
        " ON DELETE RESTRICT"+
        ")";
    private static final String CreateNextPinRequestIdTable =
	"CREATE TABLE " + PinManagerNextRequestIdTableName + "(NEXTLONG BIGINT)";
    private static final String insertNextPinRequestId =
        "INSERT INTO " + PinManagerNextRequestIdTableName + " VALUES (0)";

    private static final String UpdatePinRequestTable =
        "UPDATE " + PinManagerPinsTableName
        + " SET Expiration=? WHERE Id=?";
    private static final String DeleteFromPinRequests =
        "DELETE FROM " + PinManagerPinsTableName + " WHERE Id=?";

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
        + " (Id, SRMId, PinId,Creation, Expiration ) VALUES (?,?,?,?,?)";
    
    private static final String deletePin =
                "DELETE FROM "+ PinManagerPinsTableName +
                " WHERE  id =?";
    private static final String deletePinRequest =
                "DELETE FROM "+ PinManagerRequestsTableName +
                " WHERE  id =?";
    
    private final String _jdbcUrl;
    private final String _jdbcClass;
    private final String _user;
    private final String _pass;
    private final PinManager _manager;

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

    public PinManagerDatabase(PinManager manager,
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

        jdbc_pool = JdbcConnectionPool.getPool(_jdbcUrl, _jdbcClass, _user, _pass);

        prepareTables();
       // readRequests();
    }

    
   private void updateSchemaVersion (Map<String,Boolean> created, Connection con) 
        throws SQLException {
        
        if(!created.get(PinManagerSchemaVersionTableName)) {
            String select = "SELECT * FROM "+
                PinManagerSchemaVersionTableName ;
            Statement s1 = con.createStatement();
             info("updateSchemaVersion trying "+select);
             ResultSet schema = s1.executeQuery(select);
             if(schema.next()) {
                 previousSchemaVersion  = schema.getInt("version");
                 String update  = "UPDATE "+
                     PinManagerSchemaVersionTableName +
                     " SET version = "+currentSchemaVersion ;
                Statement s2 = con.createStatement();
                 info("dbInit trying "+update);
                int result = s2.executeUpdate(update);
                if(result != 1) {
                    error ("update of schema version gave result="+result);
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
             info("updateSchemaVersion trying "+insert);
            int result = s1.executeUpdate(insert);
            s1.close();

        }

        if(previousSchemaVersion == currentSchemaVersion) {
            return;
        }

        if(previousSchemaVersion == 1) {
            try {
                updateSchemaToVersion3From1(con);
            }
            catch (SQLException sqle) {
                error("updateSchemaToVersion3From1 failed, shcema might have been updated already:");
                error(sqle.getMessage());
            }
            previousSchemaVersion = 3;
        }
        if(previousSchemaVersion == 2) {
            try {
                updateSchemaToVersion3(con);
            }
            catch (SQLException sqle) {
                error("updateSchemaToVersion3 failed, shcema might have been updated already:");
                error(sqle.getMessage());
            }
            previousSchemaVersion = 3;
        }
    }
    
    protected void debug(String s)
    {
        if(_manager != null)  _manager.debug(s);
        else System.out.println(s);
    }

    protected void info(String s)
    {
        if(_manager != null) _manager.info(s);
        else System.out.println(s);
    }

    protected void warn(String s)
    {
        if(_manager != null) _manager.warn(s);
        else System.out.println(s);
    }

    protected void error(String s)
    {
        if(_manager != null) _manager.error(s);
        else System.err.println(s);
    }

    protected void error(Throwable t)
    {
        if(_manager != null) _manager.error(t);
        else t.printStackTrace();
    }

    protected void fatal(String s)
    {
        if(_manager != null) _manager.error(s);
        else System.err.println(s);
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
                    info(statement);
                    int result = s.executeUpdate(statement);
                    s.close();
                }
            } catch (SQLException e) {
                warn("SQL Exception (relation could already exist): "
                     + e.getMessage());
            }
        }
    }

    
    private void updateSchemaToVersion3From1(Connection con)
        throws SQLException
    {
            String SelectEverythingFromOldPinRewquestTable =
                "SELECT PinRequestId, PnfsId, Expiration FROM "
                + TABLE_OLDPINREQUEST;
            Statement stmt = con.createStatement();
            info(SelectEverythingFromOldPinRewquestTable);

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
                pinReqsInsertStmt.executeUpdate();
                
            }
            pinsInsertStmt.close();
            pinReqsInsertStmt.close();
            stmt.close();
            stmt = con.createStatement();
            info("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.close();

            

        try {
            //check if old pins table is still there
            stmt = con.createStatement();
            info("DROP TABLE " + TABLE_OLDPINS);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINS);
            stmt.close();
            stmt = con.createStatement();
            info("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.executeUpdate("DROP TABLE " + TABLE_OLDPINREQUEST);
            stmt.close();
        } catch (SQLException e) {
            warn("Failed to drop old pinrequest table: "
                 + e.getMessage());
        }
    }

        private void updateSchemaToVersion3(Connection con) throws SQLException {
            PreparedStatement pinsInsertStmt =
                con.prepareStatement(InsertIntoPinsTable);
            PreparedStatement pinReqsInsertStmt =
                con.prepareStatement(InsertIntoPinRequestsTable);
            Statement stmt = con.createStatement();
            info(SelectAllV2Requests);
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
                }
                catch (SQLException sqle) {
                    //ignore as there possible duplications
                }
                
                pinReqsInsertStmt.setLong(1,clientId);
                pinReqsInsertStmt.setLong(2,0);
                pinReqsInsertStmt.setLong(3,pinId);
                pinReqsInsertStmt.setLong(4,0);
                pinReqsInsertStmt.setLong(5,expiration);
                pinReqsInsertStmt.executeUpdate();
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
                info("DROP TABLE " + TABLE_PINREQUEST_V2);
                stmt.executeUpdate("DROP TABLE " + TABLE_PINREQUEST_V2);
                stmt.close();
            } catch (SQLException e) {
                warn("Failed to drop PinRequests V2 table: "
                     + e.getMessage());
            }
   
    }

        
   private void prepareTables() throws SQLException {
        try {
            
            
            //connect
            Connection _con = jdbc_pool.getConnection();
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
                        info("dbinit trying "+createTables[i]);
                        int result = s.executeUpdate(createTables[i]);
                        s.close();
                       created.put(tables[i], Boolean.TRUE);
                    } catch(SQLException sqle) {
                        
                        error("SQL Exception (relation "+tables[i]+" could already exist)");
                        error(sqle.toString());
                        
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
                info("dbInit trying "+insertNextPinRequestId);
                int result = s1.executeUpdate(insertNextPinRequestId);
                s1.close();
            } else {
                info("dbInit set.next() returned nonnull");
            }
            s.close();

            // to support our transactions
            _con.setAutoCommit(false);
            jdbc_pool.returnConnection(_con);
        } catch (SQLException sqe) {
            error(sqe.toString());
            throw sqe;
        } catch (Exception ex) {
            error(ex.toString());
            throw new SQLException(ex.toString());
        }       
    }
        

    private synchronized long nextLong(Connection _con)
    {
        if (nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongIncrement = 0;
            try {
                PreparedStatement s =
                    _con.prepareStatement(SelectNextPinRequestIdForUpdate);
                info(SelectNextPinRequestIdForUpdate);
                ResultSet set = s.executeQuery();
                if (!set.next()) {
                    s.close();
                    throw new SQLException("Table " + PinManagerNextRequestIdTableName + " is empty.");
                }
                nextLongBase = set.getLong("NEXTLONG");
                s.close();
                info("nextLongBase=" + nextLongBase);
                s = _con.prepareStatement(IncreasePinRequestId);
                info(IncreasePinRequestId);
                int i = s.executeUpdate();
                s.close();
            } catch (SQLException e) {
                error("Failed to obtain ID sequence: " + e.toString());
                nextLongBase = _nextLongBase;
            }
            _nextLongBase = nextLongBase + NEXT_LONG_STEP;
        }

        long nextLong = nextLongBase + (nextLongIncrement++);
        info("nextLong=" + nextLong);
        return nextLong;
    }
    
    private void insertPin(Connection _con,
        long id,
        PnfsId pnfsId,
        long expirationTime,
        String pool,
        PinManagerPinState state) throws SQLException {
        long creationTime=System.currentTimeMillis();
        PreparedStatement pinsInsertStmt =
        _con.prepareStatement(InsertIntoPinsTable);
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
    }

    public void insertPin(
        long id,
        PnfsId pnfsId,
        long expirationTime,
        String pool,
        PinManagerPinState state) throws SQLException {
        Connection _con = null;
        try {
             _con =jdbc_pool.getConnection();
            insertPin(_con,id,pnfsId,expirationTime,pool,state);
            _con.commit();
            jdbc_pool.returnConnection(_con);
            _con = null;
        } catch(SQLException sqle) {
            error("insert failed with ");
            error(sqle.toString());
            error("ROLLBACK TRANSACTION");
            _con.rollback();
            jdbc_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        } finally {
            if(_con != null) {
                jdbc_pool.returnConnection(_con);
            }
        }
       
    }

    private void deletePin( Connection _con,
        long id) throws SQLException {
        info("executing statement: "+deletePin);
        PreparedStatement deletePinStmt =
        _con.prepareStatement(deletePin);
        deletePinStmt.setLong(1,id);
        int deleteRowCount = deletePinStmt.executeUpdate();
        if(deleteRowCount !=1 ){
            throw new SQLException("delete returned row count ="+deleteRowCount);
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
        info("executing statement: "+deletePinRequest);
        PreparedStatement deletePinReqStmt =
        _con.prepareStatement(deletePinRequest);
        deletePinReqStmt.setLong(1,id);
        int deleteRowCount = deletePinReqStmt.executeUpdate();
        if(deleteRowCount !=1 ){
            throw new SQLException("delete returned row count ="+deleteRowCount);
        }
    }
    
    public void deletePinRequest(
        long id) throws PinDBException {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        try {
             _con =jdbc_pool.getConnection();
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
        long expirationTime
        ) throws SQLException {
        long creationTime=System.currentTimeMillis();
        info("insertPinRequest()  executing statement:"+InsertIntoPinRequestsTable);
        info("?="+id+" ?="+pinId+" ?="+creationTime+" ?="+expirationTime);
        PreparedStatement pinReqsInsertStmt =
            con.prepareStatement(InsertIntoPinRequestsTable); 
        pinReqsInsertStmt.setLong(1,id);
        pinReqsInsertStmt.setLong(2,srmRequestId);
        pinReqsInsertStmt.setLong(3,pinId);
        pinReqsInsertStmt.setLong(4,creationTime);
        pinReqsInsertStmt.setLong(5,expirationTime);
        info("running insert=");
        int inserRowCount = pinReqsInsertStmt.executeUpdate();
        info("inserRowCount="+inserRowCount);
        if(inserRowCount !=1 ){
            throw new SQLException("insert returned row count ="+inserRowCount);
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
        info("executing statement: "+updatePin);
        Statement sqlStatement =
                _con.createStatement();
        sqlStatement.executeUpdate(updatePin);
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
        info("executing statement: "+updatePinRequest);
        PreparedStatement sqlStatement =
                _con.prepareStatement(updatePinRequest);
        sqlStatement.setLong(1,expiration);
        sqlStatement.setLong(2,id);
        sqlStatement.executeUpdate();
       
    }

    private static final String selectPin =
            "SELECT * FROM "+ PinManagerPinsTableName +
            " WHERE  id = ?";
    private Pin getPin(Connection _con, long id) throws SQLException{
            info("executing statement: "+selectPin);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPin);
            sqlStatement.setLong(1,id);
            ResultSet set = sqlStatement.executeQuery();
            if(!set.next()) {
                throw new SQLException("pin with id="+id+" is not found");
            }
            Pin pin = extractPinFromResultSet( set );
            sqlStatement.close();
            return pin;
    }
    
    private static final String selectPinForUpdate =
        "SELECT * FROM "+ PinManagerPinsTableName +
        " WHERE  id = ? FOR UPDATE";
    private Pin getPinForUpdate(Connection _con, long id) throws SQLException{
            info("executing statement: "+selectPinForUpdate+" ?="+id);
            
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinForUpdate);
            sqlStatement.setLong(1,id);
            ResultSet set = sqlStatement.executeQuery();
            if(!set.next()) {
                throw new SQLException("pin with id="+id+" is not found");
            }
            Pin pin = extractPinFromResultSet( set );
            sqlStatement.close();
            return pin;
    }

   private static final String selectPinRequest =
                    "SELECT * FROM "+ PinManagerRequestsTableName +
                    " WHERE  id = ?";

    private PinRequest getPinRequest(Connection _con, long id) throws SQLException{
            info("executing statement: "+selectPinRequest+" ?="+id);
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinRequest);
            sqlStatement.setLong(1,id);
            ResultSet set = sqlStatement.executeQuery();
            if(!set.next()) {
                throw new SQLException("pin with id="+id+" is not found");
            }
            PinRequest pinRequest = extractPinRequestFromResultSet( set );
            sqlStatement.close();
            return pinRequest;
    }

    private static final String selectPinRequestsByPin =
                "SELECT * FROM "+ PinManagerRequestsTableName +
                " WHERE  PinId = ?";
        
        public Set<PinRequest> getPinRequestsByPin(Connection _con, Pin pin) throws SQLException{
            info("executing statement: "+selectPinRequestsByPin+" ? "+pin.getId());
            PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinRequestsByPin);
            sqlStatement.setLong(1,pin.getId());
            ResultSet set = sqlStatement.executeQuery();
            Set<PinRequest> requests =
                    new HashSet<PinRequest>();
            while(set.next()) {
                PinRequest pinRequest =  extractPinRequestFromResultSet( set );
                pinRequest.setPin(pin);
                requests.add(pinRequest);
            }
            sqlStatement.close();
        return requests;
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
           info("executing statement: "+selectPinByPnfsIdForUpdate);
           PreparedStatement sqlStatement =
                    _con.prepareStatement(selectPinByPnfsIdForUpdate);
           sqlStatement.setString(1,pnfsId.toString());
            ResultSet set = sqlStatement.executeQuery();
            if(!set.next()) {
                return null;
            }
            Pin pin = extractPinFromResultSet( set );
            sqlStatement.close();
            return pin;
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
            error("getPin: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }
 String selectAllPins =
            "SELECT * FROM "+ PinManagerPinsTableName;
   private Collection<Pin> getAllPins(Connection _con) throws SQLException
    {
        Collection<Pin> pins = new HashSet<Pin>();

        PreparedStatement sqlStatement =
                _con.prepareStatement(selectAllPins);
        ResultSet set = sqlStatement.executeQuery();
        while(set.next()) {
            Pin pin = extractPinFromResultSet( set );
            pin.setRequests(getPinRequestsByPin(_con,pin));
            pins.add(pin);
        }
        sqlStatement.close();
        return pins;
    }

    private static final String selectPinsByState =
            "SELECT * FROM "+ PinManagerPinsTableName+
            " WHERE State = ?";
    private Collection<Pin> getPinsByState(Connection _con,PinManagerPinState state) throws SQLException
    {
        Collection<Pin> pins = new HashSet<Pin>();

        PreparedStatement sqlStatement =
                _con.prepareStatement(selectPinsByState);
        sqlStatement.setInt(1,state.getStateId());
        ResultSet set = sqlStatement.executeQuery();
        while(set.next()) {
            Pin pin = extractPinFromResultSet( set );
            pin.setRequests(getPinRequestsByPin(_con,pin));
            pins.add(pin);
        }
        sqlStatement.close();
        return pins;
    }


   private static final String selectUnpinnedPins =
            "SELECT * FROM "+ PinManagerPinsTableName+
            " WHERE State != "+PinManagerPinState.PINNED.getStateId();
    
    private Collection<Pin> getAllPinsThatAreNotPinned(Connection _con) throws SQLException
    {
        Collection<Pin> pins = new HashSet<Pin>();

        PreparedStatement sqlStatement =
                _con.prepareStatement(selectUnpinnedPins);
        ResultSet set = sqlStatement.executeQuery();
        while(set.next()) {
            Pin pin = extractPinFromResultSet( set );
            pin.setRequests(getPinRequestsByPin(_con,pin));
            pins.add(pin);
        }
        sqlStatement.close();
        return pins;
    }

    public Collection<Pin> getAllPins() throws PinDBException
    {
        Connection _con = getThreadLocalConnection();
        if(_con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
       
        try {
            return getAllPins(_con);
        } catch(SQLException sqle) {
            error("getAllPins: "+sqle);
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
            error("getAllPinsThatAreNotPinned: "+sqle);
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
            error("getPinsByState: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }
   
    private static final String selectExpiredPinRequest =
        "SELECT * FROM "+ PinManagerRequestsTableName +
        " WHERE  Expiration != -1 AND " +
        " Expiration < ?";
 
   private Set<PinRequest> getExpiredPinRequests(Connection _con) throws SQLException{
        long currentTimeMissis = System.currentTimeMillis();
        info("executing statement: "+selectExpiredPinRequest);
        PreparedStatement sqlStatement =
                _con.prepareStatement(selectExpiredPinRequest);
        sqlStatement.setLong(1,currentTimeMissis);
        ResultSet set = sqlStatement.executeQuery();
        Set<PinRequest> requests =
                new HashSet<PinRequest>();
        while(set.next()) {
            PinRequest pinRequest =  extractPinRequestFromResultSet( set );
            Pin pin = getPin(_con,pinRequest.getPinId());
            pinRequest.setPin(pin);
            requests.add(pinRequest);
        }
        sqlStatement.close();
        return requests;
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
            error("getExpiredPinRequests: "+sqle);
            throw new PinDBException(sqle.toString());
        }
    }

    private Pin extractPinFromResultSet( ResultSet set )
        throws java.sql.SQLException
    {
       return new Pin(
                        set.getLong( "id" ),
                        new PnfsId(set.getString("PnfsId")),
                        null,
                        set.getLong("Creation"),
                        set.getLong("Expiration"),
                        set.getString("Pool"),
                        set.getLong("StateTranstionTime"),
                        PinManagerPinState.getState(set.getInt("State")));
    }
     private PinRequest extractPinRequestFromResultSet( ResultSet set )
        throws java.sql.SQLException
    {
         return new PinRequest(
                        set.getLong( "id" ),
                        set.getLong("SRMId"),
                        set.getLong("PinId"),
                        set.getLong("Creation"),
                        set.getLong("Expiration"));
    }
   
    public PinRequest insertPinRequestIntoNewOrExistingPin( 
        PnfsId pnfsId,long lifetime,long srmRequestId) throws PinDBException {
        long expirationTime = 
            System.currentTimeMillis() + lifetime;
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
                    expirationTime);
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
            sqle.printStackTrace();
            throw new PinDBException("insertPinRequestIntoNewOrExistingPin failed: "+sqle);
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
        PinManagerDatabase db = new PinManagerDatabase(null,args[0],
                args[1],
                args[2],
                args[3],
                null);
        long id = db.nextLong(db.jdbc_pool.getConnection());
        db.insertPin(id,
            new PnfsId("0001000000000000000010B8"),
            System.currentTimeMillis()+1000L,null,PinManagerPinState.INITIAL);
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
            jdbc_pool.returnConnection(_con);
            _con = null;
       } catch(SQLException sqle) {
            error("commitDBOperations failed with ");
            error(sqle);
            error("ROLLBACK TRANSACTION");
            _con = null;
            throw new PinDBException(2,"commitDBOperations failed with "+
                sqle.toString()) ;
       } finally {
            clearThreadLocalConnectionl();
            if(_con != null) {
                jdbc_pool.returnConnection(_con);
            }
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
            error("rollbackDBOperations failed with ");
            error(sqle.toString());
            error("ROLLBACK TRANSACTION");
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
   
   /**
    * utility method for execution of the prepared updates
    */
   
    private int executePreparedUpdate( String statement, Object ... args) throws PinDBException
    {
        Connection con = getThreadLocalConnection();
        if(con == null) {
           throw new PinDBException(1,"DB is not initialized in this thread!!!");
        }
        debug("Executing '" + statement +
                          "' with arguments " + Arrays.toString(args));
        try {
            PreparedStatement s = con.prepareStatement(statement);
            for (int i = 0; i < args.length; i++)
                s.setObject(i + 1, args[i]);
            int count = s.executeUpdate();
            s.close();
            debug("Updated " + count + " records");
            return count;
        } catch (SQLException sqle) {
            error(sqle);
            throw new PinDBException(sqle.getMessage());
        }
     }

   
}