//
// $Id: BillingDB.java,v 1.34 2007-01-30 19:36:11 tdh Exp $
//

package diskCacheV111.util;

import dmg.util.* ;
import diskCacheV111.vehicles.* ;

import java.sql.*;

public class BillingDB {



    private final static String _billingTableName = "billinginfo";
    private final static String _storageTableName = "storageinfo";
    private final static String _doorTableName    = "doorinfo";
    private final static String _hitTableName     = "hitinfo";
    private final static String _costTableName    = "costinfo";

    private final static String _createBillingTable = "CREATE TABLE " + _billingTableName + "(" +
            "dateStamp TIMESTAMP,"     +
            "cellName VARCHAR,"        +
            "action VARCHAR,"          +
            "transaction VARCHAR,"     +
            "pnfsID VARCHAR,"          +
            "fullSize numeric,"        +
            "transferSize numeric,"    +
            "storageClass VARCHAR,"    +
            "isNew BOOLEAN,"           +
            "client VARCHAR,"          +
            "connectionTime numeric,"  +
            "errorCode numeric,"       +
            "errorMessage VARCHAR,"    +
            "protocol VARCHAR,"        +
            "initiator VARCHAR"       +
            ")";

    private final static String _createStorageTable = "CREATE TABLE " + _storageTableName + "(" +
            "dateStamp TIMESTAMP,"     +
            "cellName VARCHAR,"        +
            "action VARCHAR,"          +
            "transaction VARCHAR,"     +
            "pnfsID VARCHAR,"          +
            "fullSize numeric,"        +
            "storageClass VARCHAR,"    +
            "connectionTime numeric,"  +
            "queuedTime numeric,"      +
            "errorCode numeric,"       +
            "errorMessage VARCHAR"     +
            ")";

    private final static String _createDoorTable = "CREATE TABLE " + _doorTableName + "(" +
            "dateStamp TIMESTAMP,"     +
            "cellName VARCHAR,"        +
            "action VARCHAR,"          +
            "owner VARCHAR,"           +
            "mappedUID numeric,"       +
            "mappedGID numeric,"       +
            "client VARCHAR,"          +
            "transaction VARCHAR,"     +
            "pnfsID VARCHAR,"          +
            "connectionTime numeric,"  +
            "queuedTime numeric,"      +
            "errorCode numeric,"       +
            "errorMessage VARCHAR,"     +
            "path VARCHAR"            +
            ")";

    private final static String _createHitTable = "CREATE TABLE " + _hitTableName + "(" +
            "dateStamp TIMESTAMP,"      +
            "cellName VARCHAR,"         +
            "action VARCHAR,"           +
            "transaction VARCHAR,"      +
            "pnfsID VARCHAR,"           +
            "fileCached boolean,"       +
            "errorCode numeric,"        +
            "errorMessage VARCHAR"      +
            ")";

    private final static String _createCostTable = "CREATE TABLE " + _costTableName + "(" +
            "dateStamp TIMESTAMP,"      +
            "cellName VARCHAR,"         +
            "action VARCHAR,"           +
            "transaction VARCHAR,"      +
            "pnfsID VARCHAR,"           +
            "cost numeric,"             +
            "errorCode numeric,"        +
            "errorMessage VARCHAR"      +
            ")";

    private final static String[][] _tableList = {
        {_billingTableName , _createBillingTable},
        {_storageTableName,  _createStorageTable},
        {_doorTableName, _createDoorTable},
        {_hitTableName,  _createHitTable},
        {_costTableName, _createCostTable}
    };


    private Connection _con = null;
    private int _insertsCount = 0;
    private int _maxInsertsBeforeCommit = 1;
    private int _maxTimeBeforeCommit = 0;
    private PreparedStatement psSI = null;  // storageinfo
    private PreparedStatement psDI = null;  // doorinfo
    private PreparedStatement psMI = null;  // moverinfo (billinginfo)
    private PreparedStatement psHI = null;  // hitinfo
    private PreparedStatement psCI = null;  // costinfo

    private String _jdbcUrl;
    private String _user;
    private String _pass;

    public BillingDB(String jdbcUrl, String jdbcClass, String user, String pass, int commitNumber, int commitInterval)
    throws SQLException {

        _maxTimeBeforeCommit = commitInterval;

        if( commitNumber > 1 ) {
            _maxInsertsBeforeCommit = commitNumber;
        }

        dbInit( jdbcUrl, jdbcClass, user, pass, null);
    }

    public BillingDB(Args args) throws SQLException {

        _jdbcUrl = args.getOpt("jdbcUrl");
        String jdbcClass = args.getOpt("jdbcDriver");
        _user = args.getOpt("dbUser");
        _pass = args.getOpt("dbPass");
        String pwdfile = args.getOpt("pgPass");
        String commitInterval = args.getOpt("dbCommitTime");
        String commitNumber = args.getOpt("dbCommitNumber");

        if(commitNumber != null ) {
            _maxInsertsBeforeCommit = Integer.parseInt(commitNumber);
        }

        if(commitInterval != null ) {
            _maxTimeBeforeCommit = Integer.parseInt(commitInterval);
        }

        dbInit( _jdbcUrl, jdbcClass, _user, _pass, pwdfile);
    }

    private void dbInit(String jdbcUrl, String jdbcClass, String user, String pass, String pwdfile)
    throws SQLException {

        if( (jdbcUrl == null )  ||
            (jdbcClass == null) ||
            (user == null )     ||
            (pass == null && pwdfile == null) ) {

            throw new
                    IllegalArgumentException("Not enough arguments to Init SQL database");
        }
        if (pwdfile != null && pwdfile.trim().length() > 0) {
            Pgpass pgpass = new Pgpass(pwdfile);      //VP
            pass = pgpass.getPgpass(jdbcUrl, user);   //VP
        }

        try {

            // Add driver to JDBC
            Class.forName(jdbcClass);

            _con = DriverManager.getConnection(jdbcUrl, user, pass);

            DatabaseMetaData md = _con.getMetaData();

            for(int i = 0; i < _tableList.length; i++) {
                boolean tableExist = false;

                ResultSet tableRs = md.getTables(null, null, _tableList[i][0] , null );

                if(tableRs.next()) {
                    // table exist
                    tableExist = true;
                }

                if( !tableExist ) {
                    // Table do not exist
                    Statement s = _con.createStatement();
                    int result = s.executeUpdate(_tableList[i][1]);
                }

            }
//VP: Prepare statements for connection before using them
            prepareStatements();
//VP
            // to be fast
            _con.setAutoCommit(false);


        } catch (SQLException sqe) {
            sqe.printStackTrace();
            throw sqe;
        } catch (Exception ex) {
            throw new SQLException(ex.toString());
        }



        if( _maxTimeBeforeCommit > 0 ) {
            new TimeCommiter().start();
        }
    }

    private void prepareStatements()
    throws SQLException {
//                                                                     1  2  3  4  5  6  7  8  9 10 11 12 13 14 15
        if(psSI!=null) try { psSI.close(); } catch(SQLException sqle1) {sqle1.printStackTrace();}
        if(psDI!=null) try { psDI.close(); } catch(SQLException sqle1) {sqle1.printStackTrace();}
        if(psMI!=null) try { psMI.close(); } catch(SQLException sqle1) {sqle1.printStackTrace();}
        if(psHI!=null) try { psHI.close(); } catch(SQLException sqle1) {sqle1.printStackTrace();}
        if(psCI!=null) try { psCI.close(); } catch(SQLException sqle1) {sqle1.printStackTrace();}

        psSI = _con.prepareStatement(this.insertCMD(_storageTableName)+"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        psDI = _con.prepareStatement(this.insertCMD(   _doorTableName)+"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        psMI = _con.prepareStatement(this.insertCMD(_billingTableName)+"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        psHI = _con.prepareStatement(this.insertCMD(    _hitTableName)+"?, ?, ?, ?, ?, ?, ?, ?)");
        psCI = _con.prepareStatement(this.insertCMD(   _costTableName)+"?, ?, ?, ?, ?, ?, ?, ?)");
    }

    private void dbReconnect()
    throws SQLException {
        System.err.println("Statement execution failed. Trying to renew the connection...");
        try {
            try {
              if(_con!=null) _con.rollback();
            } catch(SQLException sqle1) { System.err.println("Could not roll back connection"); }

            _con = DriverManager.getConnection(_jdbcUrl, _user, _pass);

//VP: We've got a new connection, prepare statements once again
            prepareStatements();
//VP
            // to be fast
            _con.setAutoCommit(false);
            System.err.println("Success!");
        } catch (SQLException sqe) {
            System.err.println("Failed! Give up...");
            throw sqe;
        } catch (Exception ex) {
            System.err.println("Failed! Here is what I know...");
            throw new SQLException(ex.toString());
        }

        //if( _maxTimeBeforeCommit > 0 ) {
        //    new TimeCommiter().start();
        //}
    }

    private String insertCMD( String tableName) {
        return "INSERT INTO " + tableName + " VALUES (";
    }

    public void log( InfoMessage message) throws SQLException {

        if ( message instanceof MoverInfoMessage ) {
            this.logMoverInfoMessage( ( MoverInfoMessage) message );
            return;
        }

        if ( message instanceof StorageInfoMessage ) {
            this.logStorageInfoMessage( ( StorageInfoMessage) message );
            return;
        }

        if ( message instanceof DoorRequestInfoMessage ) {
            this.logDoorInfoMessage( ( DoorRequestInfoMessage) message );
            return;
        }

        if ( message instanceof PoolHitInfoMessage ) {
            this.logPoolHitInfoMessage( ( PoolHitInfoMessage) message );
            return;
        }

        if ( message instanceof PoolCostInfoMessage ) {
            this.logPoolCostInfoMessage( ( PoolCostInfoMessage) message );
            return;
        }


    }


    public void logStorageInfoMessage( StorageInfoMessage info) throws SQLException {

        try {
            psSI.setTimestamp( 1, new Timestamp(info.getTimestamp()));
            psSI.setString( 2, info.getCellName() );
            psSI.setString( 3, info.getMessageType() );
            psSI.setString( 4, info.getTransaction() );
            psSI.setString( 5, info.getPnfsId().getId() );
            psSI.setLong  ( 6, info.getFileSize() );
            psSI.setString( 7, info.getStorageInfo().getStorageClass()+"@"+info.getStorageInfo().getHsm() );
            psSI.setLong  ( 8, info.getTransferTime() );
            psSI.setLong  ( 9, info.getTimeQueued() );
            psSI.setInt   (10, info.getResultCode() );
            psSI.setString(11, info.getMessage() );

            int result = psSI.executeUpdate( );
            doCommitIfNeeded(false);
        } catch ( SQLException sqe) {
            System.err.println("Logging of StorageInfo to billing database failed.");
            sqe.printStackTrace();
            try {
                dbReconnect();
            } catch(SQLException se) {
                se.printStackTrace();
                throw se ;
            }
        }
    }

    public void logDoorInfoMessage( DoorRequestInfoMessage info) throws SQLException {

        try {
            psDI.setTimestamp( 1, new Timestamp(info.getTimestamp()));
            psDI.setString(  2, info.getCellName() );
            psDI.setString(  3, info.getMessageType() );
            psDI.setString(  4, info.getOwner() );
            psDI.setInt   (  5, info.getUid() );
            psDI.setInt   (  6, info.getGid() ) ;
            psDI.setString(  7, info.getClient() );
            psDI.setString(  8, info.getTransaction() );
            if (info.getPnfsId() != null) {
                psDI.setString(  9, info.getPnfsId().getId() );
            }
            else {
                psDI.setString(  9, "" );
            }
            psDI.setLong  ( 10, info.getTransactionDuration() );
            psDI.setLong  ( 11, info.getTimeQueued() );
            psDI.setInt   ( 12, info.getResultCode() );
            psDI.setString( 13, info.getMessage() );
            psDI.setString( 14, info.getPath() );

            int result = psDI.executeUpdate( );
            doCommitIfNeeded(false);
        } catch ( SQLException sqe) {
            System.err.println("Logging of DoorInfo to billing database failed.");
            sqe.printStackTrace();
            try {
                dbReconnect();
            } catch(SQLException se) {
                se.printStackTrace();
                throw se ;
            }
        }
    }


    public void logMoverInfoMessage( MoverInfoMessage info) throws SQLException  {

        try {
            String[] clients = { "<unknown>",null};
            String protocol = "<unknown>";

            if (info.getProtocolInfo() instanceof IpProtocolInfo) {
                clients = ( (IpProtocolInfo)info.getProtocolInfo() ).getHosts();
                protocol = ( (IpProtocolInfo)info.getProtocolInfo() ).getVersionString();
            }
            psMI.setTimestamp( 1, new Timestamp(info.getTimestamp()));
            psMI.setString( 2, info.getCellName() );
            psMI.setString( 3, info.getMessageType() );
            psMI.setString( 4, info.getTransaction() );
            psMI.setString( 5, info.getPnfsId().getId() );
            psMI.setLong  ( 6, info.getFileSize() );
            psMI.setLong  ( 7, info.getDataTransferred() );
            psMI.setString( 8, info.getStorageInfo().getStorageClass()+"@"+info.getStorageInfo().getHsm() );
            psMI.setBoolean(9, info.isFileCreated() );
            psMI.setString(10, clients[0] );
            psMI.setLong  (11, info.getConnectionTime() );
            psMI.setInt   (12, info.getResultCode() );
            psMI.setString(13, info.getMessage() );
            psMI.setString(14, protocol);
            psMI.setString(15, info.getInitiator() );

            int result = psMI.executeUpdate( );
            doCommitIfNeeded(false);
        } catch ( SQLException sqe) {
            System.err.println("Logging of MoverInfo to billing database failed.");
            sqe.printStackTrace();
            try {
                dbReconnect();
            } catch(SQLException se) {
                se.printStackTrace();
                throw se ;
            }
        }

    }


    public void logPoolHitInfoMessage( PoolHitInfoMessage info) throws SQLException {

        try {
            psHI.setTimestamp( 1, new Timestamp(info.getTimestamp()));
            psHI.setString( 2, info.getCellName() );
            psHI.setString( 3, info.getMessageType() );
            psHI.setString( 4, info.getTransaction() );
            psHI.setString( 5, info.getPnfsId().getId() );
            psHI.setBoolean(6, info.getFileCached() );
            psHI.setInt   ( 7, info.getResultCode() );
            psHI.setString( 8, info.getMessage() );

            int result = psHI.executeUpdate( );
            doCommitIfNeeded(false);
        } catch ( SQLException sqe) {
            System.err.println("Logging of PoolHitInfo to billing database failed.");
            sqe.printStackTrace();
            try {
                dbReconnect();
            } catch(SQLException se) {
                se.printStackTrace();
                throw se ;
            }
        }
    }


    public void logPoolCostInfoMessage( PoolCostInfoMessage info) throws SQLException {

        try {
            psCI.setTimestamp( 1, new Timestamp(info.getTimestamp()));
            psCI.setString( 2, info.getCellName() );
            psCI.setString( 3, info.getMessageType() );
            psCI.setString( 4, info.getTransaction() );
            psCI.setString( 5, info.getPnfsId().getId() );
            psCI.setDouble( 6, info.getCost() );
            psCI.setInt   ( 7, info.getResultCode() );
            psCI.setString( 8, info.getMessage() );


            int result = psCI.executeUpdate( );
            doCommitIfNeeded(false);
        } catch ( SQLException sqe) {
            System.err.println("Logging of PoolCostInfo to billing database failed.");
            sqe.printStackTrace();
            try {
                dbReconnect();
            } catch(SQLException se) {
                se.printStackTrace();
                throw se ;
            }
        }
    }


    /**
     *
     * @param mode force to commit if true
     */
    private synchronized void doCommitIfNeeded(boolean mode) {

        _insertsCount++;
        if( (_insertsCount >= _maxInsertsBeforeCommit) ||
                ( mode && _insertsCount > 0) ) {
            try{
              _con.commit();
            } catch (SQLException ignored) {
              try {
                _con.rollback();
              } catch(SQLException sqle1) {}
            }
            _insertsCount = 0;
        }
    }

    private void shrinkDB( long upTo ) {

    	PreparedStatement ps = null;
        for( int i = 0; i < _tableList.length; i++) {

            String sqlQuery = "DELETE FROM "+ _tableList[i] + " WHERE dateStamp < ?";
            try {

                ps = _con.prepareStatement(sqlQuery);
                ps.setTimestamp( 1, new Timestamp( upTo) );

                int result = ps.executeUpdate( );
                doCommitIfNeeded(false);
                ps.close();
            } catch ( SQLException ignored) { }
        }
    }

    class TimeCommiter extends Thread {

        Connection sqlCon;
        int timeout;

        TimeCommiter() {
        }

        public void run() {
            while (true) {
                try {
//                  wait(timeout);
                    Thread.sleep(_maxTimeBeforeCommit);
                    // do only if something to do
                    doCommitIfNeeded(true);
                } catch ( Exception ingnored ) {}
            }
        }
    }

}
