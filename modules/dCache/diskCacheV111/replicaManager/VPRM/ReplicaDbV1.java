// $Id: ReplicaDbV1.java,v 1.35.10.7 2007-10-12 23:35:36 aik Exp $

package diskCacheV111.replicaManager ;

import  diskCacheV111.util.* ;
import  dmg.cells.nucleus.*;

import  java.util.* ;
import  java.io.* ;
import  java.sql.*;
import  java.text.*;

/**
 * This class works with replicas database
 */
public class ReplicaDbV1 implements ReplicaDb1 {
    private final static String _cvsId = "$Id: ReplicaDbV1.java,v 1.35.10.7 2007-10-12 23:35:36 aik Exp $";
    private Connection _conn = null;
    private Statement _stmt = null;
    private CellAdapter _cell = null;

    /**
     * Class constructor opens connection to the database and create a statement for queries
     */
    public ReplicaDbV1(CellAdapter cell, String url, String jdbcClass,
                       String user, String password, String pwdfile )
        throws Exception {
//      throws IllegalArgumentException {

        _cell = cell;

        if ((url == null )  ||  (jdbcClass == null) ||
            (user == null)  ||  (password == null && pwdfile == null) ) {
          throw new
              IllegalArgumentException("Not enough arguments to Init SQL database");
        }

        if (pwdfile != null && pwdfile.length() > 0) {
            Pgpass pgpass = new Pgpass(pwdfile);
            password = pgpass.getPgpass(url, user);
        }

        try {
            Class.forName(jdbcClass);

            // Connect to database
            say("Connecting to Database URL = " + url);
            _conn = DriverManager.getConnection(url, user, password);
            _stmt = _conn.createStatement();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can not connect to the DB");
            throw ( ex );
//             System.exit(1);
        }
    }

    /**
     * Class constructor opens connection to the database and create a statement for queries
     */
    public ReplicaDbV1(CellAdapter cell, String url, String jdbcClass,
                       String user, String password) throws Exception
    {
        this(cell, url, jdbcClass, user, password, (String)null);
    }

    /*
     * Report SQL exception ex for the sql statement sql in method m.
     */
    private void reportSQLException( String m, SQLException ex, String sql ) {
        int    iErr   = ex.getErrorCode();
        String sState = ex.getSQLState();

        esay("SQL exception in method "+m+": '"+ex+ "', errCode="+iErr+" SQLState="+sState+
             " SQLStatement=["+sql+"]");
    }

    private void ignoredSQLException( String m, SQLException ex, String sql ) {
        int    iErr   = ex.getErrorCode();
        String sState = ex.getSQLState();

        say("Ignore SQL exception in method "+m+": '"+ex+ "', errCode="+iErr+" SQLState="+sState+
             " SQLStatement=["+sql+"]");
    }


    private void reportSQLException( SQLException ex ) {
        esay("Database access error");
        ex.printStackTrace();
    }

    /**
     * Add record (poolname, pnfsid) to the table 'replicas'
     */
    public synchronized void addPool( PnfsId pnfsId , String poolName ) {
        String sql = "insert into replicas values ('"+poolName+"','"+pnfsId.toString()+"',now())";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
          String exMsg   = ex.getMessage();
          //say("DEBUG: exMsg=["+exMsg+"]");
          //exMsg=[ERROR:  Cannot insert a duplicate key into unique index replica]

          // This error string is system dependent or even version dependent:
          if (   exMsg.startsWith("ERROR:  Cannot insert a duplicate key into unique index replica")
                 || exMsg.startsWith("ERROR: duplicate key violates unique constraint")
                 ) {
            String s = exMsg.substring(5);
            say("WARNING"+ s +"; caused by duplicate message, ignore for now. pnfsid="
                   +pnfsId.toString() +" pool=" + poolName  );
            ignoredSQLException( "addPool()", (SQLException)ex, sql );

          } else {
            ex.printStackTrace();
            esay("Database access error");
          }
        }
    }

    /**
     * Remove record (poolname, pnfsid) from the table 'replicas'
     */
    public void removePool( PnfsId pnfsId , String poolName ) {
        String sql = "delete from replicas where pool = '"+poolName+"' and pnfsId = '"+pnfsId.toString()+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch ( SQLException ex ) {
          esay("WARNING: Database access error, can not delete pnfsId='"+pnfsId.toString()+"' "
               +"at pool = '"+poolName+"' from replicas DB table");
          reportSQLException( "removePool()", ex, sql );
        }
    }

    /**
     * Get the number of pools for given pnfsid
     * depreciated - will not work with newer postgres release
     */
    public int countPools( PnfsId pnfsId ) {
        String sql = "SELECT pool FROM ONLY replicas WHERE pnfsId = '"+ pnfsId.toString() + "'";
        try {
            ResultSet rset = _stmt.executeQuery(sql);
            // getFetchSize() is depreciated and will not work with newer postgres release
            return rset.getFetchSize();
        }
        catch (SQLException ex) {
            // ex.printStackTrace();
            esay("Database access error");
            reportSQLException( "countPools()", ex, sql );
            return -1;
        }
    }

    /**
     * Remove all the records with given pnfsid from the table
     */
    public void clearPools( PnfsId pnfsId ) {
        String sql = "delete from replicas where pnfsId = '"+pnfsId.toString()+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Database access error");
        }
    }

    /**
     * Abstract class for DB access
     */
    private abstract class DbIterator implements Iterator {

        protected ResultSet _rset = null;
        protected Statement _stmt = null;

        public boolean hasNext() {
            try {
                return _rset.next();
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Can't step to the next element of the result set");
            }
            return false;
        }

        public Object next() {
            try {
                return _rset.getObject(1);
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Can't get the next element of the result set");
            }
            return null;
        }

        public void remove() {
            throw new UnsupportedOperationException("No remove");
        }
    }

    /**
     * Private class for PNFSIDs access
     */
    private class PnfsIdIterator extends DbIterator {

        public PnfsIdIterator( ) {
            String sql = "SELECT DISTINCT pnfsId FROM ONLY replicas";
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }

        public PnfsIdIterator( String poolName ) {
            String sql = "SELECT pnfsId FROM ONLY replicas WHERE  pool = '"+poolName+"'";
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }

        public PnfsIdIterator( long timestamp ) {
            String sql = "SELECT pnfsId FROM action WHERE timestamp < '"+timestamp+"'";
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Returns all PNFSIDs from the DB
     */
    public Iterator pnfsIds() {
        return new PnfsIdIterator();
    }

    /**
     * Returns all PNFSIDs for the given pool from the DB
     */
    public Iterator pnfsIds(String poolName) {
        return new PnfsIdIterator(poolName);
    }

    /**
     * Private class for PNFSIDs access
     */
    private class PoolsIterator extends DbIterator {

        /**
         * Returns all pools from the DB
         */
        public PoolsIterator( ) {
            String sql = "SELECT * FROM pools";
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }

        /**
         * Returns all pools for given pnfsid from the DB
         */
        public PoolsIterator( PnfsId pnfsId ) {
            String sql = "SELECT pool FROM ONLY replicas WHERE pnfsId = '"+pnfsId.toString()+"'";
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Returns all pools from DB
     */
    public Iterator getPools( ) {
        return new PoolsIterator( );
    }

    /**
     * Returns all pools for a given pnfsid
     */
    public Iterator getPools( PnfsId pnfsId ) {
        return new PoolsIterator(pnfsId);
    }

    private class PoolsWritableIterator extends DbIterator {
        /**
         * Returns Writable pools from the DB
         */
        public PoolsWritableIterator( ) {
            String query = "select * from pools WHERE status='online'";

            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( query );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Returns all writable pools from DB
     */
    public Iterator getPoolsWritable( ) {
        return new PoolsWritableIterator( );
    }

    private class PoolsReadableIterator extends DbIterator {
        /**
         * Returns Readable pools from the DB
         */
        public PoolsReadableIterator( ) {
            String query = "select * from pools WHERE "
                           +"(  status='"+ONLINE+"' "
                           +"OR status='"+DRAINOFF+"' "
                           +"OR status='"+OFFLINE_PREPARE+"')";

            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( query );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Returns all Readable pools from DB
     */
    public Iterator getPoolsReadable( ) {
        return new PoolsReadableIterator( );
    }

    /**
     * Clears the tables
     */
    public void clearAll() {
        try {
//             _conn.setAutoCommit(false);
            _stmt.executeUpdate("BEGIN");
            _stmt.executeUpdate("DELETE FROM replicas");
            _stmt.executeUpdate("DELETE FROM pools");
            _stmt.executeUpdate("COMMIT");
//             _conn.commit();
//             _conn.setAutoCommit(true);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't clear the tables");
        }
    }

    /**
     * Clears pools and replicas tables for the pool 'pool' in argument
     *
     * @throws Exception if can not remove pool from DB
     */
    public void clearPool( String poolName ) {
        try {
            _stmt.executeUpdate("BEGIN");
            _stmt.executeUpdate("DELETE FROM replicas WHERE pool='"+poolName+"'");
            _stmt.executeUpdate("DELETE FROM pools    WHERE pool='"+poolName+"'");
            _stmt.executeUpdate("COMMIT");
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't remove pool '"+poolName+"' from the DB");
        }
    }

    /**
     * Private class to get the PNFSIDs with number of replicas > maximum
     */
    private class getRedundantIterator extends DbIterator {

        public getRedundantIterator(int maxcnt) {
            // Workaround postgres 8.x feature :
            // Do selection from 'replicas' and 'action', then drop pnfsid present in 'action'
            //                was : FROM ONLY replicas, pools ...

            String sql = "SELECT * FROM (SELECT pnfsid, sum(CASE WHEN pools.status='"+ONLINE+"' THEN 1 ELSE 0 END) "+
                         "FROM      replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp "+
                         "WHERE sum > "+maxcnt+" AND pnfsid NOT IN (SELECT pnfsid FROM action) ORDER BY sum DESC"
                         ;
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }

        public Object next() {
            try {
                return new Object[] {_rset.getObject(1), _rset.getObject(2)};
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Can't get the next element of the result set");
            }
            return null;
        }
    }

    /**
     * Returns all pnfsids with counters > 4
     */
    public Iterator getRedundant(int maxcnt) {
        return new getRedundantIterator(maxcnt);
    }

    /**
     * Private class to get the PNFSIDs with number of replicas < minimum
     */
    private class getDeficientIterator extends DbIterator {

        public getDeficientIterator(int mincnt) {

            // Workaround postgres 8.x feature :
            // Do selection from 'replicas' and 'action', then drop pnfsid present in 'action'
            //                was : FROM ONLY replicas, pools ...
            String sql = "SELECT * FROM (SELECT pnfsid,"+
                         "sum(CASE WHEN pools.status='"+ONLINE
                         +        "' OR pools.status='"+OFFLINE
                         +        "' OR pools.status='"+OFFLINE_PREPARE
                         +      "' THEN 1 ELSE 0 END) "+
                         "FROM      replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp "+
                         "WHERE sum > 0 and sum < "+mincnt+" AND pnfsid NOT IN (SELECT pnfsid FROM action) ORDER BY sum ASC"
                         ;
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }

        public Object next() {
            try {
                return new Object[] {_rset.getObject(1), _rset.getObject(2)};
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Can't get the next element of the result set");
            }
            return null;
        }
    }

    /**
     * Returns all pnfsids with counters = 1
     */
    public Iterator getDeficient(int mincnt) {
        return new getDeficientIterator(mincnt);
    }

    /**
     * Private class to get the PNFSIDs with number of replicas = 0
     */
    private class getMissingIterator extends DbIterator {

        public getMissingIterator( ) {
            String sql = "SELECT pnfsid FROM (SELECT pnfsid, "+
                         "sum(CASE "+
                         "WHEN pools.status='"+ONLINE
                         +"' OR pools.status='"+OFFLINE
                         +"' OR pools.status='"+OFFLINE_PREPARE
                         +"' THEN 1 "+
                         "WHEN pools.status='reduce' THEN -1 ELSE 0 "+
                         "END) FROM replicas, pools WHERE replicas.pool=pools.pool GROUP BY pnfsid) AS tmp WHERE sum=0"
                         ;
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Returns all pnfsids with counters = 0
     */
    public Iterator getMissing( ) {
        return new getMissingIterator( );
    }

// Removed
//     public void addPoolStatus(String poolName, String poolStatus) {
//         try {
//             _stmt.executeUpdate("insert into pools values ('"+poolName+"','"+poolStatus+"')");
//         }
//         catch (Exception ex) {
//             ex.printStackTrace();
//             System.out.println("Can't add pool '"+poolName+"'");
//         }
//     }

    public void removePoolStatus(String poolName) {
        String sql = "delete from pools where pool='"+poolName+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't remove pool '"+poolName+"' from the DB");
        }
    }

    /**
     * Set the value of pool status.
     * @param poolName  Pool name.
     * @param poolStatus  Value to assign to pool status.
     */
    public void setPoolStatus(String poolName, String poolStatus) {
        String sql_i = "insert into pools values ('"+poolName+"','"+poolStatus+"',now())";
        String sql_u = "update pools set status='"+poolStatus+"', datestamp=now() where pool='"+poolName+"'";

        try {
            _stmt.executeUpdate( sql_i );
        }
        catch (Exception ex) {
            // say("setPoolStatus() INFO: Can't add pool '"+poolName+"'"
            //    +" to 'pools' table in DB, will try to update");
            try {
                _stmt.executeUpdate( sql_u );
            }
            catch (Exception ex2) {
                ex2.printStackTrace();
                esay("setPoolStatus() ERROR: Can't add/update pool '"+poolName+"'"
                     +" status in 'pools' table in DB");
            }
        }
    }

    /**
     * Get the value of pool status.
     * @return value of pool status.
     */
    public String getPoolStatus(String poolName) {
        String sql = "SELECT status FROM pools WHERE pool='"+ poolName + "'";
        try {
            ResultSet rset = _stmt.executeQuery(sql);
            rset.next();
            return rset.getString(1);
        }
        catch (SQLException ex) {
            reportSQLException( "getPoolStatus()", ex, sql );

            esay("DB: Can't get status for pool '"+poolName+"' from pools table, return 'UNKNOWN'");
            return "UNKNOWN";
        }
    }

    /**
     * Add transaction into DB
     */
    public void addTransaction(PnfsId pnfsId, long timestamp, int count) {
        String poolName = null;
        if      (count > 0)
            poolName = "replicate";
        else if (count < 0)
            poolName = "reduce";
        else
            poolName = "exclude";

        String sql = "INSERT INTO action VALUES ('"+poolName+"','"+pnfsId.toString()+"',now(),"+timestamp+")";

        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't add transaction to the DB");
        }
    }

    /**
     * Remove transaction from DB
     */
    public void removeTransaction(PnfsId pnfsId) {
        String sql = "DELETE FROM action WHERE pnfsId = '"+pnfsId.toString()+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't remove transaction from the DB");
        }
    }

    /**
     * Clear transactions from DB
     */
    public void clearTransactions() {
        String sql1 = "DELETE FROM action WHERE pool='replicate'";
        String sql2 = "DELETE FROM action WHERE pool='reduce'";

        try {
            _stmt.executeUpdate( sql1 );
            _stmt.executeUpdate( sql2 );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't clear transactions from the DB");
        }
    }

    /**
     * Return the timestamp for a given PNFSID
     */
    public long getTimestamp(PnfsId pnfsId) {
        String sql ="SELECT timestamp FROM action WHERE pnfsId = '"+ pnfsId.toString() + "'";
        try {
            ResultSet rset = _stmt.executeQuery( sql );
            rset.next();
            return rset.getLong(1);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't get data from the DB");
            return -1;
        }
    }

    /**
     * Return the list of PNFSIDs which are older than 'timestamp'
     */
    public Iterator pnfsIds(long timestamp) {
        return new PnfsIdIterator(timestamp);
    }

    public void removePool( String poolName ) {
        String sql = "DELETE FROM replicas WHERE pool = '"+poolName+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Can't remove pool from the DB");
        }
    }

    /**
     * Private class to get the PNFSIDs which are in the drainoff pools only
     */
    private class getDrainingIterator extends DbIterator {

        public getDrainingIterator( ) {
            String sql = "SELECT rd.pnfsid "+
                         "FROM ONLY replicas rd, pools pd "+
                         "WHERE rd.pool = pd.pool AND pd.status = '"+DRAINOFF+"' "+
                         "GROUP BY rd.pnfsid "+
                         "EXCEPT "+
                         "SELECT r.pnfsid "+
                         "FROM ("+
                         "       SELECT rr.pnfsid FROM ONLY replicas rr, pools pp "+
                         "       WHERE rr.pool = pp.pool  AND pp.status = '"+DRAINOFF+"' "+
                         "       GROUP BY rr.pnfsid"+
                         "     ) r, "+
                         "     ONLY replicas r1, "+
                         "     pools p1 "+
                         "WHERE r.pnfsid  = r1.pnfsid"+
                         " AND  p1.pool   = r1.pool"+
                         " AND  ( p1.status = '"+ONLINE+"' "+
                         "     OR r.pnfsid IN (SELECT pnfsid FROM action) ) "+
                         "GROUP BY r.pnfsid"
                         ;
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Get the list of PNFSIDs which are in the drainoff pools only
     */
    public Iterator getInDrainoffOnly( ) {
        return new getDrainingIterator( );
    }

    /**
     * Private class to get the PNFSIDs which are in the offline pools only
     */
    private class getOfflineIterator extends DbIterator {

        public getOfflineIterator( ) {
            String sql = "SELECT ro.pnfsid "+
                         "FROM ONLY replicas ro, pools po "+
                         "WHERE ro.pool = po.pool AND po.status = '"+OFFLINE_PREPARE+"' "+
                         "GROUP BY ro.pnfsid "+
                         "EXCEPT "+
                         "SELECT r.pnfsid "+
                         "FROM ("+
                         "       SELECT rr.pnfsid FROM ONLY replicas rr, pools pp "+
                         "       WHERE rr.pool = pp.pool  AND pp.status = '"+OFFLINE_PREPARE+"' "+
                         "       GROUP BY rr.pnfsid"+
                         "     ) r, "+
                         "     ONLY replicas r1, "+
                         "     pools p1 "+
                         "WHERE r.pnfsid = r1.pnfsid"+
                         " AND  p1.pool  = r1.pool"+
                         " AND  ( p1.status = '"+ONLINE+"' "+
                         "     OR r.pnfsid IN (SELECT pnfsid FROM action) ) "+
                         "GROUP BY r.pnfsid"
                         ;
            try {
                _stmt = _conn.createStatement();
                _rset = _stmt.executeQuery( sql );
            }
            catch (Exception ex) {
                ex.printStackTrace();
                esay("Database access error");
            }
        }
    }

    /**
     * Get the list of PNFSIDs which are in the offline pools only
     */
    public Iterator getInOfflineOnly( ) {
        return new getOfflineIterator( );
    }

    /**
     *
     */
    public void setHeartBeat(String name, String desc) {
        String sql_i = "insert into heartbeat values ('"+name+"','"+desc+"',now())";
        String sql_u = "update heartbeat set description='"+desc+"', datestamp=now() where process='"+name+"'";

        try {
            _stmt.executeUpdate( sql_i );
        }
        catch (Exception ex) {
            try {
                _stmt.executeUpdate( sql_u );
            }
            catch (Exception ex2) {
                ex2.printStackTrace();
                esay("setHeartBeat() ERROR: Can't add/update process '"+name+"' status in 'heartbeat' table in DB");
            }
        }
    }

    /**
     *
     */
    public void removeHeartBeat(String name) {
        String sql = "delete from heartbeat where process = '"+name+"'";
        try {
            _stmt.executeUpdate( sql );
        }
        catch (Exception ex) {
            ex.printStackTrace();
            esay("Database access error");
        }
    }

    //////////////////////////////// End Of Interface ///////////////////////////////////////


    private void say(String s) {
        if (_cell == null) {
            System.err.println(s);
        } else {
            _cell.say(s);
        }
    }

    private void esay(String s) {
        if (_cell == null) {
            System.err.println(s);
        } else {
            _cell.esay(s);
        }
    }

    public static void printClassName(Object obj) {
        System.out.println("The class of " + obj +" is " + obj.getClass().getName());
    }

    public static void main( String [] args )throws Exception
    {
        System.out.println("Test ReplicaDbV1, cvsId="+ _cvsId );

        ReplicaDbV1 db = new ReplicaDbV1(
            null,
            "jdbc:postgresql://localhost:5432/replicas",
            "org.postgresql.Driver", "enstore", "NoPassword", "/root/.pgpass") ;

        System.out.println("List pnfsId's in all pools");
        for ( Iterator i = db.pnfsIds() ; i.hasNext() ; ) {
            System.out.println( i.next().toString());
        }

        for ( Iterator p = db.getPools( ) ; p.hasNext() ; ) {
            String pool = p.next().toString();
            System.out.println("Pool : "+pool);
            for (Iterator j = db.pnfsIds(pool) ; j.hasNext() ; ) {
                System.out.println( j.next().toString());
            }
        }
//         for ( Iterator j = db.pnfsIds("pool1") ; j.hasNext() ; ) {
//             System.out.println( j.next().toString());
//         }
//         for ( Iterator j = db.pnfsIds("pool2") ; j.hasNext() ; ) {
//             System.out.println( j.next().toString());
//         }
//         for ( Iterator j = db.pnfsIds("pool3") ; j.hasNext() ; ) {
//             System.out.println( j.next().toString());
//         }
//         for ( Iterator j = db.pnfsIds("pool4") ; j.hasNext() ; ) {
//             System.out.println( j.next().toString());
//         }

        PnfsId pnfsId = new PnfsId("1234") ;

        System.out.println("WARNING: db.countPools(...) is depreciated and will not work with newer postgres release ");

        db.addPool( pnfsId , "pool1" ) ;
        db.addPool( pnfsId , "pool2" ) ;
        db.addPool( pnfsId , "pool3" ) ;
        System.out.println("pools: " + db.countPools(pnfsId));

        db.removePool( pnfsId , "pool1" ) ;
        System.out.println("pools: " + db.countPools(pnfsId));

        db.addPool( pnfsId , "pool1" ) ;
        System.out.println("pools: " + db.countPools(pnfsId));

        db.clearPools(pnfsId);
        System.out.println("pools: " + db.countPools(pnfsId));

// This has to generate an error: Cannot insert a duplicate key into unique index replicas_index
//          for (int i = 0; i < 5; i++) {
//              db.addPool( new PnfsId("2000"+i) , "pool1" ) ;
//          }

//          for (int i = 0; i < 5; i++) {
//              db.addPool( new PnfsId("2000"+i) , "pool3" ) ;
//          }

        db.addPool( pnfsId , "pool2" ) ;
//         db.addPool( pnfsId , "pool2" ) ;  // This has to generate an error: Cannot insert a duplicate key into unique index replicas_index
        System.out.println("pools: " + db.countPools(pnfsId));
        for ( Iterator i = db.getPools( pnfsId ) ; i.hasNext() ; ) {
            System.out.println(" pnfsid : "+pnfsId+ ", pool : "+i.next());
        }

        db.removePool( pnfsId , "pool2" ) ;
        System.out.println("pools: " + db.countPools(pnfsId));
        for ( Iterator i = db.getPools( pnfsId ) ; i.hasNext() ; ) {
            System.out.println(" pnfsid : "+pnfsId+ ", pool : "+i.next());
        }

        for ( Iterator i = db.getMissing( ) ; i.hasNext() ; ) {
            System.out.println(" Missing pnfsid : "+i.next());
        }


        for ( Iterator i = db.getDeficient(2) ; i.hasNext() ; ) {
            Object[] r = (Object[])(i.next());
//             System.out.println(" Length : "+r.length);
            System.out.println(" Deficient pnfsid : "+r[0]+": "+r[1]);
//             printClassName(i.next());
//             printClassName(new int[] {1,2,3});
        }

        for ( Iterator i = db.getRedundant(3) ; i.hasNext() ; ) {
            Object[] r = (Object[])(i.next());
            System.out.println(" Redundant pnfsid : "+r[0]+": "+r[1]);
        }

        System.out.println("pool1: Status : '"+db.getPoolStatus("pool1")+"'");
        System.out.println("pool11111111111: Status : '"+db.getPoolStatus("pool11111111111")+"'");

        db.setPoolStatus("pool9","offline");
        System.out.println("pool9: Status : '"+db.getPoolStatus("pool9")+"'");

        db.clearPool("pool9");

        System.exit(0);
    }
}
