// $Id: SpaceManager.java,v 1.28.4.2 2007-07-16 22:03:16 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.28.4.1  2006/09/19 17:03:41  podstvkv
// Enable empty pgPass argument
//
// Revision 1.28  2006/01/09 18:13:49  timur
// fixed a space reservation update bug that was leaving space reserved in pools
//
// Revision 1.27  2005/12/14 10:11:03  tigran
// setting cell type to class name
//
// Revision 1.26  2005/11/22 10:59:31  patrick
// Versioning enabled.
//
// Revision 1.25  2005/11/01 23:28:13  timur
// make space reservation more reliable and cleanup ufter failures
//
// Revision 1.24  2005/10/03 19:01:08  timur
// space relase of unexistant reservation should not cause failure
//
// Revision 1.23  2005/09/30 21:47:39  timur
// more space reservation - pnfs communication improvements
//
// Revision 1.22  2005/09/27 21:46:51  timur
// do not leave pnfs entry behind after space reservation is created
//
// Revision 1.21  2005/08/19 23:45:26  timur
// added Pgpass for postgress md5 password suport
//
// Revision 1.20  2005/06/27 17:02:36  timur
// removed usage of PnfsHandler in SpaceManager, to prevent doing blocking calls in message handling code
//
// Revision 1.19  2005/06/14 22:20:06  timur
// minor space manager related changes
//
// Revision 1.18  2005/04/22 16:05:52  timur
// fixed some message timeout issues, nomalized paths in space manager
//
// Revision 1.17  2005/03/23 18:17:19  timur
// SpaceReservation working for the copy case as well
//
// Revision 1.16  2005/03/10 23:12:07  timur
// Fisrt working version of space reservation module
//
// Revision 1.15  2005/03/09 23:22:57  timur
// more space reservation code
//
// Revision 1.14  2005/03/01 23:12:09  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.13  2005/02/20 17:30:04  timur
// Added message to be sent from the door
//
// Revision 1.12  2005/02/18 23:18:59  timur
// more changes to SpaceManager, reservation and release work
//
// Revision 1.11  2005/02/17 04:49:28  timur
// more fixes
//
// Revision 1.10  2005/02/17 04:41:07  timur
// use executeUpdate for updates
//
// Revision 1.9  2005/02/17 04:29:59  timur
// fixed a few sqk statements
//
// Revision 1.8  2005/02/17 04:18:35  timur
// fixed reserve space command
//
// Revision 1.7  2005/02/16 22:23:36  timur
// added accounting of reserve and release
//
// Revision 1.6  2005/02/16 06:13:05  timur
// added SpaceManagerReleaseSpaceMessage message, working on space manger code
//
// Revision 1.5  2005/02/15 19:52:16  timur
// some more SpaceReservation code
//
// Revision 1.4  2005/02/04 23:05:44  timur
// working on space manager
//
// Revision 1.3  2005/02/02 22:57:21  timur
// working on space manager
//
// Revision 1.2  2005/01/31 22:52:04  timur
// started working on space reservation
//
// Revision 1.1  2004/10/20 21:32:30  timur
// adding classes for space management
//
// Revision 1.13  2004/06/22 01:32:09  timur
// Fixed an initialization bug
//

/*
 * PinManager.java
 *
 * Created on April 28, 2004, 12:54 PM
 */

package diskCacheV111.services;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellVersion;
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.StorageInfo;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;
import java.util.List;
import java.util.ArrayList;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import diskCacheV111.util.FsPath;
import diskCacheV111.vehicles.spaceManager.SpaceManagerMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerReserveSpaceMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerReleaseSpaceMessage;
import diskCacheV111.vehicles.spaceManager.
        SpaceManagerGetInfoAndLockReservationByPathMessage;
import diskCacheV111.vehicles.spaceManager.
        SpaceManagerGetInfoAndLockReservationMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUtilizedSpaceMessage;
import diskCacheV111.vehicles.spaceManager.SpaceManagerUnlockSpaceMessage;
import diskCacheV111.vehicles.PoolStatusChangedMessage;
import java.util.Date;
import diskCacheV111.util.Pgpass;
//import diskCacheV111.vehicles.PnfsG

/**
 *   <pre> Space Manager dCache service provides ability 
 *    \to reserve space in the pools
 *   before performing the transfer of the file
 *
 *
 * @author  timur
 */
public class SpaceManager  extends CellAdapter implements Runnable {
    
    private long spaceReservationCleanupPeriodInSeconds = 60*60; // one hour in seconds
    private String jdbcUrl;
    private String jdbcClass;
    private String user;
    private String pass;
    private String pwdfile;
    
    private String SpaceManagerNextReservationTokenTableName = 
        "spacemanagernextid";
    private String SpaceManagerPoolSpaceTableName = 
        "SpaceManagerPoolSpace".toLowerCase();
    private String SpaceManagerSpaceReservationTableName = 
        "SpaceManagerPoolReservation".toLowerCase();
    
    private Timer reservationTimer = new Timer(true);
    private Map reservationTimerTasks = new Hashtable();
    protected static final String stringType=" VARCHAR(32672) ";
    protected static final String longType=" BIGINT ";
    protected static final String intType=" INTEGER ";
    protected static final String dateTimeType= " TIMESTAMP ";
    protected static final String booleanType= " INT ";
    private JdbcConnectionPool connection_pool;
    
    private String CreateSpaceManagerNextReservationToken =
    "CREATE TABLE "+SpaceManagerNextReservationTokenTableName+
    " ( NextToken "+ longType + " )";
    
    private  String CreateSpaceManagerPoolSpaceTable = 
    "CREATE TABLE "+ SpaceManagerPoolSpaceTableName+" ( "+
    " PoolName "+stringType+" PRIMARY KEY "+
    ", ReservedSpaceSize "+longType+" "+
    ", LockedSpaceSize "+longType+" "+
    ");";
    private static final int POOL_SPACE_POOL_NAME_STRING_FIELD=1;
    private static final int POOL_SPACE_RESERVED_SIZE_LONG_FIELD=2;
    private static final int POOL_SPACE_LOCKED_SIZE_LONG_FIELD=3;
    
    private  String CreateSpaceManagerSpaceReservationTable = 
    "CREATE TABLE "+ SpaceManagerSpaceReservationTableName+" ( "+
    " SpaceToken "+longType+" PRIMARY KEY "+
    ", ReservedSpaceSize "+longType+" "+
    ", LockedSpaceSize "+longType+" "+
    ", CreationTime "+longType+" "+
    ", Lifetime "+longType+" "+
    ", PoolName "+stringType+" "+
    ", PnfsPath "+stringType+" "+ // this is used to determine the correct pool(s) for this space
                                  // file does not have to be the actual file or one of the file(s) 
                                  // that will be written
    ", CreatedPnfsEntry "+booleanType+" "+
    ", Utilized "+booleanType+" "+
    ", CONSTRAINT fk_"+SpaceManagerPoolSpaceTableName+
    "PoolName FOREIGN KEY (PoolName) REFERENCES "+
    SpaceManagerPoolSpaceTableName +" (PoolName) "+
    " ON DELETE RESTRICT"+
    ");";
        
    private static final int SPACE_RESERVATION_SPACE_TOKEN_LONG_FIELD=1;
    private static final int SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD=2;
    private static final int SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD=3;
    private static final int SPACE_RESERVATION_CREATION_TIME_LONG_FIELD=4;
    private static final int SPACE_RESERVATION_LIFETIME_LONG_FIELD=5;
    private static final int SPACE_RESERVATION_POOL_NAME_STRING_FIELD=6;
    private static final int SPACE_RESERVATION_PNFS_PATH_STRING_FIELD=7;
    private static final int SPACE_RESERVATION_CREATED_PNFS_ENTRY_BOOL_FIELD=8;
    private static final int SPACE_RESERVATION_UTILIZED_BOOL_FIELD=9;
    
    private String pnfsManager = "PnfsManager";
    private String poolManager = "PoolManager";
    
    
    /** Creates a new instance of SpaceManager */
    public SpaceManager(String name, String argString) throws Exception {
        
        super( name ,SpaceManager.class.getName(), argString , false );
        Args _args = getArgs();
        jdbcUrl = _args.getOpt("jdbcUrl");
        jdbcClass = _args.getOpt("jdbcDriver");
        user = _args.getOpt("dbUser");
        pass = _args.getOpt("dbPass");
        pwdfile = _args.getOpt("pgPass");
        if (pwdfile != null && pwdfile.trim().length() > 0) {
            Pgpass pgpass = new Pgpass(pwdfile);      //VP
            pass = pgpass.getPgpass(jdbcUrl, user);   //VP
        }
        
        connection_pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
        
        if(_args.getOpt("poolManager") != null) {
            poolManager = _args.getOpt("poolManager");
        }
        
        if(_args.getOpt("pnfsManager") != null) {
            poolManager = _args.getOpt("pnfsManager");
        }
        
        if(_args.getOpt("cleanupPeriod") != null) {
            spaceReservationCleanupPeriodInSeconds = Long.parseLong(_args.getOpt("cleanupPeriod"));
        }
        
        try {
            dbinit();
            //initializeDatabasePinRequests();
            
        }
        catch (Throwable t) {
            esay("error starting PinManager");
            esay(t);
            start();
            kill();
        }
        restoreTimers();
        start();
        
        getNucleus().newThread(this,"SpaceManagerCleanup").start();
        
    }
    
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.28.4.2 $" ); }

    public void getInfo(java.io.PrintWriter printWriter) {
        printWriter.println("SpaceManager "+getCellName());
        printWriter.println("JdbcUrl="+jdbcUrl);
        printWriter.println("jdbcClass="+jdbcClass);
        printWriter.println("databse user="+user);
        printWriter.println("reservation space cleanup period in secs : "+spaceReservationCleanupPeriodInSeconds);
    }
    
    public String hh_set_cleanup_period = " # set perioud between cleaup runs in seconds";
    public String ac_set_cleanup_period_$_1(Args args) throws Exception {
        long periodInSeconds = Long.parseLong(args.argv(0));
        if(periodInSeconds <= 0 ) {
            return "cleanup perioud should be a positive long integer";
        }
        spaceReservationCleanupPeriodInSeconds = periodInSeconds;
        return "set";
    }
    
    public String hh_reserve = "<path> <bytes> <seconds> <host> # reserve <bytes> of space \n"+
                               "                                # for <seconds> seconds \n"+
                               "                                # from <host> ";
    public String ac_reserve_$_4( Args args ) throws Exception {
        String path = args.argv(0);
        path = normalizePath(path);
        long bytes = Long.parseLong( args.argv(1) ) ;
        long lifetime = Long.parseLong( args.argv(2) ) ;
        lifetime *=1000;
        String host = args.argv(3);
        Reserver reserver = new Reserver(path,bytes,lifetime,host);
        reserver.waitCompleteon();
        //Pinner pinner = new Pinner(pnfsId,null,lifetime,null,null);
        //pinner.waitCompleteon();
        //if(pinner.isSuccess()) {
        //    return " Pin Successfull, pin request id="+pinner.requestId;
        //}
        
        String s;
        if(reserver.isSuccess()) {
            s = "reservation is successfull, spaceToken="+reserver.getReservationToken();
        }
        else {
            s = " reservation failed "+reserver.getErrorObject();
        }
        return s;
    }
    
    public String hh_release = "[-force] <spaceReservationToken> [ <bytes> ] # release all nonlocked (or all if force)\n"+
    " or <bytes> of space in a space reservation identified by <spaceReservatgionToken>" ;
    public String ac_release_$_1_2(Args args) throws Exception {
        long spaceToken = Long.parseLong( args.argv(0));
        Releaser releaser;
        boolean force = args.getOpt("force") != null;
        if(args.argc() > 1) {
         long bytes = Long.parseLong( args.argv(1) ) ;
         if(bytes <=0) {
             return "cannot relese nonpositive number of bytes : "+bytes;
         }
            releaser = new Releaser(spaceToken,bytes,force);
        }
        else
        {
            releaser = new Releaser(spaceToken,force);
        }
        releaser.waitCompleteon();
        return "release is successfull :"+ releaser.isSuccess();
      
    }
    public String hh_ls = " # list space reservations\n"+
    " or <bytes> of space in a space reservation identified by <spaceReservatgionToken>" ;
    public String ac_ls_$_0(Args args) throws Exception {
        boolean isLongFormat = args.getOpt("l") != null;
        return listSpaceReservations(isLongFormat);
    }
    
    public String hh_cleanup = " # run cleanup";
    public String ac_cleanup_$_0(Args args) throws Exception {
          runCleaup();
          return "Done";
    }
    
    public static class SpaceReservationInfo
    {
        public String spaceToken;
        public long reservedSpace;
        public long expirationTime;
        public long continuousSize;
        

    }
    
    public SpaceReservationInfo reserveSpace(long continousSize, long lifetime,String pnfsPaths,String host) {
        pnfsPaths = normalizePath(pnfsPaths);
        Reserver reserver = new Reserver(pnfsPaths,continousSize,lifetime,host);
        reserver.waitCompleteon();
        return null;
    }
    
    public void reserveSpace(SpaceManagerReserveSpaceMessage reserveRequest,CellMessage cellMessage) {
        new Reserver(reserveRequest, cellMessage);
        
    }
    
     public void releaseSpace(SpaceManagerReleaseSpaceMessage releaseRequest,CellMessage cellMessage) {
        new Releaser(releaseRequest, cellMessage);
        
    }
    
    
    private void dbinit() throws SQLException {
        try {
            
            // Add driver to JDBC
            Class.forName(jdbcClass);
            
            //connect
            Connection _con = connection_pool.getConnection();
            _con.setAutoCommit(true);
            //get database info
            DatabaseMetaData md = _con.getMetaData();
            String tables[] = new String[] {
                                            SpaceManagerNextReservationTokenTableName,
                                            SpaceManagerPoolSpaceTableName,
                                            SpaceManagerSpaceReservationTableName};
            String createTables[] =
            new String[] {
                          CreateSpaceManagerNextReservationToken,
                          CreateSpaceManagerPoolSpaceTable,
                          CreateSpaceManagerSpaceReservationTable};
            for (int i =0; i<tables.length;++i) {
                ResultSet tableRs = md.getTables(null, null, tables[i] , null );
                
                
                if(!tableRs.next()) {
                    try {
                        Statement s = _con.createStatement();
                        say("dbinit trying "+createTables[i]);
                        int result = s.executeUpdate(createTables[i]);
                    }
                    catch(SQLException sqle) {
                        
                        esay("SQL Exception (relation could already exist)");
                        esay(sqle);
                        
                    }
                }
            }
                          
           // need to initialize the NextToken value
            String select = "SELECT * FROM "+SpaceManagerNextReservationTokenTableName;
            Statement s = _con.createStatement();
            ResultSet set = s.executeQuery(select);
            if(!set.next())
            {
                    String insert = "INSERT INTO "+ SpaceManagerNextReservationTokenTableName+ 
                        " VALUES ("+Long.MIN_VALUE+")";
                    //say("dbInit trying "+insert);
                    s = _con.createStatement();
                    say("dbInit trying "+insert);
                    int result = s.executeUpdate(insert);
            }
            else
            {
                say("dbInit set.next() returned nonnull");
            }
            
            // to support our transactions
            _con.setAutoCommit(false);
            connection_pool.returnConnection(_con);
        }
        catch (SQLException sqe) {
            esay(sqe);
            throw sqe;
        }
        catch (Exception ex) {
            esay(ex);
            throw new SQLException(ex.toString());
        }
        
        
    }
    
    public void restoreTimers() {

        long thisMoment = System.currentTimeMillis();
        say("restoreTimers");
        // set of token/expiration pairs
           java.util.Set s = new java.util.HashSet();
        
        // we should obtain the token before requesting the connection
        // to avoid exhaust of the connection pool and a deadlock
       Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservations =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName;
            say("executing statement: "+selectSpaceReservations);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet selectSet = sqlStatement.executeQuery(
           selectSpaceReservations);
           while(selectSet.next()) {
               long spaceToken = selectSet.getLong(SPACE_RESERVATION_SPACE_TOKEN_LONG_FIELD);
               long creationTime = selectSet.getLong(SPACE_RESERVATION_CREATION_TIME_LONG_FIELD);
               long lifetime = selectSet.getLong(SPACE_RESERVATION_LIFETIME_LONG_FIELD);
               long expirationTime = creationTime+lifetime;
               long remainingLifetime = expirationTime - thisMoment;
               if(remainingLifetime > 0) {
                   startTimer(spaceToken,remainingLifetime) ;
                } else {
                   s.add(new Long(spaceToken));
                }
           }
           selectSet.close();
           //just in case
           _con.commit();
            
        }
        catch(SQLException sqle) {
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqlee) {
                 //we are in the error state anyway, just log it       
                esay(sqlee);
            }
            connection_pool.returnFailedConnection(_con);
            _con = null;
            esay(sqle);
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        // we want to do the expiraion part outside of the part where 
        // we obtained a connection from the pool, because otherwise 
        // we are risking creating a racing condition, since 
        // releaser will use database as well
        
        for(java.util.Iterator i = s.iterator(); i.hasNext(); )
        {
            
            long spaceToken=  ((Long)i.next()).longValue();
            new Releaser(spaceToken,true);
        }
        
    }
    
    public synchronized  long getNextToken() throws SQLException  {
        Connection _con = null;
        try {
        
            _con = connection_pool.getConnection();
           long nextLong;
           String select_for_update = "SELECT * from "+
            SpaceManagerNextReservationTokenTableName+" FOR UPDATE ";
           try
           {
            Statement s = _con.createStatement();
            say("dbInit trying "+select_for_update);
            ResultSet set = s.executeQuery(select_for_update);
            if(!set.next()) {
                throw new SQLException("table "+
                SpaceManagerNextReservationTokenTableName+" is empty!!!");
            }
            nextLong = set.getLong(1);
            String increase_nextlong = "UPDATE "+SpaceManagerNextReservationTokenTableName+
            " SET NextToken=NextToken+1";
            s = _con.createStatement();
            int i = s.executeUpdate(increase_nextlong);
             
            _con.commit();
           }
           catch(SQLException e)
           {
               e.printStackTrace();
               _con.rollback();
               throw e;
           }
           connection_pool.returnConnection(_con);
           _con = null;
           return nextLong;
       }
       finally
       {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
           
       }
    }

    
    
    public long registerNewReservation(
        String pool,
        String pnfsPath,
        long reservationSize,
        long totalPoolReservationSize,
        long lifetime) throws SQLException{
            pnfsPath = normalizePath(pnfsPath);
            say("regestering new space reservation of "+reservationSize+
            " bytes for "+lifetime+" milliseconds at pool \""+pool+
            "\" for file \""+pnfsPath);
        // we should obtain the token before requesting the connection
        // to avoid exhaust of the connection pool and a deadlock
        long nextReservationToken = getNextToken();
        long creationTime = System.currentTimeMillis();
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);

           if (!updateSet.next()) {
               // we did not find anything, try to insert a new blank record
               updateSet.close();
               try 
               {

                   String updatePoolSpace = "INSERT INTO "+SpaceManagerPoolSpaceTableName +
                   " VALUES ( '"+pool+"', 0, 0)";
                    sqlStatement =
                        _con.createStatement();
                    say("executing statement: "+updatePoolSpace);
                   sqlStatement.executeUpdate(updatePoolSpace);
               }
               catch (SQLException e)
               {

                   esay(e);
                   esay("ignoring, it might happen that someone else has created a record");
                   
               }
               sqlStatement =
                    _con.createStatement();
                    say("executing statement: "+selectPoolSpaceForUpdate);
               updateSet = sqlStatement.executeQuery(
               selectPoolSpaceForUpdate);
               if(!updateSet.next()) {
                   updateSet.close();
                   throw new SQLException(" can not insert or udate the pool record for pool:"+pool);
               }
           }
            String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET ReservedSpaceSize = ReservedSpaceSize +"+reservationSize+
                   " WHERE  PoolName = '"+pool+"'";
            say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
            //insert new values into space reservation table 
            String insertSpaceReservation = "INSERT  INTO "+
            SpaceManagerSpaceReservationTableName +" VALUES ( "+
            nextReservationToken+" , "+reservationSize+",0,"+
            creationTime+", "+lifetime+" , '"+pool+"','"+
            pnfsPath+"', 0,0)";
             say("executing statement: "+insertSpaceReservation);
            sqlStatement =  _con.createStatement();
            sqlStatement.executeUpdate(insertSpaceReservation);
            say("COMMIT TRANSACTION");
            _con.commit();
            
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
        
        startTimer(nextReservationToken,lifetime);
        return nextReservationToken;
     }
    
    /** lock all available space and return poolname and Long locked size
     */
    
    public Object[] lockSpace(long spaceToken, boolean force)
    throws SQLException{
        say("lockSpace for reservation "+spaceToken);
        
        // we should obtain the token before requesting the connection
        // to avoid exhaust of the connection pool and a deadlock
        String pool;
        long available;
        long reserved;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservationForUpdate =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  SpaceToken = '"+spaceToken+"' FOR UPDATE ;";
            say("executing statement: "+selectSpaceReservationForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectSpaceReservationForUpdate);

           if (!updateSet.next()) {
               updateSet.close();
               throw new SQLException(" no space reservation for spaceToken = "+
               spaceToken+" found ");
           }
           reserved = updateSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
           long locked = updateSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
           available = reserved - locked;
           if(  available < 0) {
               updateSet.close();
               throw new SQLException("inconsistent state: available space is less then zero: "+available);
           }
           pool = updateSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
           
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            sqlStatement =
                _con.createStatement();
           ResultSet updateSet1 = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);
           if(!updateSet1.next() ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, record for pool "+
                    pool+" not found");
           }
           long poolReserved = updateSet1.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           long poolLocked = updateSet1.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
           long poolAvailable = poolReserved - poolLocked;
           if( poolAvailable < available ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, space available in pool "+
                    pool+" = "+poolAvailable + " is less than space available in space "+
                    " reservation for the same pool ="+available);
           }
           // lock space in pool    
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+available+
                   " WHERE  PoolName = '"+pool+"'";
           say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
           // and in a reservation 
           String updateSpaceReservationTable = "UPDATE "+SpaceManagerSpaceReservationTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+available+
                   " WHERE  SpaceToken = "+spaceToken ;
           say("executing statement: "+updateSpaceReservationTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updateSpaceReservationTable);
            say("COMMIT TRANSACTION");
            _con.commit();
            
        }
        catch(SQLException sqle) {
            say("update failed with exception, could be release of utilized space"+sqle);
            //esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
        long returnSize = force?reserved:available;
        return new Object[]{pool,new Long(returnSize)};
     }

    /**
     * locks space in database and returns the pool, in which the space is locked
     */
    public String lockSpace(long spaceToken, long lockSize)
    throws SQLException{
        say("lockSpace for reservation "+spaceToken+" locked space size "+lockSize);
        if(lockSize <= 0) {
            throw new SQLException("unlock size is <= 0");
        }
        // we should obtain the token before requesting the connection
        // to avoid exhaust of the connection pool and a deadlock
        long nextReservationToken = getNextToken();
        long creationTime = System.currentTimeMillis();
        String pool;
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservationForUpdate =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  SpaceToken = '"+spaceToken+"' FOR UPDATE ;";
            say("executing statement: "+selectSpaceReservationForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectSpaceReservationForUpdate);

           if (!updateSet.next()) {
               updateSet.close();
               throw new SQLException(" no space reservation for spaceToken = "+
               spaceToken+" found ");
           }
           long reserved = updateSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
           long locked = updateSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
           long available = reserved - locked;
           if(  available < lockSize) {
               updateSet.close();
               throw new SQLException(" not enough available space to lock");
           }
           pool = updateSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
           
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            sqlStatement =
                _con.createStatement();
           ResultSet updateSet1 = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);
           if(!updateSet1.next() ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, record for pool "+
                    pool+" not found");
           }
           long poolReserved = updateSet1.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           long poolLocked = updateSet1.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
           long poolAvailable = poolReserved - poolLocked;
           if( poolAvailable < available ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, space available in pool "+
                    pool+" = "+poolAvailable + " is less than space available in space "+
                    " reservation for the same pool ="+available);
           }
           // lock space in pool    
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+lockSize+
                   " WHERE  PoolName = '"+pool+"'";
            say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
           // and in a reservation 
           String updateSpaceReservationTable = "UPDATE "+SpaceManagerSpaceReservationTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+lockSize+
                   " WHERE  SpaceToken = "+spaceToken ;
            say("executing statement: "+updateSpaceReservationTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updateSpaceReservationTable);
            say("COMMIT TRANSACTION");
            _con.commit();
            
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        
        
        return pool;
     }
    
   public void unlockSpace(SpaceManagerUnlockSpaceMessage unlockRequest,
                           CellMessage cellMessage ) {
       say("unlockSpace() ");
        try {
            long spaceToken = unlockRequest.getSpaceToken();
            long unlock = unlockRequest.getSize();
            unlockSpace(spaceToken,unlock);
            returnMessage(unlockRequest,cellMessage);
        }
        catch (SQLException sqle) {
             returnFailedResponse(sqle,
               unlockRequest,cellMessage);

        }
   }
   
   public void unlockSpace( long spaceToken,
        long lockedBytesToUnlock) throws SQLException {
        say("unlockSpace(token="+spaceToken+",ulocksize="+lockedBytesToUnlock);
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservationForUpdate =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  SpaceToken = '"+spaceToken+"' FOR UPDATE ;";
            say("executing statement: "+selectSpaceReservationForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectSpaceReservationForUpdate);

           if (!updateSet.next()) {
               updateSet.close();
               throw new SQLException(" no space reservation for spaceToken = "+
               spaceToken+" found ");
           }
           long reserved = updateSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
           long locked = updateSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
           long available = reserved - locked;
           if(available <0)
           {
               throw new SQLException(" inconsistent state, reserved ="+reserved+
               " which is less then locked = "+locked);
           }
           if( locked < lockedBytesToUnlock) {
               updateSet.close();
               throw new SQLException(" not enough locked space to unlock");
           }
           if(lockedBytesToUnlock == 0) {
               esay("Warning: attempt to unlock 0 bytes");
               updateSet.close();
               _con.commit();
               return;
           }
           String pool = updateSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
           
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            sqlStatement =
                _con.createStatement();
           ResultSet updateSet1 = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);
           if(!updateSet1.next() ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, record for pool "+
                    pool+" not found");
           }
           long poolReserved = updateSet1.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           long poolLocked = updateSet1.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
           long poolAvailable = poolReserved - poolLocked;
           if( poolLocked < locked ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, space locked in pool "+
                    pool+" = "+poolLocked + " is less than space available in space "+
                    " reservation for the same pool ="+locked);
           }
           // unlock space in pool    
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET LockedSpaceSize = LockedSpaceSize -"+lockedBytesToUnlock+
                   " WHERE  PoolName = '"+pool+"'";
            say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
           // and in a reservation 
           String updateSpaceReservationTable= "UPDATE "+SpaceManagerSpaceReservationTableName +
                       " SET LockedSpaceSize = LockedSpaceSize -"+lockedBytesToUnlock+
                       " WHERE  SpaceToken = "+spaceToken ;
                say("executing statement: "+updateSpaceReservationTable);
                sqlStatement =
                    _con.createStatement();
                sqlStatement.executeUpdate(updateSpaceReservationTable);
            say("COMMIT TRANSACTION");
            _con.commit();
            
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
   }
   
   public void unlockAndDecreaseSpace(long spaceToken,
        long lockedBytesToUnlock,
        long decreaseSpaceAmount,
        Long remainingReservedSpaceInPool,
        boolean utilized) 
        throws SQLException
   {
       String pnfsPath = null;
      // boolean createdPnfsEntry=false;
       boolean deletedReservation = false;
       say("unlockAndDecreaseSpace("+spaceToken+","+
        lockedBytesToUnlock+","+
        decreaseSpaceAmount+","+
        remainingReservedSpaceInPool+");");
       if(decreaseSpaceAmount > lockedBytesToUnlock) {
           throw new SQLException("trying to decrease space by the amount larger then unlocking amount ");
       }
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservationForUpdate =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  SpaceToken = "+spaceToken+" FOR UPDATE ;";
            say("executing statement: "+selectSpaceReservationForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectSpaceReservationForUpdate);
           say("SELECT RETURNED");
           if (!updateSet.next()) {
               updateSet.close();
               throw new SQLException(" no space reservation for spaceToken = "+
               spaceToken+" found ");
           }
           long reserved = updateSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
           long locked = updateSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
           pnfsPath = updateSet.getString(SPACE_RESERVATION_PNFS_PATH_STRING_FIELD);
           //createdPnfsEntry =updateSet.getBoolean(SPACE_RESERVATION_CREATED_PNFS_ENTRY_BOOL_FIELD);
           utilized |= updateSet.getBoolean(SPACE_RESERVATION_UTILIZED_BOOL_FIELD);
           long available = reserved - locked;
           if(available <0)
           {
               throw new SQLException(" inconsistent state, reserved ="+reserved+
               " which is less then locked = "+locked);
           }
           if( locked < lockedBytesToUnlock) {
               updateSet.close();
               throw new SQLException(" not enough locked space to unlock");
           }
           if(lockedBytesToUnlock == 0) {
               esay("Warning: attempt to unlock 0 bytes");
               // continue anyway, 
               // we might need to delete the record
              // updateSet.close();
              // _con.commit();
              // return;
           }
           String pool = updateSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
           
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ";
            say("executing statement: "+selectPoolSpaceForUpdate);
            sqlStatement =
                _con.createStatement();
           ResultSet updateSet1 = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);
           if(!updateSet1.next() ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, record for pool "+
                    pool+" not found");
           }
           long poolReserved = updateSet1.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           long poolLocked = updateSet1.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
           long poolAvailable = poolReserved - poolLocked;
           if( poolLocked < locked ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, space locked in pool "+
                    pool+" = "+poolLocked + " is less than space available in space "+
                    " reservation for the same pool ="+locked);
           }
           if(remainingReservedSpaceInPool != null) {
               if( (poolReserved - decreaseSpaceAmount) != remainingReservedSpaceInPool.longValue()) {
                   esay( "warning, after this update the registered reserved space in pool will be:"+
                   (poolReserved - decreaseSpaceAmount)+ 
                   "\n \t\t and the actual reserved size reported by pool is "+
                   remainingReservedSpaceInPool);
               }
           }
           // unlock space in pool    
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET LockedSpaceSize = LockedSpaceSize -"+lockedBytesToUnlock+
                   ", ReservedSpaceSize = ReservedSpaceSize-"+decreaseSpaceAmount+
                   " WHERE  PoolName = '"+pool+"'";
            say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
           // and in a reservation 
           String updateSpaceReservationTable;
           if( (reserved-decreaseSpaceAmount)  <= 0) {
               updateSpaceReservationTable = "DELETE FROM "+SpaceManagerSpaceReservationTableName +
                   " WHERE  SpaceToken = "+spaceToken ;
               deletedReservation = true;
              
           } else {
               updateSpaceReservationTable = "UPDATE "+SpaceManagerSpaceReservationTableName +
                   " SET LockedSpaceSize = LockedSpaceSize -"+lockedBytesToUnlock+
                   ", ReservedSpaceSize = ReservedSpaceSize-"+decreaseSpaceAmount+
                   ", utilized="+(utilized?1:0)+
                   " WHERE  SpaceToken = "+spaceToken ;
           }
            say("executing statement: "+updateSpaceReservationTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updateSpaceReservationTable);
            say("COMMIT TRANSACTION");
            _con.commit();
            
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            _con.rollback();
            connection_pool.returnFailedConnection(_con);
            _con = null;
            throw sqle;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
   }
    
    private void getAndLockSpace(
        SpaceManagerGetInfoAndLockReservationMessage getInfoAndLockRequest,
        CellMessage cellMessage) {
        boolean getByPath = getInfoAndLockRequest instanceof 
        SpaceManagerGetInfoAndLockReservationByPathMessage;
        String path = null;
        long spaceToken = 0;
        if(getByPath) {
             path = getInfoAndLockRequest.getPath();
                say("getAndLockSpace for path "+path);
        }
        else {
            spaceToken = getInfoAndLockRequest.getSpaceToken();
            say("getAndLockSpace for spaceToken="+spaceToken);
        }
        
        // we should obtain the token before requesting the connection
        // to avoid exhaust of the connection pool and a deadlock
       Connection _con = null;
        try {
            _con = connection_pool.getConnection();
            
            String selectSpaceReservationForUpdate;
            if(getByPath) {
            selectSpaceReservationForUpdate=
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  PnfsPAth = '"+path+"' FOR UPDATE ;";
            }
            else
            {
              selectSpaceReservationForUpdate=
                "SELECT * FROM "+ SpaceManagerSpaceReservationTableName +
                " WHERE  SpaceToken = "+spaceToken+" FOR UPDATE ;";
            }
            say("executing statement: "+selectSpaceReservationForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectSpaceReservationForUpdate);

           if (!updateSet.next()) {
               updateSet.close();
               throw new SQLException(" no space reservation for PnfsPath = "+
               path+" found ");
           }
           spaceToken = updateSet.getLong(SPACE_RESERVATION_SPACE_TOKEN_LONG_FIELD);
           long reserved = updateSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
           long locked = updateSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
           long available = reserved - locked;
           long creationTime = updateSet.getLong(SPACE_RESERVATION_CREATION_TIME_LONG_FIELD);
           long lifetime = updateSet.getLong(SPACE_RESERVATION_LIFETIME_LONG_FIELD);
           if(  available <= 0) {
               updateSet.close();
               throw new SQLException(" not enough available space to lock");
           }
           String pool = updateSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
           path = updateSet.getString(SPACE_RESERVATION_PNFS_PATH_STRING_FIELD);
           
            String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            sqlStatement =
                _con.createStatement();
           ResultSet updateSet1 = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);
           if(!updateSet1.next() ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, record for pool "+
                    pool+" not found");
           }
           long poolReserved = updateSet1.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           long poolLocked = updateSet1.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
           long poolAvailable = poolReserved - poolLocked;
           if( poolAvailable < available ) {
               updateSet.close();
               updateSet1.close();
               throw new SQLException(" inconsistent state, space available in pool "+
                    pool+" = "+poolAvailable + " is less than space available in space "+
                    " reservation for the same pool ="+available);
           }
           // lock space in pool    
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+available+
                   " WHERE  PoolName = '"+pool+"'";
           say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            
           // and in a reservation 
           String updateSpaceReservationTable = "UPDATE "+SpaceManagerSpaceReservationTableName +
                   " SET LockedSpaceSize = LockedSpaceSize +"+available+
                   " WHERE  SpaceToken = "+spaceToken ;
           say("executing statement: "+updateSpaceReservationTable);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(updateSpaceReservationTable);
            say("COMMIT TRANSACTION");
            _con.commit();
            getInfoAndLockRequest.setPath(path);
            getInfoAndLockRequest.setAvailableLockedSize(available);
            getInfoAndLockRequest.setSpaceToken(spaceToken);
            getInfoAndLockRequest.setCreation_time(creationTime);
            getInfoAndLockRequest.setCreation_time(creationTime+lifetime);
            getInfoAndLockRequest.setPool(pool);
            returnMessage(getInfoAndLockRequest,cellMessage);
            
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqle1) {
                 //we are in the error state anyway, just log it       
                esay(sqle1);
            }
               returnFailedResponse(sqle,
               getInfoAndLockRequest,cellMessage);

            connection_pool.returnFailedConnection(_con);
            _con = null;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public String listSpaceReservations(boolean isLongFormat) {
       say("listSpaceReservations isLongFormat="+isLongFormat);
       // we should obtain the token before requesting the connection
       // to avoid exhaust of the connection pool and a deadlock
       Connection _con = null;
       StringBuffer sb;
        try {
            _con = connection_pool.getConnection();
            String selectSpaceReservations =
            "SELECT * FROM "+ SpaceManagerSpaceReservationTableName;
            say("executing statement: "+selectSpaceReservations);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet selectSet = sqlStatement.executeQuery(
           selectSpaceReservations);
           
           sb = new StringBuffer("\tSpace Reservations:\n");
           while(selectSet.next()) {
               long spaceToken = selectSet.getLong(SPACE_RESERVATION_SPACE_TOKEN_LONG_FIELD);
               sb.append("spaceToken ").append(spaceToken);
               long reserved = selectSet.getLong(SPACE_RESERVATION_RESERVED_SIZE_LONG_FIELD);
               sb.append(", reserved ").append(reserved);
               long locked = selectSet.getLong(SPACE_RESERVATION_LOCKED_SIZE_LONG_FIELD);
               sb.append(", locked ").append(locked);
               long available = reserved - locked;
               sb.append(", available ").append(available);
               long creationTime = selectSet.getLong(SPACE_RESERVATION_CREATION_TIME_LONG_FIELD);
               long lifetime = selectSet.getLong(SPACE_RESERVATION_LIFETIME_LONG_FIELD);
               Date creationDate = new Date(creationTime);
               Date expirationDate = new Date(creationTime+lifetime);
               sb.append(", created ").append(creationDate);
               sb.append(", expires ").append(expirationDate);
               String pool = selectSet.getString(SPACE_RESERVATION_POOL_NAME_STRING_FIELD);
               sb.append(", pool ").append(pool);
               String pnfsPath =selectSet.getString(SPACE_RESERVATION_PNFS_PATH_STRING_FIELD);
               sb.append(", pnfsPath ").append(pnfsPath);
              // boolean createdPnfsEntry = selectSet.getBoolean(SPACE_RESERVATION_CREATED_PNFS_ENTRY_BOOL_FIELD);
               //sb.append(",createdPnfsEntry ").append(createdPnfsEntry).append('\n');
               boolean spaceUtilized = selectSet.getBoolean(SPACE_RESERVATION_UTILIZED_BOOL_FIELD);
               sb.append(",spaceUtilized ").append(spaceUtilized).append('\n');
               
           }
           selectSet.close();
           sb.append("\tSpace Reservations By Pool:\n");
           
            String selectPoolSpace =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName;
            say("executing statement: "+selectPoolSpace);
            sqlStatement =
                _con.createStatement();
           selectSet = sqlStatement.executeQuery(
           selectPoolSpace);
           while(selectSet.next() ) {
               String pool =     selectSet.getString(1);
               sb.append(pool);
               long poolReserved = selectSet.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
               sb.append(" : reserved ").append(poolReserved);
               long poolLocked = selectSet.getLong(POOL_SPACE_LOCKED_SIZE_LONG_FIELD);
               sb.append(", locked ").append(poolLocked);
               long poolAvailable = poolReserved - poolLocked;
               sb.append(", available (reserved-locked) ").append(poolAvailable).append('\n');
          }
           selectSet.close();
           //just in case
           _con.commit();
            
        }
        catch(SQLException sqle) {
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqle1) {
                 //we are in the error state anyway, just log it       
                esay(sqle1);
            }
            connection_pool.returnFailedConnection(_con);
            _con = null;
            return sqle.toString();
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        return sb.toString();
        
    }
    
    private void utilizedSpace(
        SpaceManagerUtilizedSpaceMessage utilizedRequest,
        CellMessage cellMessage) {
        try {    
            unlockAndDecreaseSpace(utilizedRequest.getSpaceToken(),
                utilizedRequest.getSize(), 
                utilizedRequest.getSize(),
                null,  //we do not know how much remains in pool at this moment
                true
            );
            returnMessage(utilizedRequest, cellMessage);
         
        } catch (Exception e) {
            returnFailedResponse(e,utilizedRequest,cellMessage);
        }
    }
    
    private void poolStatusChanged(PoolStatusChangedMessage poolStatusChanged){
        String pool = poolStatusChanged.getPoolName();
        switch(poolStatusChanged.getPoolState()) {
            case PoolStatusChangedMessage.UP: {
                poolCameUp(pool);
                break;
            }
            case PoolStatusChangedMessage.DOWN: {
                poolWentDown(pool);
                break;
            }
            case PoolStatusChangedMessage.RESTART: {
                poolCameUp(pool);
                break;
            }
            default:{
                esay("poolStatusChanged, pool="+pool+" unknown pool state!!!");
            }
        }
    }
    private void poolCameUp(String pool){
       say("poolCameUp("+pool+");");
       long poolReserved;
       Connection _con = null;
       try {
            _con = connection_pool.getConnection();
             String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"';";
            say("executing statement: "+selectPoolSpaceForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);

           if (!updateSet.next()) {
               // we did not find anything,nothing needs to be changed
               updateSet.close();
               _con.commit();
               return;
           }
           //this is what we think should be in the pool
           poolReserved = updateSet.getLong(POOL_SPACE_RESERVED_SIZE_LONG_FIELD);
           _con.commit();

        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqle1) {
                 //we are in the error state anyway, just log it       
                esay(sqle1);
            }

            connection_pool.returnFailedConnection(_con);
            _con = null;
            return;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
        // now we make sure that pool's reservation is exactly what we think it should be
        UpdateSpaceCompanion.updateSpaceAmount(pool, poolReserved, this);
    }
    
    // we gonna just delete all the space reservations assosiated with the pool
    // so that the attempt to use the space reservations fail
    private void poolWentDown(String pool){
       say("poolWentDown("+pool+");");
        Connection _con = null;
        try {
            _con = connection_pool.getConnection();
             String selectPoolSpaceForUpdate =
            "SELECT * FROM "+ SpaceManagerPoolSpaceTableName +
            " WHERE  PoolName = '"+pool+"' FOR UPDATE ;";
            say("executing statement: "+selectPoolSpaceForUpdate);
            Statement sqlStatement =
                _con.createStatement();
           ResultSet updateSet = sqlStatement.executeQuery(
           selectPoolSpaceForUpdate);

           if (!updateSet.next()) {
               // we did not find anything,nothing needs to be changed
               updateSet.close();
               _con.commit();
               return;
           }
 
            String deleteResrvationsForDownedPool =
            "DELETE FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  PoolName = '"+pool+"' ;";
            say("executing statement: "+deleteResrvationsForDownedPool);
            sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(deleteResrvationsForDownedPool);
            say("COMMIT TRANSACTION");
            _con.commit();
            
            //set pool space to 0
           String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
           " SET ReservedSpaceSize = 0 "+
           " WHERE  PoolName = '"+pool+"'";
            say("executing statement: "+updatePoolSpaceTable);
            sqlStatement =
            _con.createStatement();
            sqlStatement.executeUpdate(updatePoolSpaceTable);
            _con.commit();
        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqle1) {
                 //we are in the error state anyway, just log it       
                esay(sqle1);
            }
            connection_pool.returnFailedConnection(_con);
            _con = null;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
    
    public void messageArrived( CellMessage cellMessage ) {
        Object o = cellMessage.getMessageObject();
        say("Message  arrived: "+o +" from "+cellMessage.getSourcePath());
        if(o instanceof SpaceManagerReserveSpaceMessage) {
            SpaceManagerReserveSpaceMessage reserveRequest = 
                (SpaceManagerReserveSpaceMessage) o;
            reserveSpace(reserveRequest,cellMessage);
        }
        else if(o instanceof SpaceManagerReleaseSpaceMessage) {
            SpaceManagerReleaseSpaceMessage releaseRequest = 
                (SpaceManagerReleaseSpaceMessage) o;
            releaseSpace(releaseRequest,cellMessage);
            
        }
        else if(o instanceof SpaceManagerGetInfoAndLockReservationMessage) {
            SpaceManagerGetInfoAndLockReservationMessage getInfoAndLockRequest = 
                (SpaceManagerGetInfoAndLockReservationMessage) o;
            getAndLockSpace(getInfoAndLockRequest,cellMessage);
            
        }
        else if(o instanceof SpaceManagerUtilizedSpaceMessage) {
            SpaceManagerUtilizedSpaceMessage utilizedRequest = 
                (SpaceManagerUtilizedSpaceMessage) o;
            utilizedSpace(utilizedRequest,cellMessage);
            
        }
        else if(o instanceof SpaceManagerUnlockSpaceMessage) {
            SpaceManagerUnlockSpaceMessage unlockRequest = 
                (SpaceManagerUnlockSpaceMessage) o;
            unlockSpace(unlockRequest,cellMessage);
        }
        else if(o instanceof PoolStatusChangedMessage) {
            PoolStatusChangedMessage poolStatusChanged = 
                (PoolStatusChangedMessage) o;
            poolStatusChanged(poolStatusChanged);
        }
        else {
            esay("unknown Space Manager message :"+o);
            super.messageArrived(cellMessage);
            return;
        }
        
    }
    
    public void exceptionArrived(ExceptionEvent ee) {
        say("Exception Arrived: "+ee);
        super.exceptionArrived(ee);
    }
    
    
    public void startTimer(final long spaceToken, long lifetime) {
        TimerTask tt = new TimerTask() {
            public void run() {
                // force = true means release ev
                 new Releaser(spaceToken,true);
            }
        };
        
        reservationTimerTasks.put(new Long(spaceToken), tt);
        
        // this is very approximate
        // but we do not need hard real time
        reservationTimer.schedule(tt,lifetime);
    }
    
    public void stopTimer(final long spaceToken) {
        Object o = reservationTimerTasks.remove(new Long(spaceToken));
        if(o == null) {
            esay("stopTimer(): timer not found for spaceToken="+spaceToken);
            return;
        }
        
        TimerTask tt = (TimerTask)o;
        tt.cancel();
    }
    
    public void returnFailedResponse(Object reason ,
    SpaceManagerMessage spaceManagerMessage, CellMessage cellMessage) {
        if( reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }
        
        try {
            spaceManagerMessage.setReply();
            spaceManagerMessage.setFailed(1, reason);
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e) {
            esay("can not send a failed responce");
            esay(e);
        }
    }
    
    public void returnMessage(SpaceManagerMessage spaceManagerMessage, CellMessage cellMessage) {
        try {
            spaceManagerMessage.setReply();
            cellMessage.revertDirection();
            sendMessage(cellMessage);
        }
        catch(Exception e) {
            esay("can not send a responce");
            esay(e);
        }
        
    }
    
    /** instance of this class is created for each pin request
     * its responsibility is to update  database, create timer
     * and communicate with other dCache cells in order to accomplish
     * pinning process
     */
    
    class Reserver implements ReserveSpaceCallbacks {
        protected volatile int state = INITIAL;
        // state constats
        protected static final int INITIAL=0;
        protected static final int FINAL_SUCCESS=11;
        protected static final int FINAL_FAILED=12;
        private int uid = -1;
        private int gid = -1;
        private String path;
        private long reservationSize;
        private long lifetime;
        private String host = "localhost";
        private StorageInfo storageInfo;
        private PnfsId pnfsId;
        private long reservationToken;
        
        private Object sync = new Object();
        private boolean completed = false;
        private Object errorObject = null;
        private SpaceManagerReserveSpaceMessage reserveRequest;
        CellMessage cellMessage;
        
        public Object getErrorObject() {
            return errorObject;
            
        }
        
        public void complete() {
            synchronized(sync) {
                completed = true;
                sync.notify();
            }
            
        }
        
        public boolean isSuccess() {
            return state == FINAL_SUCCESS;
        }
        
        public void waitCompleteon() {
            while(true) {
                synchronized(sync) {
                    if(completed) return;
                    try {
                        sync.wait();
                    }
                    catch(InterruptedException ie) {
                    }
                }
            }
        }
        
        
        public Reserver(String path,long reservationSize,long lifetime,String host)  {
                this.path = normalizePath(path);
                this.reservationSize = reservationSize;
                this.lifetime = lifetime;
                this.host = host;
                ReserveSpaceCompanion.reserveSpace(path,
                    this,reservationSize,host,SpaceManager.this);
       }
        
       public Reserver(SpaceManagerReserveSpaceMessage reserveRequest,
        CellMessage cellMessage){
              this.uid = reserveRequest.getUid();
              this.gid = reserveRequest.getGid();
              this.reserveRequest = reserveRequest;
              this.cellMessage = cellMessage;
              this.storageInfo = reserveRequest.getStorageInfo();
              this.pnfsId = reserveRequest.getPnfsId();
              this.path = normalizePath(reserveRequest.getPath());
              this.reservationSize = reserveRequest.getSize();
              this.host = reserveRequest.getHostToTransferIntoSpace();
              this.lifetime = reserveRequest.getLifetime();
              if(storageInfo != null  && pnfsId != null) {
                  ReserveSpaceCompanion.reserveSpace(path,
                    storageInfo,pnfsId,this,reservationSize,host,SpaceManager.this);
              } else if(uid != -1 && gid != -1)
              {
                  ReserveSpaceCompanion.reserveSpace(path,
                    uid,gid,this,reservationSize,host,SpaceManager.this);
                  
              } else {
                  ReserveSpaceCompanion.reserveSpace(path,
                    this,reservationSize,host,SpaceManager.this);
              }
       }
        
        private void error(Object errorObject) {
            state =  FINAL_FAILED;
            this.errorObject = errorObject;
            complete();
            if(reserveRequest != null) {
                returnFailedResponse(errorObject, reserveRequest, cellMessage);
               
            }
        }

       public void Error(String error) {
           error((Object) error);
        }
        
        public void Exception(Exception e) {
           error((Object) e);
        }
        
        public void ReserveSpaceFailed(String reason) {
           error((Object) reason);

        }
        
        public void SpaceReserved(String pool, long totalPoolReservationSize) {
            try
            {
                reservationToken = registerNewReservation(pool,
                    path,reservationSize,totalPoolReservationSize,lifetime);
            }
            catch (SQLException sqle) {
                esay(sqle);
                new Releaser(pool,reservationSize);
                error("reserved space  successfully, but failed to register with exception:"+sqle);
                return;
            }
            state =  FINAL_SUCCESS;
            complete();
            if(reserveRequest != null) {
                reserveRequest.setSpaceToken(reservationToken);
                returnMessage(reserveRequest, cellMessage);
               
            }
        }
        
        public void Timeout() {
           error((Object) "Timeout");
        }
        
        /** Getter for property reservationToken.
         * @return Value of property reservationToken.
         *
         */
        public long getReservationToken() {
            return reservationToken;
        }
        
        
    }
    
    class Releaser implements ReleaseSpaceCallbacks {
        protected volatile int state = INITIAL;
        // state constats
        protected static final int INITIAL=0;
        protected static final int FINAL_SUCCESS=11;
        protected static final int FINAL_FAILED=12;
        private SpaceManagerReleaseSpaceMessage releaseRequest;
        private CellMessage cellMessage;
        long spaceToken;
        private String poolname;
        private long bytes;
        private String host = "localhost";
        private boolean updateDB;
        
        private Object sync = new Object();
        private boolean completed = false;
        private Object errorObject = null;
        public boolean force; // if this flag is set, everything is released,
                              // even if the space is locked
        public Object getErrorObject() {
            return errorObject;
            
        }
        
        public void complete() {
            synchronized(sync) {
                completed = true;
                sync.notify();
            }
            
        }
        
        public boolean isSuccess() {
            return state == FINAL_SUCCESS;
        }
        
        public void waitCompleteon() {
            while(true) {
                synchronized(sync) {
                    if(completed) return;
                    try {
                        sync.wait();
                    }
                    catch(InterruptedException ie) {
                    }
                }
            }
        }
         
        // this is the dumm Releaser which does not update db, it should not be used
         public Releaser(String pool,long bytes)  {
                if (pool == null || bytes <= 0) {
                    throw new IllegalArgumentException("pool is null or size is nonpositive");
                }
                this.poolname = pool;
                this.bytes = bytes;
                ReleaseSpaceCompanion.releaseSpace(poolname,bytes,this,SpaceManager.this);
       }

         public Releaser(long spaceToken,long bytes)  {
             this(spaceToken,bytes,false);
         }
         
        public Releaser(long spaceToken,long bytes,boolean force)  {
                if ( bytes <= 0) {
                    throw new IllegalArgumentException("size is nonpositive");
                }
                say("releaser spaceToken="+spaceToken+" bytes="+bytes);
                this.spaceToken = spaceToken;
                this.bytes = bytes;
                this.force = force;
                updateDB=true;
                if(lockSpaceToRelease(spaceToken,bytes)) {
                    ReleaseSpaceCompanion.releaseSpace(poolname,bytes,this,SpaceManager.this);
                }
       }
        
        public Releaser(long spaceToken,boolean force)  {
                say("releaser spaceToken="+spaceToken);
                this.spaceToken = spaceToken;
                this.force = force;
                updateDB=true;
                if(lockSpaceToRelease(spaceToken)) {
                    ReleaseSpaceCompanion.releaseSpace(poolname,bytes,this,SpaceManager.this);
                }
       }
        
        public Releaser(SpaceManagerReleaseSpaceMessage releaseRequest,CellMessage cellMessage)  {
                this.releaseRequest = releaseRequest;
                this.cellMessage = cellMessage;
                this.spaceToken = releaseRequest.getSpaceToken();
                Long size = releaseRequest.getSize();
                updateDB=true;
                say("releaser spaceToken="+spaceToken+" size="+size);
                // if size is null this means that we want to release all space
                if(size == null) {
                    if(lockSpaceToRelease(spaceToken)) {
                        ReleaseSpaceCompanion.releaseSpace(poolname,bytes,this,SpaceManager.this);
                    }
                }else {
                    // if size is not null this means that we want to release space specified
                    // by the size variable
                     bytes = size.longValue();
                    if(lockSpaceToRelease(spaceToken,bytes)) {
                        ReleaseSpaceCompanion.releaseSpace(poolname,bytes,this,SpaceManager.this);
                    }
                }
        }
        
        private boolean lockSpaceToRelease(long spaceToken, long size) {
            if(size <=0) {
                esay("lockSpaceToRelease, size <0!");
            }
            try{
                poolname  = lockSpace(spaceToken,size);
                bytes = size;
               say("lockSpace returned poolname ="+poolname);
                return true;
            }
            catch(SQLException e) {
                esay("lockSpace failed");
                esay(e);
                error(e);
                return false;
            }
                
        }

        private boolean lockSpaceToRelease(long spaceToken) {
            try{
                Object[] oa = lockSpace(spaceToken,force);
                poolname = (String) oa[0];
                bytes =  ((Long)oa[1]).longValue();
               say("lockSpace returned poolname ="+poolname+" bytes="+bytes);
                return true;
            }
            catch(SQLException e) {
                say("lockSpace failed, could be release of the utilized space: "+e);
                if(releaseRequest != null) {
                       // we do not send error in this case since the nonexistent 
                       // space request is release by virtue of its absence
                      returnMessage(releaseRequest, cellMessage);
                }

                return false;
            }
                
        }
        
        private void error(Object errorObject) {
            say("release error:" +errorObject);
            state =  FINAL_FAILED;
            this.errorObject = errorObject;
            complete();
            if(releaseRequest != null) {
                returnFailedResponse(errorObject, releaseRequest, cellMessage);
               
            }
        }
        
        
        public void Error(String error) {
            if(updateDB) {
                try {
                    unlockSpace(spaceToken,bytes);
                } catch (SQLException sqle) {
                    esay(sqle);
                }
            }
            error(error);
        }
        
        public void Exception(Exception e) {
            if(updateDB) {
                try {
                    unlockSpace(spaceToken,bytes);
                } catch (SQLException sqle) {
                    esay(sqle);
                }
            }
            error(e);
        }
        
        public void ReleaseSpaceFailed(String reason) {

            if(updateDB) {
                try {
                    unlockSpace(spaceToken,bytes);
                } catch (SQLException sqle) {
                    esay(sqle);
                }
            }
            error(reason);

        }
        
        public void SpaceReleased(String pool, long remainingReservedSpaceInPool) {
            if(pool != this.poolname )
            {
                esay("release space in a wrong pool!!!!");
            }
            if(updateDB) {
                try {
                    unlockAndDecreaseSpace(spaceToken,bytes,bytes,
                        new Long(remainingReservedSpaceInPool),false);
                } catch (SQLException sqle)
                {
                    error("space released, but failed to update db:"+sqle);
                    return;
                }
            }
            state =  FINAL_SUCCESS;
            complete();
            
            if(releaseRequest != null) {
                returnMessage(releaseRequest, cellMessage);
               
            }
        }
        
        public void Timeout() {
            if(updateDB) {
                try {
                    unlockSpace(spaceToken,bytes);
                } catch (SQLException sqle) {
                    esay(sqle);
                }
            }
            error("timeout");
        }
        
    }
    
    public static final String normalizePath(String path) {
        if(path == null) return null;
        return new FsPath(path).toString();
    }
    
    
    public void run() {
        while(true) {
            try
            {
                Thread.sleep(spaceReservationCleanupPeriodInSeconds*1000L);
            }catch (InterruptedException ie){
                esay("Space Manager cleanup thread is interrupted");
                return;
            }
            try
            {
                runCleaup();
            }catch(SQLException sqle){
                esay(sqle);
            }
        }
    }
    
    public void runCleaup() throws SQLException {
        say("cleanup thread runs");
        Connection _con = null;
        long expiration_moment = System.currentTimeMillis() - 1000*60*10;
        try {
            _con = connection_pool.getConnection();

           //delete all expired reservations
           String deleteExpiredReservations =
           "DELETE FROM "+ SpaceManagerSpaceReservationTableName +
            " WHERE  creationtime+lifetime < "+expiration_moment;
            say("executing statement: "+deleteExpiredReservations);
            Statement sqlStatement =
                _con.createStatement();
            sqlStatement.executeUpdate(deleteExpiredReservations);
            say("commiting delete");
                _con.commit();
             String selectPools =
            "SELECT PoolName FROM "+ SpaceManagerPoolSpaceTableName;
            say("executing statement: "+selectPools);
             sqlStatement =
                _con.createStatement();
           ResultSet poolsSet = sqlStatement.executeQuery(
           selectPools);
           List pools = new ArrayList();
           while (poolsSet.next()) {
               String pool = poolsSet.getString(1);
                //get correct values for space sizes
                String countReservedLockedSpace = 
                "SELECT sum(ReservedSpaceSize) , sum(LockedSpaceSize) FROM "+
                SpaceManagerSpaceReservationTableName+
                " WHERE PoolName='"+pool+"'";
                say("executing statement: "+countReservedLockedSpace);
                sqlStatement =
                    _con.createStatement();
               ResultSet resultSet = sqlStatement.executeQuery(
               countReservedLockedSpace);
               long reservedspacesize = 0;
               long lockedspacesize = 0;
               if(resultSet.next()) {
                   reservedspacesize= resultSet.getLong(1);
                   lockedspacesize = resultSet.getLong(2);
               }

                 resultSet.close();

               String updatePoolSpaceTable = "UPDATE "+SpaceManagerPoolSpaceTableName +
               " SET ReservedSpaceSize = "+reservedspacesize+
               ", LockedSpaceSize = "+lockedspacesize+
               " WHERE  PoolName = '"+pool+"'";
                say("executing statement: "+updatePoolSpaceTable);
                sqlStatement =
                    _con.createStatement();
                sqlStatement.executeUpdate(updatePoolSpaceTable);
                say("commiting update");
                 _con.commit();

                 //actually update the space in the pool
                 UpdateSpaceCompanion.updateSpaceAmount(pool, reservedspacesize, this);
           };
           poolsSet.close();

        }
        catch(SQLException sqle) {
            esay("update failed with ");
            esay(sqle);
            say("ROLLBACK TRANSACTION");
            try{
            _con.rollback();
            }
            catch (SQLException sqle1) {
                 //we are in the error state anyway, just log it       
                esay(sqle1);
            }

            connection_pool.returnFailedConnection(_con);
            _con = null;
            return;
        }
        finally {
            if(_con != null) {
                connection_pool.returnConnection(_con);
            }
        }
    }
}
