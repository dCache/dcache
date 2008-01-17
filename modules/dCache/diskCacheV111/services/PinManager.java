// $Id: PinManager.java,v 1.42 2007-10-25 15:02:43 behrmann Exp $
// $Log: not supported by cvs2svn $
// Revision 1.41  2007/08/16 22:04:58  timur
// simplified srmRm, return SRM_INVALID_PATH if file does not exist
//
// Revision 1.38  2007/08/03 20:20:00  timur
// implementing some of the findbug bugs and recommendations, avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.37  2007/08/03 15:46:01  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes etc, per findbug recommendations
//
// Revision 1.36  2007/07/16 21:56:01  timur
// make sure the empty pgpass is ignored
//
// Revision 1.35  2007/05/24 13:51:13  tigran
// merge of 1.7.1 and the head
//
// Revision 1.34  2007/04/18 23:35:30  timur
// initial state of the pin should be INITIAL_STATE and not PINNED\!
//
// Revision 1.33  2007/02/10 04:48:13  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.32  2006/10/10 21:04:35  timur
// srmBringOnline related changes
//
// Revision 1.31  2006/05/12 22:44:54  litvinse
// *** empty log message ***
//
// Revision 1.30  2005/12/14 10:07:55  tigran
// setting cell type to class name
//
// Revision 1.29  2005/11/22 10:59:30  patrick
// Versioning enabled.
//
// Revision 1.28  2005/11/17 17:45:33  timur
// fixed two select statements rejected by Postgres8.1
//
// Revision 1.27  2005/08/26 21:59:33  timur
// one more bug removed
//
// Revision 1.26  2005/08/26 21:44:37  timur
// bugs removed
//
// Revision 1.25  2005/08/26 21:01:57  timur
// reorganized data structures and state transitions of PinManager
//
// Revision 1.24  2005/08/23 16:22:11  timur
// added new debug messages
//
// Revision 1.23  2005/08/22 17:22:08  timur
// better recovery from unexpected states of PinRequests
//
// Revision 1.22  2005/08/19 23:45:26  timur
// added Pgpass for postgress md5 password suport
//
// Revision 1.21  2005/08/16 16:35:25  timur
// added duration control
//
// Revision 1.20  2005/08/15 19:30:59  timur
// mostly working
//
// Revision 1.19  2005/08/15 18:19:43  timur
// new PinManager first working version, needs more testing
//
// Revision 1.18  2005/08/12 17:14:54  timur
// a first approximation of the new PinManager, not tested, but compiles, do not deploy in production
//
// Revision 1.17  2005/03/07 22:57:43  timur
// more work on space reservation
//
// Revision 1.16  2005/03/01 23:12:09  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.15  2004/11/09 08:04:46  tigran
// added SerialVersion ID
//
// Revision 1.14  2004/10/20 21:32:29  timur
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
import dmg.util.Args;
import dmg.cells.nucleus.ExceptionEvent;
import dmg.cells.nucleus.CellVersion;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.StorageInfo;
import java.sql.*;
import java.util.Timer;
import java.util.TimerTask;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetStorageInfoMessage;
import diskCacheV111.vehicles.PnfsFlagMessage;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolMgrSelectReadPoolMsg;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.vehicles.DCapProtocolInfo;
import java.util.Set;
import java.util.Collection;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import diskCacheV111.util.Pgpass;
//import diskCacheV111.vehicles.PnfsG

/**
 *   <pre>
 *   This cell performs "pinning and unpining service on behalf of other services
 *   cenralized pin management supports:
 *    pining/unpinning of the same resources by multiple requestors,
 *     synchronization of pinning and unpinning
 *     lifetimes for pins
 *
 * PINNING
 * 1) when pin for a file exists and another request arrives
 *  no action is taken, the database pinrequest record is created
 * 2) if pin does not exist new pinrequest record is created
 *       Pnfs flag is not set anymore
 *       the file is staged if nessesary and pinned in a read pool
 *       with PinManager, as an owner, and lifetime
 *
 *
 * UNPINNING
 *  1)if pin request expires / canseled and other pin requests
 *  for the same file exist, no action is taken, other then removal
 *  of the database  pin request record
 *  2) if last pin request is removed then the file is unpinned
 * which means sending of the "set stiky to false message" is send to all
 * locations,
 *the pnfs flag is removed
 * database  pin request record is removed
 *
 *
 *
 * @author  timur
 */
public class PinManager extends CellAdapter implements Runnable  {
    private long maxPinDuration=24*60*60*1000; // one day
    private String jdbcUrl;
    private String jdbcClass;
    private String user;
    private String pass;
    private String pwdfile;
    //keep the names  spelled in the lower case to make postgress driver to work
    // correctly

    private static final String PinRequestTableName = "pinrequestsv2";
    private static final String NextPinRequestIdTableName = "nextpinrequestid";
    private static final String OldPinRequestTableName = "pinrequestsv1";
    private static final String OldPinsTableName = "pins";

    // timer waiting for expirations of pin requests
    private Timer pinTimer = new Timer(true);

    // a map from request ids to pin timer tasks
    private Map<Long,TimerTask> pinTimerTasks = new Hashtable<Long,TimerTask>();

    // this is the most important data type in this class
    // for each pnfsId pinned there is one mapping here
    // a mapping to possibly many pin requests stored as
    // PinRequest structures
    private Map<PnfsId,Pin> pnfsIdToPins = new Hashtable<PnfsId,Pin>();

    // all database oprations will be done in the lazy
    // fassion in a low priority thread
    private Thread databaseUpdateThread;

    // all database oprations will be done in the lazy
    // fassion in a low priority thread
    private Thread updateWaitQueueThread;

    //this list will contain a list of sql database operations
    // we will have three types of operations
    // insertion of a new pin record
    // removal of the pin record
    // and update of the next pin request id

    private List<String> databaseStatementsQueue = new LinkedList<String>();
    // database types
    private static final String stringType=" VARCHAR ";
    private static final String longType=" numeric ";
    private static final String intType=" numeric ";
    private static final String dateTimeType= " TIMESTAMP ";
    private static final String booleanType= " BOOLEAN ";

    /** pool of jdbc connections */
    private JdbcConnectionPool pool;

    private static final int PinTable_PnfsId = 1;

    /**
     * we are going to use the currentTimeMillis
     * as the next PinRequestId
     * so that it can be used as an identifier for the request
     * and the creation time stamp
     * if the PinRequestId already exists, we will increment by one
     * until we get a unique one
     */

    private  String CreatePinRequestTable = "CREATE TABLE "+ PinRequestTableName+" ( "+
    " PinRequestId "+longType+" PRIMARY KEY,"+
    " PnfsId "+stringType+","+ //forein key

    // Expiration is of type long,
    // its value is time in milliseconds since the midnight of 1970 GMT (i think)
    // which has the same meaning as the value returned by
    // System.currentTimeMillis()
    // working with TIMESTAMP and with
    // java.sql.Date and java.sql.Time proved to be upredicatble and
    // too complex to work with.
    " Expiration "+longType+","+
    " RequestId "+longType +
    ");";
    private String CreateNextPinRequestIdTable =
	"CREATE TABLE " + NextPinRequestIdTableName + "(NEXTLONG BIGINT)";
    String insertNextPinRequestId = "INSERT INTO "+ NextPinRequestIdTableName+ " VALUES ("+Long.MIN_VALUE+")";
    /**
     * the constants representing position of each field in the table record
     *  if table structure is changed, these need to change too
     */
    private static final int PinRequestTable_PinRequestId = 1;
    private static final int PinRequestTable_PnfsId = 2;
    private static final int PinRequestTable_Expiration = 3;


    private long nextRequestId;

    private String InsertIntoPinRequestTable = "INSERT INTO "+PinRequestTableName+
    " VALUES ( ";
    private String UpdatePinRequestTable = "UPDATE "+PinRequestTableName+
    " SET ";


    private String DeleteFromPinRequests = "DELETE FROM "+PinRequestTableName +
    " WHERE PinRequestId = ";

    private String SelectPinRequest = "SELECT * FROM "+PinRequestTableName +
    " WHERE PinRequestId = ";

    private String SelectNextPinRequestId = "SELECT * FROM "+NextPinRequestIdTableName;



    /** in the begining we examine the whole database to see is there is a list of
     * outstanding pins which need to be expired or timed for experation
     */
    private String SelectEverything = "SELECT "+PinRequestTableName+".PinRequestId, "+
    PinRequestTableName+".PnfsId, "+
    PinRequestTableName+".Expiration, "+
    PinRequestTableName+".RequestId"+
    " FROM "+PinRequestTableName;

    private String pnfsManager = "PnfsManager";
    private String poolManager = "PoolManager";

    private long nextLongBase;
    private static final long NEXT_LONG_STEP=1000;
    private long nextLongIncrement=NEXT_LONG_STEP;

    /** Creates a new instance of PinManager */
    public PinManager(String name , String argString) throws Exception {

        super( name , PinManager.class.getName(), argString , false );
        Args _args = getArgs();
        jdbcUrl = _args.getOpt("jdbcUrl");
        jdbcClass = _args.getOpt("jdbcDriver");
        user = _args.getOpt("dbUser");
        pass = _args.getOpt("dbPass");
        pwdfile = _args.getOpt("pgPass");
        if (pwdfile != null && pwdfile.trim().length() > 0 ) {
            Pgpass pgpass = new Pgpass(pwdfile);      //VP
            pass = pgpass.getPgpass(jdbcUrl, user);   //VP
        }

        pool = JdbcConnectionPool.getPool(jdbcUrl, jdbcClass, user, pass);
        if(_args.getOpt("poolManager") != null) {
            poolManager = _args.getOpt("poolManager");
        }

        if(_args.getOpt("pnfsManager") != null) {
            poolManager = _args.getOpt("pnfsManager");
        }

        if(_args.getOpt("maxPinDuration") != null) {
            maxPinDuration = Long.parseLong(_args.getOpt("maxPinDuration"));
        }


        try {
            dbinit();
            initializeDatabasePinRequests();
            databaseUpdateThread = getNucleus().newThread(this,"DatabaseUpdateThread");
            updateWaitQueueThread =getNucleus().newThread(this,"UpdateWaitQueueThread");
            //databaseUpdateThread.setPriority(Thread.MIN_PRIORITY);
            databaseUpdateThread.start();
            updateWaitQueueThread.start();
        }
        catch (Throwable t) {
            esay("error starting PinManager");
            esay(t);
            start();
            kill();
        }
        start();


    }

    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.42 $" ); }

    public long nextPinrequestId() {
         return nextLong();
    }

    long _nextLongBase = 0;
    private static final String SelectNextPinRequestIdForUpdate =
        "SELECT * from "+NextPinRequestIdTableName+" FOR UPDATE ";
    private static final String IncreasePinRequestId = "UPDATE "+NextPinRequestIdTableName+
                        " SET NEXTLONG=NEXTLONG+"+NEXT_LONG_STEP;
    public synchronized long nextLong() {
        if(nextLongIncrement >= NEXT_LONG_STEP) {
            nextLongIncrement =0;
            Connection _con = null;

            try {
                _con = pool.getConnection();
                PreparedStatement s = _con.prepareStatement(SelectNextPinRequestIdForUpdate);
                say("dbInit trying "+SelectNextPinRequestIdForUpdate);
                ResultSet set = s.executeQuery();
                if(!set.next()) {
                    s.close();
                    throw new SQLException("table "+NextPinRequestIdTableName+" is empty!!!");
                }
                nextLongBase = set.getLong("NEXTLONG");
                s.close();
                say("nextLongBase is ="+nextLongBase);
                s = _con.prepareStatement(IncreasePinRequestId);
                say("executing statement: "+IncreasePinRequestId);
                int i = s.executeUpdate();
                s.close();
                _con.commit();
            } catch(SQLException e) {
                e.printStackTrace();
                try{
                    _con.rollback();
                }catch(Exception e1) {

                }
                pool.returnFailedConnection(_con);
                _con = null;
                nextLongBase = _nextLongBase;

            } finally {
                if(_con != null) {
                    pool.returnConnection(_con);

                }
            }
            _nextLongBase = nextLongBase+ NEXT_LONG_STEP;
        }

        long nextLong = nextLongBase +(nextLongIncrement++);;
        say(" return nextLong="+nextLong);
        return nextLong;
    }

    public void addToDatabaseStatementsQueue(String statement) {
        synchronized(databaseStatementsQueue) {
            databaseStatementsQueue.add(statement);
            databaseStatementsQueue.notify();
        }

    }


    public String hh_pin_pnfsid = "<pnfsId> <seconds> # pin a file by pnfsid for <seconds> seconds" ;
    public String ac_pin_pnfsid_$_2( Args args ) throws Exception {
        PnfsId pnfsId = new PnfsId( args.argv(0) ) ;
        long lifetime = Long.parseLong( args.argv(1) ) ;
        lifetime *=1000;
        return pin(pnfsId,lifetime,null,null);

    }

    public String hh_unpin = "<pinRequestId> <pnfsId> # unpin a a file by pinRequestId and by pnfsId" ;
    public String ac_unpin_$_2( Args args ) throws Exception {
        long pinRequestId = Long.parseLong(args.argv(0));
        PnfsId pnfsId = new PnfsId( args.argv(1) ) ;
        return unpin(pnfsId, pinRequestId,null,null);
    }

    public String hh_set_max_pin_duration = " # sets new max pin duration value in milliseconds" ;
    public String ac_set_max_pin_duration_$_1( Args args ) throws Exception {
        StringBuffer sb = new StringBuffer();
        long newMaxPinDuration = Long.parseLong(args.argv(0));
        if(newMaxPinDuration >0 ) {

            sb.append("old max pin duration was ");
            sb.append(maxPinDuration).append(" milliseconds\n");
            maxPinDuration = newMaxPinDuration;
            sb.append("max pin duration value set to ");
            sb.append(maxPinDuration).append(" milliseconds\n");

        } else {
            sb.append("max pin duration value must be nonnegative !!!");
        }

        return sb.toString();
    }
    public String hh_get_max_pin_duration = " # gets current max pin duration value" ;
    public String ac_get_max_pin_duration_$_0( Args args ) throws Exception {

        return Long.toString(maxPinDuration)+" milliseconds";
    }

    public String hh_ls = " [pnfsId] # lists all pins or specified pin" ;
    public String ac_ls_$_0_1(Args args) throws Exception {
        if (args.argc() > 0) {
            PnfsId pnfsId = new PnfsId(args.argv(0));
            Pin pin = pnfsIdToPins.get(pnfsId);
            return pin.toString();
        }

        Collection<Pin> pins = pnfsIdToPins.values();
        if (pins.isEmpty()) {
            return "no files are pinned";
        }

        StringBuffer sb = new StringBuffer();
        for (Pin pin : pins) {
            pin.append(sb);
        }
        return sb.toString();
    }

    public void getInfo( java.io.PrintWriter printWriter ) {
        StringBuffer sb = new StringBuffer();
        sb.append("PinManager\n");
        sb.append("\tjdbcClass=").append(jdbcClass).append('\n');
        sb.append("\tjdbcUrl=").append(jdbcUrl).append('\n');
        sb.append("\tjdbcUser=").append(user).append('\n');
        sb.append("\tmaxPinDuration=").append(maxPinDuration).append(" milliseconds \n");
        sb.append("\tnumber of files pinned=").append(pnfsIdToPins.size());
        printWriter.println(sb.toString());

    }

    private void dbinit() throws SQLException {
        //connection
            Connection _con = pool.getConnection();
            _con.setAutoCommit(true);
        try {

            // Add driver to JDBC
            Class.forName(jdbcClass);


            //get database info
            DatabaseMetaData md = _con.getMetaData();
            String tables[] = new String[] {PinRequestTableName,NextPinRequestIdTableName};
            String createTables[] =
            new String[] {CreatePinRequestTable,CreateNextPinRequestIdTable};

            for (int i =0; i<tables.length;++i) {
                ResultSet tableRs = md.getTables(null, null, tables[i] , null );


                if(!tableRs.next()) {
                    try {
                        Statement s = _con.createStatement();
                        say("dbinit trying "+createTables[i]);
                        int result = s.executeUpdate(createTables[i]);
                        s.close();
                        if(tables[i].equals(NextPinRequestIdTableName) ){
                             s = _con.createStatement();
                             say("dbinit trying "+insertNextPinRequestId);
                             result = s.executeUpdate(insertNextPinRequestId);
                        }
                        s.close();
                    }
                    catch(SQLException sqle) {

                        esay("SQL Exception (relation could already exist)");
                        esay(sqle);

                    }
                }


            }

            //check if old style pin requests table is present
            try {
                ResultSet tableRs = md.getTables(null, null, OldPinRequestTableName , null );
                if(tableRs.next()) {
                    //it is still there
                    //copy everything into the new table
                    String SelectEverythingFromOldPinRewquestTable =
                    "SELECT "+OldPinRequestTableName+".PinRequestId, "+
                    OldPinRequestTableName+".PnfsId, "+
                     OldPinRequestTableName+".Expiration FROM "+OldPinRequestTableName;
                     Statement stmt = _con.createStatement();
                     say("dbinit trying "+SelectEverythingFromOldPinRewquestTable);
                     ResultSet rs = stmt.executeQuery(SelectEverythingFromOldPinRewquestTable);
                     while(rs.next()) {
                        String pinRequestId = rs.getString(1);
                        PnfsId pnfsId = new PnfsId(rs.getString(2));
                        long expiration = rs.getLong(3);
                        Statement stmt1 = _con.createStatement();
                        say("dbinit trying :"+InsertIntoPinRequestTable+
                        pinRequestId+",\'"+pnfsId+"\',"+expiration+",NULL)");
                        stmt1.executeUpdate(InsertIntoPinRequestTable+
                        pinRequestId+",\'"+pnfsId+"\',"+expiration+",NULL)");
                        stmt1.close();
                        //rs.deleteRow();
                    }
                    stmt.close();
                    stmt = _con.createStatement();
                    say("dbinit trying :"+"DROP TABLE "+OldPinRequestTableName);
                    stmt.executeUpdate("DROP TABLE "+OldPinRequestTableName);
                    stmt.close();

                }
            }catch(SQLException sqlee) {
                esay(" failed to read values from old pinrequest table ");
                esay(sqlee);
            }

            try {
                //check if old pins table is still there
                ResultSet tableRs = md.getTables(null, null, OldPinsTableName , null );
                if(tableRs.next()) {
                    Statement stmt = _con.createStatement();
                    say("dbinit trying :"+"DROP TABLE "+OldPinsTableName);
                    stmt.executeUpdate("DROP TABLE "+OldPinsTableName);
                    stmt.close();

                }
            }catch(SQLException sqlee) {
                esay(" failed to read values from old pinrequest table ");
                esay(sqlee);
            }

           Statement s = _con.createStatement();
           say("dbinit trying "+SelectNextPinRequestId);
           ResultSet rs = s.executeQuery(SelectNextPinRequestId);
           if(rs.next())
           {
               nextRequestId = rs.getLong(1);
               nextRequestId++;
           }
           else
           {
               esay("can't read nextRequestId!!!");
               nextRequestId = System.currentTimeMillis();
           }

           s.close();

            // to support our transactions
            _con.setAutoCommit(false);
            pool.returnConnection(_con);
            _con = null;
        }
        catch (SQLException sqe) {
            esay(sqe);
            throw sqe;
        }
        catch (Exception ex) {
            esay(ex);
            throw new SQLException(ex.toString());
        }
        finally{
            if(_con != null) pool.returnFailedConnection(_con);
        }


    }



    /**
     * this method reads the pin requests from the database and either expires them or
     * starts timers, depending on the expiration time of each pin
     */
    private void initializeDatabasePinRequests() throws SQLException {
        long currentTimestamp = System.currentTimeMillis();
        Connection _con = pool.getConnection();
        try {

            Statement stmt = _con.createStatement();
            say("initializeDatabasePinRequests, trying "+SelectEverything);
            ResultSet rs = stmt.executeQuery(SelectEverything);
            while(rs.next()) {
                long pinRequestId = rs.getLong(1);
                PnfsId pnfsId = new PnfsId(rs.getString(2));
                long expiration = rs.getLong(3);
                long requestId = rs.getLong(4);
                if(expiration <= currentTimestamp) {
                    expiration = currentTimestamp + 60*1000 ;
                }
                long lifetime = expiration - currentTimestamp;
                Pin pin = null;
                if(pnfsIdToPins.containsKey(pnfsId)){
                    pin = pnfsIdToPins.get(pnfsId);
                }
                else {
                    pin = new Pin(pnfsId);
                    pin.setState(Pin.PINNED_STATE);
                    pnfsIdToPins.put(pnfsId,pin);
                }
                PinRequest pinRequest = new PinRequest(pinRequestId,
                        expiration,
                        requestId);

                pin.addPinRequest(pinRequest);
                startTimer(pnfsId, pinRequestId, lifetime);
            }
            rs.close();
            stmt.close();
            pool.returnConnection(_con);
            _con = null;
        }
        catch(SQLException sqle) {
            esay("initializeDatabasePinRequests failed "+sqle);
            throw sqle;
        }
        finally {
            if(_con != null) {
                pool.returnFailedConnection(_con);
            }
        }

    }


    public void messageArrived( CellMessage cellMessage ) {
        Object o = cellMessage.getMessageObject();
        if(!(o instanceof PinManagerMessage )) {
            super.messageArrived(cellMessage);
            return;
        }
        say("Message  arrived:"+o +" from "+cellMessage.getSourcePath());
        if(o instanceof PinManagerPinMessage) {
            PinManagerPinMessage pinRequest = (PinManagerPinMessage) o;
            pin(pinRequest, cellMessage);
        } else if(o instanceof PinManagerUnpinMessage) {
            PinManagerUnpinMessage unpinRequest = (PinManagerUnpinMessage) o;
            unpin(unpinRequest, cellMessage);
        } else if(o instanceof PinManagerExtendLifetimeMessage) {
            PinManagerExtendLifetimeMessage extendLifetimeRequest = (PinManagerExtendLifetimeMessage) o;
            extendLifetime(extendLifetimeRequest, cellMessage);
        }
    }

    public void exceptionArrived(ExceptionEvent ee) {
        esay("Exception Arrived: "+ee);
        esay(ee.getException());
        super.exceptionArrived(ee);
    }

    public void saveInPersistantStorage(PinRequest request, PnfsId pnfsId) {
        addToDatabaseStatementsQueue(InsertIntoPinRequestTable+
            request.getPinRequestId()+",\'"+pnfsId+"\',"+request.getExpiration()+
                ","+request.getRequestId()+")");
    }

    public void updateInPersistantStorage(PinRequest request, PnfsId pnfsId) {
        addToDatabaseStatementsQueue(UpdatePinRequestTable+
                " Expiration"+request.getExpiration()+
                " WHERE "+request.getPinRequestId());
    }
    public void deleteFromPersistantStorage(PinRequest info) {
        addToDatabaseStatementsQueue(DeleteFromPinRequests
            +info.getPinRequestId());
    }

    public void returnFailedResponse(Object reason,PinManagerMessage request,CellMessage cellMessage ) {
        esay("returnFailedResponse: "+reason);

        if(request == null ||cellMessage == null ) {
            esay("can not return failed response: pinManagerMessage is null ");
            return;
        }
        if( reason != null && !(reason instanceof java.io.Serializable)) {
            reason = reason.toString();
        }

        if(request.getReplyRequired()) {

            try {
                request.setReply();
                request.setFailed(1, reason);
                cellMessage.revertDirection();
                PinManager.this.sendMessage(cellMessage);
            }
            catch(Exception e) {
                esay("can not send a failed responce");
                esay(e);
            }
        }
    }
    public void returnResponse(PinManagerMessage request,CellMessage cellMessage ) {
        say("returnResponse");

        if(request == null ||cellMessage == null ) {
            esay("can not return  response: pinManagerMessage is null ");
            return;
        }

        if(request.getReplyRequired()) {

            try {
                request.setReply();
                cellMessage.revertDirection();
                PinManager.this.sendMessage(cellMessage);
            }
            catch(Exception e) {
                esay("can not send a responce");
                esay(e);
            }
        }
    }

    public void pin(PinManagerPinMessage pinRequest,CellMessage cellMessage) {
        PnfsId pnfsId = pinRequest.getPnfsId();
        if(pnfsId == null ) {
            returnFailedResponse("pnfsId == null", pinRequest, cellMessage);
            return;
        }
        long lifetime = pinRequest.getLifetime();
        if(lifetime <=0 )
        {
            returnFailedResponse("lifetime <=0", pinRequest, cellMessage);
            return;
        }
        pin(pnfsId,lifetime,pinRequest,cellMessage);
    }

    public String pin(PnfsId pnfsId,long lifetime,PinManagerPinMessage pinRequest,CellMessage cellMessage) {

        say("pin pnfsId="+pnfsId+" lifetime="+lifetime);
        long requestId = 0;
        if(pinRequest != null) {
            requestId = pinRequest.getRequestId();
        }
        if(lifetime > maxPinDuration) {
            lifetime = maxPinDuration;
            say("Pin lifetime exceeded maxPinDuration, new lifetime is set to "+lifetime);
        }

        synchronized(pnfsIdToPins) {
            Pin pin;
            if (pnfsIdToPins.containsKey(pnfsId)) {
                pin = pnfsIdToPins.get(pnfsId);
            } else {
                pin = new Pin(pnfsId);
                pin.setState(Pin.INITIAL_STATE);
                pnfsIdToPins.put(pnfsId, pin);
            }


            long pinRequestId = nextPinrequestId();
            PinRequest _pinRequest =
               new PinRequest(pinRequestId,
                    System.currentTimeMillis()+lifetime,
                    requestId);
            _pinRequest.setRequest(pinRequest);
            _pinRequest.setCellMessage(cellMessage);
            saveInPersistantStorage(_pinRequest, pnfsId);
            pin.addPinRequest(_pinRequest);
        }
        return "pinning started";
    }




    public void extendLifetime(PinManagerExtendLifetimeMessage extendLifetimeRequest, CellMessage cellMessage) {
       // new Unpinner(unpinRequest.getPinId(),unpinRequest.getPnfsId(),unpinRequest,cellMessage);
        String pinIdStr = extendLifetimeRequest.getPinId();
        if(pinIdStr == null) {
            returnFailedResponse("pinIdStr == null", extendLifetimeRequest, cellMessage);
            return;
        }
        PnfsId pnfsId = extendLifetimeRequest.getPnfsId();
        if(pnfsId == null ) {
            returnFailedResponse("pnfsId == null", extendLifetimeRequest, cellMessage);
            return;
        }
        long pinId = Long.parseLong(pinIdStr);
        long newLifetime = extendLifetimeRequest.getNewLifetime();
        extendLifetime(pnfsId, pinId,newLifetime, extendLifetimeRequest, cellMessage);
    }

    public String extendLifetime(PnfsId pnfsId, long pinRequestId, long newLifetime,
            PinManagerExtendLifetimeMessage extendLifetimeRequest, CellMessage cellMessage) {
        say("extend lifetime pnfsId="+pnfsId+" pinId="+pinRequestId+" new lifetime="+newLifetime);
        if(newLifetime > maxPinDuration) {
            newLifetime = maxPinDuration;
            say("Pin newLifetime exceeded maxPinDuration, newLifetime is set to "+newLifetime);
        }
        synchronized(pnfsIdToPins) {
           if(!pnfsIdToPins.containsKey(pnfsId)) {
                returnFailedResponse("pnfsId = "+pnfsId+" is not pinned", extendLifetimeRequest, cellMessage);
                return "pnfsId = "+pnfsId+" is not pinned";
            }

            Pin pin = pnfsIdToPins.get(pnfsId);


            PinRequest pinRequest = pin.getPinRequest(pinRequestId);
            if(pinRequest == null) {
                returnFailedResponse("pinId = "+pinRequestId+" is not found to pin pnfsId = "+pnfsId,
                        extendLifetimeRequest, cellMessage);
                return "pinId = "+pinRequestId+" is not found to pin pnfsId = "+pnfsId;
            }
            long expiration = pinRequest.getExpiration();
            long currentTime = System.currentTimeMillis();
            long remainingTime = expiration - currentTime;
            if(remainingTime >= newLifetime) {
                returnResponse(extendLifetimeRequest, cellMessage);
                return "remainingTime("+extendLifetimeRequest+") >= newLifetime("+cellMessage+")";
            }
            expiration = currentTime + newLifetime;
            pinRequest.setExpiration(expiration);
            updateInPersistantStorage(pinRequest, pnfsId);
            returnResponse(extendLifetimeRequest, cellMessage);
            return "extendLifetime succeeded";
        }
    }


    public void unpin(PinManagerUnpinMessage unpinRequest, CellMessage cellMessage) {
       // new Unpinner(unpinRequest.getPinId(),unpinRequest.getPnfsId(),unpinRequest,cellMessage);
        String pinIdStr = unpinRequest.getPinId();
        if(pinIdStr == null) {
            returnFailedResponse("pinIdStr == null", unpinRequest, cellMessage);
            return;
        }
        PnfsId pnfsId = unpinRequest.getPnfsId();
        if(pnfsId == null ) {
            returnFailedResponse("pnfsId == null", unpinRequest, cellMessage);
            return;
        }
        long pinId = Long.parseLong(pinIdStr);
        unpin(pnfsId, pinId, unpinRequest, cellMessage);
    }

    public String unpin(PnfsId pnfsId, long pinRequestId,PinManagerUnpinMessage unpinRequest, CellMessage cellMessage) {
        say("unpin pnfsId="+pnfsId+" pinId="+pinRequestId);
        synchronized(pnfsIdToPins) {
           if(!pnfsIdToPins.containsKey(pnfsId)) {
                returnFailedResponse("pnfsId = "+pnfsId+" is not pinned", unpinRequest, cellMessage);
                return "pnfsId = "+pnfsId+" is not pinned";
            }

           Pin pin = pnfsIdToPins.get(pnfsId);

            PinRequest pinRequest = pin.getPinRequest(pinRequestId);
            if(pinRequest == null) {
                returnFailedResponse("pinId = "+pinRequestId+" is not found to pin pnfsId = "+pnfsId,
                        unpinRequest, cellMessage);
                return "pinId = "+pinRequestId+" is not found to pin pnfsId = "+pnfsId;
            }
            pinRequest.setRequest(unpinRequest);
            pinRequest.setCellMessage(cellMessage);
            pin.unpinRequest(pinRequest);
        }
        return "unpinning started";
    }


    public void startTimer(final PnfsId pnfsId, final long requestId,long lifetime) {
        TimerTask tt = new TimerTask() {
            public void run() {
                //new Unpinner(pnfsId );
                unpin(pnfsId,requestId,null,null);
                TimerTask tt = pinTimerTasks.remove(requestId);
                if(tt == null) {
                    esay("TimerTask.run(): this timer for requestId="+requestId+" not found in pinTimerTasks hashtable");
                }

            }
        };

        pinTimerTasks.put(requestId, tt);

        // this is very approximate
        // but we do not need hard real time
        pinTimer.schedule(tt,lifetime);
    }

    public void stopTimer( long requestId) {
        TimerTask tt = pinTimerTasks.remove(requestId);
        if (tt == null) {
            esay("stopTimer(): timer not found for requestId=" + requestId);
        } else {
            tt.cancel();
        }
    }


    public void run()  {
        if(Thread.currentThread() == this.databaseUpdateThread) {
            while(true) {
                String nextSqlStatement = null;
                synchronized(databaseStatementsQueue) {
                    if(databaseStatementsQueue.isEmpty()) {
                        try{
                            databaseStatementsQueue.wait(1000*60);
                        }catch(InterruptedException ie) {
                            esay(ie);
                            esay("Database update thread received InterruptedException, terminating");
                            return;
                        }
                        continue;
                    }
                    nextSqlStatement = databaseStatementsQueue.remove(0);
                    if(nextSqlStatement == null ) continue;
                }
                Connection _con = null;
                try {
                    _con = pool.getConnection();
                    Statement stmt = _con.createStatement();
                    say("Database update thread executes: "+nextSqlStatement);
                    stmt.execute(nextSqlStatement);
                    _con.commit();
                    pool.returnConnection(_con);
                    _con = null;
                }
                catch(SQLException sqle) {
                    esay(sqle);
                }
                finally {
                    if(_con != null) {
                        pool.returnFailedConnection(_con);
                    }
                }
            }
        }
        else if (Thread.currentThread() == this.updateWaitQueueThread) {
            getNucleus().updateWaitQueue();
            try {
                Thread.sleep(30000L);
            }
            catch(InterruptedException ie) {
                esay("UpdateWaitQueueThread interrupted, quiting");
                return;
            }
        }
    }

    public  class PinRequest  {
        // States of the pin requests
        private long pinRequestId;
        private long expiration;
        //original srm request id
        private long requestId;
        private PinManagerMessage request;
        private CellMessage cellMessage;



        public PinRequest(long pinRequestId,long expiration, long requestId) {
            this.pinRequestId = pinRequestId;
            this.expiration = expiration;
            this.requestId = requestId;
        }

        public void say(String s) {
            PinManager.this.say("PinRequest : "+pinRequestId+" "+s);
        }

        public void esay(String s) {
            PinManager.this.esay("PinRequest : "+pinRequestId+" "+s);
        }

         public void esay(Throwable t) {
            PinManager.this.esay(t);
        }

        /** Getter for property pinRequestId.
         * @return Value of property pinRequestId.
         *
         */
        public long getPinRequestId() {
            return pinRequestId;
        }

        /** Getter for property expiration.
         * @return Value of property expiration.
         *
         */
        public long getExpiration() {
            return expiration;
        }

        public void setExpiration(long expiration) {
            this.expiration = expiration;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            append(sb);
            return sb.toString();
        }

        public long getRequestId() {
            return requestId;
        }
        public void append(StringBuffer sb) {
            sb.append("request id : ");
            sb.append(pinRequestId);
            sb.append(" expires : ").append(new java.util.Date(expiration));
            sb.append(" orginal srm request id:").append(requestId);
            if(request == null){
                sb.append(" request message : null");
            } else {
                sb.append("request message : ").append(request);
            }
        }

        /**
         * Getter for property request.
         * @return Value of property request.
         */
        public diskCacheV111.vehicles.PinManagerMessage getRequest() {
            return request;
        }

        /**
         * Setter for property request.
         * @param request New value of property request.
         */
        public void setRequest(diskCacheV111.vehicles.PinManagerMessage request) {
            this.request = request;
        }

        /**
         * Getter for property cellMessage.
         * @return Value of property cellMessage.
         */
        public dmg.cells.nucleus.CellMessage getCellMessage() {
            return cellMessage;
        }

        /**
         * Setter for property cellMessage.
         * @param cellMessage New value of property cellMessage.
         */
        public void setCellMessage(dmg.cells.nucleus.CellMessage cellMessage) {
            this.cellMessage = cellMessage;
        }
        public void returnFailedResponse(Object reason ) {
            esay("returnFailedResponse: "+reason);

            if(request == null ||cellMessage == null ) {
                esay("can not return failed response: pinManagerMessage is null ");
                return;
            }
            if( reason != null && !(reason instanceof java.io.Serializable)) {
                reason = reason.toString();
            }

            if(request.getReplyRequired()) {

                try {
                    request.setReply();
                    request.setFailed(1, reason);
                    cellMessage.revertDirection();
                    PinManager.this.sendMessage(cellMessage);
                }
                catch(Exception e) {
                    esay("can not send a failed responce");
                    esay(e);
                }
            }
            setCellMessage(null);
            setRequest(null);
        }

        public void returnMessage() {
            say("returnMessage("+request+ " ,"+cellMessage+")");
            if(request == null || cellMessage == null ) {
                esay("can not return message: pinManagerMessage is null ");
                return;
            }
            if(request instanceof PinManagerPinMessage) {
                ((PinManagerPinMessage)request).setPinId(Long.toString(getPinRequestId()));
            }
            if(request.getReplyRequired()) {
                try {
                    request.setReply();
                    cellMessage.revertDirection();
                    sendMessage(cellMessage);
                }
                catch(Exception e) {
                    esay("can not send a responce");
                    esay(e);
                }
            }
            else {
                say("reply is not required");
            }
            setCellMessage(null);
            setRequest(null);

        }


    }

     public  class Pin  {
        private PnfsId pnfsId;
        private StorageInfo storageInfo;
        private int state =  INITIAL_STATE;
        private static final int INITIAL_STATE = 0;
        private static final int PINNING_STATE = 1;
        private static final int PINNED_STATE = 2;
        private static final int UNPINNING_STATE = 3;
        private static final int PIN_AFTER_UNPINNING_IS_DONE_STATE = 4;
        private Handler handler ;

        public Pin(PnfsId pnfsId) {
            this.pnfsId = pnfsId;
        }

        public void say(String s) {
            PinManager.this.say("Pin : "+pnfsId+" "+s);
        }

        public void esay(String s) {
            PinManager.this.esay("Pin : "+pnfsId+" "+s);
        }

         public void esay(Throwable t) {
            PinManager.this.esay(t);
        }


        private String getStateString() {
            switch (getState()) {
                case(INITIAL_STATE):
                {
                    return "INITIAL_STATE";
                }
                case(PINNING_STATE):
                {
                    return "PINNING_STATE";
                }
                case(PINNED_STATE):
                {
                    return "PINNED_STATE";
                }
                case(UNPINNING_STATE):
                {
                    return "UNPINNING_STATE";
                }
                default:
                {
                    return "UNKNOWN_STATE";
                }
            }
        }

         public int getState() {
             return state;
         }

         public void setState(int state) {
             this.state = state;
         }

         private HashSet requests = new HashSet();
        //
         // this should be called from synchronized(pnfsIdToPins) block
         public void addPinRequest(PinRequest request) {

             say("addPinRequest : "+request);
             if(storageInfo == null && request.getRequest() != null ) {
                 storageInfo = request.getRequest().getStorageInfo();
             }
             switch (getState()) {
                case(INITIAL_STATE):
                {
                    state = PINNING_STATE;
                    requests.add(request);
                    say("start new Pinner");
                    handler = new Pinner(pnfsId, storageInfo,this);
                    break;
                }
                case(PINNING_STATE):
                {
                    requests.add(request);
                    esay("request is still being pinned, wait");
                    if(handler == null){
                        esay("pin is in already  PINNING_STATE, but handler is null!!!");
                    }
                    break;
                }
                case(PINNED_STATE):
                {
                    requests.add(request);
                    request.returnMessage();
                    //return success
                    break;
                }
                case(UNPINNING_STATE):
                {
                    requests.add(request);
                    say("pin is in the unpinning state, wait");
                    if(handler == null){
                        esay("pin is in UNPINNING_STATE, but handler is null!!!");
                    }
                    break;
                }
                default:
                {
                    request.returnFailedResponse("UNKNOWN_STATE");
                    throw new IllegalStateException("UNKNOWN_STATE");
                }
            }
         }

         // this should be called from synchronized(pnfsIdToPins)
         public void unpinRequest(PinRequest request) {
             say("unpinRequest "+request);
             if(requests.contains(request))
             {
                 requests.remove(request);
                 request.returnMessage();
                 deleteFromPersistantStorage(request);
                 stopTimer(request.getPinRequestId());
             }
             else
             {
                 esay("request not found"+request);
             }
             if(!requests.isEmpty() ){
                 return;
             }
             say(" no more pin requests to pin this pnfs, unpinning");
             switch (getState()) {
                case(PINNING_STATE):
                {
                    esay("request is still being pinned, wait");
                    if(handler == null){
                        esay("pin is in PINNING_STATE, but handler is null!!!");
                    }
                    break;
                }
                case(PINNED_STATE):
                {
                    state = UNPINNING_STATE;
                    say("starting Unpinner");
                    handler = new Unpinner(pnfsId,this);
                    break;
                }
                case(UNPINNING_STATE):
                {
                    say("already in the unpinning state, wait");
                    if(handler == null){
                        esay("pin is in UNPINNING_STATE, but handler is null!!!");
                    }
                    break;
                }
                default:
                {
                    esay("Illegal state for pin requests is empty, state="+getStateString());
                    PinManager.this.pnfsIdToPins.remove(pnfsId);
                }
            }
         }

         public Handler getHandler() {
             return handler;
         }

         public void setHandler(Handler handler) {
             this.handler = handler;
         }
        public String toString() {
            StringBuffer sb = new StringBuffer();
            append(sb);
            return sb.toString();
        }

        public void append(StringBuffer sb) {
            sb.append("pin of : ").append(pnfsId);
            sb.append(" state : ").append(getStateString());
            sb.append(" handler : ").append(handler);
            sb.append(" requests :\n");
            if(requests.isEmpty()) {
                sb.append("    none\n");
            } else {
                for(Iterator i = requests.iterator(); i.hasNext();) {
                    sb.append("  ").append(i.next()).append('\n');
                }
            }
        }


        /** Getter for property pnfsId.
         * @return Value of property pnfsId.
         *
         */
        public diskCacheV111.util.PnfsId getPnfsId() {
            return pnfsId;
        }

        // this should be called from synchronized(pnfsIdToPins) block
        public PinRequest getPinRequest(long pinRequestId) {
             for(Iterator i = requests.iterator() ; i.hasNext();) {
                 PinRequest request = (PinRequest) i.next();
                 if(request.getPinRequestId() == pinRequestId) {
                     return request;
                 }
             }
             return null;
        }

        public  void pinSucceded() {
             say("pinSucceded ");
             synchronized(pnfsIdToPins) {
                 if (state != PINNING_STATE){
                    esay("pinSucceded, but state is not PINNING_STATE, state : "+getStateString());
                 }
                 state = PINNED_STATE;
                 handler = null;

                 if(requests.isEmpty()){
                     say("pinSucceded, but requests is emplty, unpinning");
                     state = UNPINNING_STATE;
                     handler = new Unpinner(pnfsId,this);
                     return;
                 }
                 for(Iterator i = requests.iterator() ; i.hasNext();) {
                     PinRequest request = (PinRequest) i.next();
                     say("pinSucceded pnfsId="+pnfsId +" next pin request: "+request);
                     request.returnMessage();
                 }
             }
        }

        public void unpinSucceded() {
            say("unpinSucceded ");
            synchronized(pnfsIdToPins) {
                 if (state != UNPINNING_STATE){
                    esay("unpinSucceded, but state is not UNPINNING_STATE, state : "+getStateString());
                 }
                 handler = null;
                if(requests.isEmpty()){
                    say("unpinSucceded, no more pin requests, removing this pin");
                    PinManager.this.pnfsIdToPins.remove(pnfsId);
                    return;
                }
                else
                {
                    say("unpinSucceded, pinRequests is  not empty, pinning again");
                    state = PINNING_STATE;
                    handler = new Pinner(pnfsId, storageInfo,this);
                }
            }
        }

        public void pinFailed(Object reason) {
            say("pinFailed ");
            synchronized(pnfsIdToPins) {
                 if (state != PINNING_STATE){
                    esay("pinFailed, but state is not PINNING_STATE, state : "+getStateString());
                 }
                 handler = null;
                for(Iterator i = requests.iterator() ; i.hasNext();) {
                         PinRequest request = (PinRequest) i.next();
                        request.returnFailedResponse("pinFailed");
                        PinManager.this.deleteFromPersistantStorage(request);
                        stopTimer(request.getPinRequestId());
                }
                PinManager.this.pnfsIdToPins.remove(pnfsId);
            }
        }

        public void unpinFailed(Object reason) {
            say("unpinFailed");
            synchronized(pnfsIdToPins) {
                if (state != UNPINNING_STATE){
                   esay("unpinFailed, but state is not UNPINNING_STATE, state : "+getStateString());
                }
                handler = null;
                if(requests.isEmpty()) {
                    say("unpinFailed, no more pin requests, removing this pin");
                    PinManager.this.pnfsIdToPins.remove(pnfsId);
                    return;
                }
                else
                {
                    say("unpinSucceded, pinRequests is  not empty, pinning again");
                    state = PINNING_STATE;
                    handler = new Pinner(pnfsId, storageInfo,this);
                }
            }
        }
    }


    abstract class Handler implements CellMessageAnswerable {
        // state constats

        protected static final int INITIAL=0;
        protected static final int FINAL_SUCCESS=-1;
        protected static final int FINAL_FAILED=-2;

        protected PnfsId pnfsId;
        protected Pin pin;

        protected volatile int state = INITIAL;

        public Handler(PnfsId pnfsId, Pin pin ) {
            if(pnfsId == null || pin == null ) {
                esay("pnfsId == null || pin == null");
                throw new NullPointerException("pnfsId == null || pin == null");
            }
            this.pnfsId = pnfsId;
            this.pin = pin;
        }
        private Object sync = new Object();
        private boolean completed = false;
        private Object errorObject = null;

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


        public abstract void answerArrived(CellMessage request, CellMessage answer);

        public void answerTimedOut(CellMessage request) {
        }

        public void exceptionArrived(CellMessage request, Exception exception) {
        }
        public int getState(){
            return state;
        }

    }

    /** instance of this class is created for each pin request
     * its responsibility is to update  database, create timer
     * and communicate with other dCache cells in order to accomplish
     * pinning process
     */
    class Pinner extends Handler {
        // state constats

        private static final int WAITNG_STORAGE_INFO = 1;
        private static final int RECEIVED_STORAGE_INFO = 2;
        private static final int WAITING_SET_PNFS_FLAG_REPLY = 3;
        private static final int RECEIVED_SET_PNFS_FLAG_REPLY = 4;
        private static final int WAITNG_STORAGE_INFO_WHILE_PINNING = 5;
        private static final int RECEIVED_STORAGE_INFO_WHILE_PINNING = 6;
        private static final int WAITNG_POOL_NAME = 7;
        private static final int RECEIVED_POOL_NAME = 8;
        private static final int WAITNG_SET_STIKY_RESPONCE = 9;
        private static final int RECEIVED_SET_STIKY_RESPONCE = 10;
        private StorageInfo storageInfo;
        private String readPoolName;


        public Pinner(PnfsId pnfsId,StorageInfo storageInfo,Pin pin) {
            super(pnfsId, pin);
            say("constructor");
            try {
                this.storageInfo = storageInfo;
                pin();
            }
            catch(Exception e) {
                esay(e);
                // need to return failures here
                returnFailedResponse(e);
            }
        }

        private String getStateString() {
            switch (getState()) {
                case(INITIAL):
                {
                    return "INITIAL";
                }
                case(FINAL_FAILED):
                {
                    return "FINAL_FAILED";
                }
                case(FINAL_SUCCESS):
                {
                    return "FINAL_SUCCESS";
                }
                case(WAITNG_STORAGE_INFO):
                {
                    return "WAITNG_STORAGE_INFO";
                }
                case(RECEIVED_STORAGE_INFO):
                {
                    return "RECEIVED_STORAGE_INFO";
                }
                case(RECEIVED_SET_PNFS_FLAG_REPLY):
                {
                    return "RECEIVED_SET_PNFS_FLAG_REPLY";
                }
                case(WAITING_SET_PNFS_FLAG_REPLY):
                {
                    return "WAITING_SET_PNFS_FLAG_REPLY";
                }
                case(WAITNG_STORAGE_INFO_WHILE_PINNING):
                {
                    return "WAITNG_STORAGE_INFO_WHILE_PINNING";
                }
                case(RECEIVED_STORAGE_INFO_WHILE_PINNING):
                {
                    return "RECEIVED_STORAGE_INFO_WHILE_PINNING";
                }
                case(WAITNG_POOL_NAME):
                {
                    return "WAITNG_POOL_NAME";
                }
                case(RECEIVED_POOL_NAME):
                {
                    return "RECEIVED_POOL_NAME";
                }
                case(WAITNG_SET_STIKY_RESPONCE):
                {
                    return "WAITNG_SET_STIKY_RESPONCE";
                }
                case(RECEIVED_SET_STIKY_RESPONCE):
                {
                    return "RECEIVED_SET_STIKY_RESPONCE";
                }
                default:
                {
                    return "UNKNOWN_STATE";
                }
            }
        }

        public void say(String s) {
            PinManager.this.say("Pinner : "+pnfsId+" "+s);
        }

        public void esay(String s) {
            PinManager.this.esay("Pinner : "+pnfsId+" "+s);
        }

         public void esay(Throwable t) {
            PinManager.this.esay(t);
        }

        public String toString(){
            return "Pinner : "+pnfsId+" : "+getStateString();
        }

        private void returnFailedResponse(Object reason){
            if(pin == null )
            {
                esay("returnFailedResponse is called, but pin is null !!!");
                return;
            }
            esay("return Failed Responce: "+reason);
            pin.pinFailed(reason);
            pin = null;
            state = FINAL_FAILED;
            complete();
        }

        private void returnSuccess(){
            if(pin == null )
            {
                esay("returnSuccess is called, but pin is null !!!");
                return;
            }
            say("returnSuccess");
            pin.pinSucceded();
            pin = null;
            state = FINAL_SUCCESS;
            complete();
        }

        private void getStorageInfo(boolean pinning) throws java.io.NotSerializableException {
            PnfsGetStorageInfoMessage storageInfoRequest =
                new PnfsGetStorageInfoMessage(pnfsId);
            if(pinning) {
                state = WAITNG_STORAGE_INFO_WHILE_PINNING;
            }
            else {
                state = WAITNG_STORAGE_INFO;
            }
            sendMessage( new CellMessage(
            new CellPath(pnfsManager) ,
            storageInfoRequest ) ,
            true , true ,
            this ,
            60*60*1000) ;
        }

        private void pin() throws Exception {

                //we need to actually pin the file in pnfs
                // set sticky flag at at least on read pool
                // and then return success
                if(storageInfo == null) {
                    getStorageInfo(true);
                }
                else {
                    setPnfsFlag();
                }
                return;


        }

        public void setPnfsFlag() {
            try {
                PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , "s" , "put" ) ;
                pfm.setValue( "*" ) ;
                pfm.setReplyRequired(true);
                state = WAITING_SET_PNFS_FLAG_REPLY;
                sendMessage( new CellMessage(
                new CellPath(pnfsManager) ,
                pfm ) ,
                true , true ,
                this ,
                60*60*1000) ;
            }
            catch(Exception ee ) {
                esay("setPnfsFlag failed : ");
                esay(ee);
                returnFailedResponse(ee);
                return ;
            }
        }

        public void findReadPoolLocation() {
            say("findReadPoolLocation()");
            DCapProtocolInfo pinfo = new DCapProtocolInfo( "DCap",3,0,"localhost",0) ;
            if(storageInfo == null) {
                esay("findReadPoolLocation() storageInfo is null!!");
                returnFailedResponse("findReadPoolLocation() storageInfo is null!!");
                return;
            }
            PoolMgrSelectReadPoolMsg request =
            new PoolMgrSelectReadPoolMsg(
            pnfsId,
            storageInfo,
            pinfo ,
            0);

            say("sending PoolMgrSelectReadPoolMsg");
            try {
                state = WAITNG_POOL_NAME;
                sendMessage(
                new CellMessage(
                new CellPath(poolManager) ,
                request ) ,
                true , true ,
                this ,
                1*24*60*60*1000
                ) ;
            }
            catch(Exception ee ) {
                esay("SelectReadPoolFailed : ");
                esay(ee);
                returnFailedResponse(ee);
                return ;
            }
        }

        public void setStickyFlag() {
            PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(readPoolName, pnfsId, true,getNucleus().getCellName(),-1);

            say("sending PoolSetStickyMessage");
            try {
                state = WAITNG_SET_STIKY_RESPONCE;
                sendMessage(
                new CellMessage(
                new CellPath(readPoolName) ,
                setStickyRequest ) ,
                true , true ,
                this ,
                1*24*60*60*1000
                ) ;
            }
            catch(Exception ee ) {
                esay("PoolSetStickyMessage failed : ");
                esay(ee);
                returnFailedResponse(ee);
                return ;
            }
        }

        public void answerArrived(CellMessage question, CellMessage answer) {
            Object o = answer.getMessageObject();
            if(o instanceof Message) {
                Message message = (Message)answer.getMessageObject() ;
                if(message instanceof PnfsFlagMessage) {
                    PnfsFlagMessage setFlagResponce =
                    (PnfsFlagMessage) message;
                    if(state != WAITING_SET_PNFS_FLAG_REPLY) {
                        esay(" PnfsFlagMessage arrived, "+
                        "but the state != WAITING_SET_PNFS_FLAG_REPLY, state is "+state);
                        return;
                    }
                    state = RECEIVED_SET_PNFS_FLAG_REPLY;
                    if(setFlagResponce.getReturnCode() != 0) {
                        esay(" failed to set the pnfs flag: "+setFlagResponce.getErrorObject());
                        returnFailedResponse(setFlagResponce.getErrorObject());
                    }
                    say("pnfs flag is set successfully");
                    findReadPoolLocation();
                    return;
                }
                else if( message instanceof PnfsGetStorageInfoMessage ) {
                    PnfsGetStorageInfoMessage pnfsGetStorageInfoResponce =
                    (PnfsGetStorageInfoMessage)message;
                    if(state != WAITNG_STORAGE_INFO && state !=WAITNG_STORAGE_INFO_WHILE_PINNING ) {
                        esay(" PnfsGetStorageInfoMessage arrived, "+
                        "but the state != WAITNG_STORAGE_INFO &&"+
                        "state !=WAITNG_STORAGE_INFO_WHILE_PINNING , state is "+state);
                        return;
                    }
                    state = RECEIVED_STORAGE_INFO;
                    if(pnfsGetStorageInfoResponce.getReturnCode() != 0) {
                        esay(" failed to receive the storage info: "+pnfsGetStorageInfoResponce.getErrorObject());
                        returnFailedResponse(pnfsGetStorageInfoResponce.getErrorObject());
                        return;
                    }
                    say("pnfs info arrived: "+pnfsGetStorageInfoResponce);
                    this.pnfsId = pnfsGetStorageInfoResponce.getPnfsId();
                    this.storageInfo = pnfsGetStorageInfoResponce.getStorageInfo();
                    if(state == WAITNG_STORAGE_INFO) {
                        try {
                            pin();
                        }
                        catch(Exception e) {
                            returnFailedResponse(e);
                        }
                        return;
                    }
                    setPnfsFlag();
                    return;

                }
                else if(message instanceof PoolMgrSelectReadPoolMsg) {
                    PoolMgrSelectReadPoolMsg poolMgrResponce =
                    (PoolMgrSelectReadPoolMsg) message;
                    if(state != WAITNG_POOL_NAME) {
                        esay(" PoolMgrSelectReadPoolMsg arrived, "+
                        "but the state != WAITNG_POOL_NAME, state is "+state);
                        return;
                    }
                    state = RECEIVED_POOL_NAME;
                    if(poolMgrResponce.getReturnCode() != 0) {
                        esay(" failed to get pool name: "+poolMgrResponce.getErrorObject());
                        returnFailedResponse(poolMgrResponce.getErrorObject());
                    }
                    say("received read pool name successfully");
                    readPoolName = poolMgrResponce.getPoolName();
                    setStickyFlag();
                    return;
                }
                else if(message instanceof PoolSetStickyMessage) {
                    PoolSetStickyMessage poolSetStickyResponce =
                    (PoolSetStickyMessage) message;
                    if(state != WAITNG_SET_STIKY_RESPONCE) {
                        esay(" PoolSetStickyMessage arrived, "+
                        "but the state != WAITNG_SET_STIKY_RESPONCE, state is "+state);
                        return;
                    }
                    state = RECEIVED_SET_STIKY_RESPONCE;
                    if(poolSetStickyResponce.getReturnCode() != 0) {
                        esay(" failed to set sticky: "+poolSetStickyResponce.getErrorObject());
                        returnFailedResponse(poolSetStickyResponce.getErrorObject());
                    }
                    say("set sticky successfully");
                    returnSuccess();
                    return;
                }
            }
        }
    }

    class Unpinner extends Handler {
        // state constats

        private static final int WAITING_DELETE_PNFS_FLAG_REPLY = 3;
        private static final int RECEIVED_DELETE_PNFS_FLAG_REPLY = 4;
        private static final int WAITNG_CACHED_LOCATIONS = 7;
        private static final int RECEIVED_CACHED_LOCATIONS = 8;


        private volatile int state = INITIAL;

        public Unpinner(PnfsId pnfsId ,Pin pin) {
            super(pnfsId, pin);
            this.pnfsId = pnfsId;
            say("constructor");
            try {
                unpin();
            }
            catch(Exception e) {
                esay(e);

                returnFailedResponse(e);
            }
        }

        private String getStateString() {
            switch (getState()) {
                case(INITIAL):
                {
                    return "INITIAL";
                }
                case(FINAL_SUCCESS):
                {
                    return "FINAL_SUCCESS";
                }
                case(FINAL_FAILED):
                {
                    return "FINAL_FAILED";
                }
                case(WAITING_DELETE_PNFS_FLAG_REPLY):
                {
                    return "WAITING_DELETE_PNFS_FLAG_REPLY";
                }
                case(RECEIVED_DELETE_PNFS_FLAG_REPLY):
                {
                    return "RECEIVED_DELETE_PNFS_FLAG_REPLY";
                }
                case(WAITNG_CACHED_LOCATIONS):
                {
                    return "WAITNG_CACHED_LOCATIONS";
                }
                case(RECEIVED_CACHED_LOCATIONS):
                {
                    return "RECEIVED_CACHED_LOCATIONS";
                }
                default:
                {
                    return "UNKNOWN_STATE";
                }
            }
        }

        public void say(String s) {
            PinManager.this.say("Unpinner : "+pnfsId+" "+s);
        }

        public void esay(String s) {
            PinManager.this.esay("Unpinner : "+pnfsId+" "+s);
        }

         public void esay(Throwable t) {
            PinManager.this.esay(t);
        }

         public String toString(){
            return "Unpinner : "+pnfsId+" : "+getStateString();
        }

        private void returnFailedResponse(Object reason){
            if(pin == null )
            {
                esay("returnFailedResponse is called, but pin is null !!!");
                return;
            }
            esay("return Failed Responce: "+reason);
            pin.unpinFailed(reason);
            pin = null;
            state = FINAL_FAILED;
            complete();
        }

        private void returnSuccess(){
            if(pin == null )
            {
                esay("returnSuccess is called, but pin is null !!!");
                return;
            }
            say("returnSuccess");
            pin.unpinSucceded();
            pin = null;
            state = FINAL_SUCCESS;
            complete();
        }


        private void unpin() throws Exception {
                deletePnfsFlag();
        }




        public void deletePnfsFlag() {
            try {
                PnfsFlagMessage pfm = new PnfsFlagMessage( pnfsId , "s" , "remove" ) ;
                pfm.setValue( "*" ) ;
                pfm.setReplyRequired(true);
                state = WAITING_DELETE_PNFS_FLAG_REPLY;
                sendMessage( new CellMessage(
                new CellPath(pnfsManager) ,
                pfm ) ,
                true , true ,
                this ,
                60*60*1000) ;
            }
            catch(Exception ee ) {
                esay("setPnfsFlag failed : ");
                esay(ee);
                returnFailedResponse(ee);
                return ;
            }
        }


        public void findCacheLocations() {
            say("findCacheLocations()");
            PnfsGetCacheLocationsMessage request =
            new PnfsGetCacheLocationsMessage(
            pnfsId);

            say("sending PnfsGetCacheLocationsMessage");
            try {
                state = WAITNG_CACHED_LOCATIONS;
                sendMessage(
                new CellMessage(
                new CellPath(pnfsManager) ,
                request ) ,
                true , true ,
                this ,
                1*24*60*60*1000
                ) ;
            }
            catch(Exception ee ) {
                esay("findCacheLocations Failed : ");
                esay(ee);
                returnFailedResponse(ee);
                return ;
            }
        }

        public void unsetStickyFlag(String poolName) {
            PoolSetStickyMessage setStickyRequest =
            new PoolSetStickyMessage(poolName, pnfsId, false,getNucleus().getCellName(),-1);

            say("sending PoolSetStickyMessage");
            try {
                sendMessage(
                new CellMessage(
                new CellPath(poolName) ,
                setStickyRequest ) );
            }
            catch(Exception ee ) {
                esay("PoolSetStickyMessage (false) failed : ");
                esay(ee);
                return ;
            }
        }

        public void answerArrived(CellMessage question, CellMessage answer) {
            Object o = answer.getMessageObject();
            if(o instanceof Message) {
                Message message = (Message)answer.getMessageObject() ;
                if(message instanceof PnfsFlagMessage) {
                    PnfsFlagMessage setFlagResponce =
                    (PnfsFlagMessage) message;
                    if(state != WAITING_DELETE_PNFS_FLAG_REPLY) {
                        esay(" PnfsFlagMessage arrived, "+
                        "but the state != WAITING_DELETE_PNFS_FLAG_REPLY, state is "+state);
                        return;
                    }
                    state = RECEIVED_DELETE_PNFS_FLAG_REPLY;
                    if(setFlagResponce.getReturnCode() != 0) {
                        esay(" failed to delete the pnfs flag: "+setFlagResponce.getErrorObject());
                        returnFailedResponse(setFlagResponce.getErrorObject());
                    }
                    say("pnfs flag is deleted successfully");
                    findCacheLocations();
                    return;
                }
                else if(message instanceof PnfsGetCacheLocationsMessage) {
                    PnfsGetCacheLocationsMessage cachedLocationsResponce =
                    (PnfsGetCacheLocationsMessage) message;
                    if(state != WAITNG_CACHED_LOCATIONS) {
                        esay(" PnfsGetCacheLocationsMessage arrived, "+
                        "but the state != WAITNG_CACHED_LOCATIONS, state is "+state);
                        return;
                    }
                    state = RECEIVED_CACHED_LOCATIONS;
                    if(cachedLocationsResponce.getReturnCode() != 0) {
                        esay(" failed to get cached locations: "+cachedLocationsResponce.getErrorObject());
                        returnFailedResponse(cachedLocationsResponce.getErrorObject());
                    }
                    say("received cached locations successfully");
                    List<String> locations = cachedLocationsResponce.getCacheLocations();
                    for(String nextLocation: locations) {
                        unsetStickyFlag(nextLocation);
                    }
                    returnSuccess();
                    return;
                }
            }
        }
    }
}
