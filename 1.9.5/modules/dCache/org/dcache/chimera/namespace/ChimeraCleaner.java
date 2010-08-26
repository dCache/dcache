package org.dcache.chimera.namespace;

import java.io.File;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.multicaster.BroadcastRegisterMessage;
import dmg.util.Args;

import org.dcache.chimera.DbConnectionInfo;
import org.dcache.chimera.XMLconfig;
import org.dcache.services.hsmcleaner.BroadcastRegistrationTask;

import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

/**
 * @author Irina Kozlova
 * @version 22 Oct 2007
 *
 * ChimeraCleaner: takes file names from the table public.t_locationinfo_trash,
 * removes them from the corresponding pools and then from the table as well.
 * @since 1.8
 */

public class ChimeraCleaner extends CellAdapter implements Runnable {

    private final String _cellName;
    private final CellNucleus _nucleus;
    private final Args _args;
    private long _refreshInterval = 300000; // see actual value in "refresh"
    private long _replyTimeout = 10000; // 10 seconds

    private final String _broadcastCellName;
    private Thread _cleanerThread = null;

    private final Object _sleepLock = new Object();

    private long _recoverTimer = 1800000L; // 30 min in milliseconds

    // how many files will be processed at once, default 100 :
    private int _processAtOnce = 100;

    private Connection _dbConnection;

    private final static String POOLUP_MESSAGE =
        "diskCacheV111.vehicles.PoolManagerPoolUpMessage";

    /**
     * Task for periodically registering the cleaner at the broadcast
     * to receive pool up messages.
     */
    private final BroadcastRegistrationTask _broadcastRegistration;

    /**
     * Timer used for implementing timeouts.
     */
    private Timer _timer = new Timer();

    /**
     * List of pools that are excluded from cleanup
     */
    private final Map<String, Long> _poolsBlackList = new ConcurrentHashMap<String, Long>();

    /**
     * Logger
     */
    private static final Logger _logNamespace = Logger.getLogger("logger.org.dcache.namespace."+ ChimeraCleaner.class.getName());

    public ChimeraCleaner(String cellName, String args) throws Exception {

        super(cellName, ChimeraCleaner.class.getName(), args, false);

        _cellName = cellName;
        _args = getArgs();
        _nucleus = getNucleus();

        useInterpreter(true);

        CellPath me = new CellPath(_cellName, _nucleus.getCellDomainName());
        _broadcastRegistration =
            new BroadcastRegistrationTask(this, POOLUP_MESSAGE, me);
        _timer.schedule(_broadcastRegistration, 0, 300000); // 5 minutes

        XMLconfig config = new XMLconfig( new File( _args.getOpt("chimeraConfig") ) );
        // for now we are looking for fsid==0 only

        DbConnectionInfo dbinfo = config.getDbInfo(0);
        /*
         * TODO: make use of c3po db connection pool
         */
        _dbConnection = dbInit(dbinfo.getDBurl(), dbinfo.getDBdrv(), dbinfo.getDBuser(), dbinfo.getDBpass() );

        try {

            // Usage : ...
            // [-refresh=<refreshInterval_in_sec.>]
            // [-processFilesPerRun=<filesPerRun] default : 100

            String refreshString = _args.getOpt("refresh");
            if (refreshString != null) {
                try {

                    _refreshInterval = Long.parseLong(refreshString) * 1000L;

                } catch (NumberFormatException ee) {
                    // bad numbers ignored
                }

            }

            String tmp = _args.getOpt("processFilesPerRun");
            if (tmp != null) {
                try {
                    _processAtOnce = Integer.parseInt(tmp);
                } catch (NumberFormatException ee) {
                    // bad numbers ignored
                }
            }

            String reportTo = _args.getOpt("reportRemove" );
            if( reportTo != null && reportTo.length() > 0 && !reportTo.equals("none") ) {
                _broadcastCellName=reportTo;
                _logNamespace.debug("Remove report sent to " + _broadcastCellName);
            }else{
                _broadcastCellName=null;
                _logNamespace.debug("Remove report disabled");
            }


               if (_logNamespace.isDebugEnabled()) {
                    _logNamespace.debug("Refresh Interval set to (in milliseconds): " + _refreshInterval);
                    _logNamespace.debug("Number of files processed at once : "+ _processAtOnce);
               }

            (_cleanerThread = _nucleus.newThread(this, "Cleaner")).start();

        } catch (Exception e) {
            _logNamespace.error("Exception occurred while running cleaner constructor: "+ e);
            start();
            kill();
            throw e;
        }
        start();
    }

    private static Connection dbInit(String jdbcUrl, String jdbcClass, String user, String pass ) throws SQLException {

        Connection dbConnection = null;

        if ((jdbcUrl == null) || (jdbcClass == null) || (user == null)
                || (pass == null) ) {
            throw new IllegalArgumentException("Not enough arguments to Init SQL database");
        }

        try {

            // Add driver to JDBC
            Class.forName(jdbcClass);

            dbConnection = DriverManager.getConnection(jdbcUrl, user, pass);

            if (_logNamespace.isDebugEnabled()){
            	_logNamespace.debug("Database connection with jdbcUrl="
                            + jdbcUrl + "; user=" + user + "; pass=" + pass);
        	}

        } catch (SQLException sqe) {
            _logNamespace.error("Failed to connect to database: " + sqe);
            throw sqe;
        } catch (Exception ex) {
            _logNamespace.error("Failed to connect to database: ", ex);
            throw new SQLException(ex.toString());
        }

        return dbConnection;
    }

    @Override
    public void cleanUp() {

        _logNamespace.debug("Clean up called");
        if (_cleanerThread != null)
            _cleanerThread.interrupt();

    }

    public void run() {

        for (int i = 0; !Thread.interrupted(); i++) {
        	if (_logNamespace.isDebugEnabled()){
        		_logNamespace.debug("*********NEW_RUN************* run():  i= "+i);
        		_logNamespace.debug("INFO: Refresh Interval (milliseconds): "+ _refreshInterval);
	            _logNamespace.debug("INFO: Number of files processed at once: " + _processAtOnce);
        	}

        	try {

                // get list of pool names from the trash_table
                List<String> poolListDB = getPoolList(_dbConnection);

                if (_logNamespace.isDebugEnabled()){
                	_logNamespace.debug("List of Pools from the trash-table : "+ poolListDB);
                }

                String[] poolList = poolListDB.toArray(new String[poolListDB.size()]);


                // if there are some pools in the blackPoolList (i.e., pools that are down/do not exist),
                //extract them from the poolListDB
                if (_poolsBlackList.size() > 0) {

                    _logNamespace.debug("htBlackPools.size()="+ _poolsBlackList.size());


                    for (Map.Entry<String, Long> blackListEntry : _poolsBlackList.entrySet()) {
                        String poolName = blackListEntry.getKey();
                        long valueTime = blackListEntry.getValue();

                        //check, if it is time to remove pool from the black list
                        if ((valueTime != 0)
                                && (_recoverTimer > 0)
                                && ((System.currentTimeMillis() - valueTime) > _recoverTimer)) {

                            _poolsBlackList.remove(poolName);
                            if (_logNamespace.isDebugEnabled()) {
                            		_logNamespace.debug("Remove the following pool from the black pool list : "+ poolName);
                            	}
                            }
                    }

                    Set<String> keyBlackPools = _poolsBlackList.keySet();

                    List<String> blackPoolListNew = new ArrayList<String>(keyBlackPools);

                    Collection<String> toRemove_coll = new ArrayList<String>(poolListDB);
                    toRemove_coll.removeAll(blackPoolListNew);
                    poolList = toRemove_coll.toArray(new String[toRemove_coll.size()]);

                }

                if ((poolList != null) && (poolList.length > 0)) {

                	if (_logNamespace.isDebugEnabled()) {

                		_logNamespace.debug("The following pools are sent to runDelete(..):");

                		for (int x = 0; x < poolList.length; x++) {
                			_logNamespace.debug("poolList[i=" + x + "]="+ poolList[x]);
                    		}

                   			_logNamespace.debug(" poolList.length="+ poolList.length);
                		}

                    runDelete(poolList);

                }



            } catch (Exception ee) {
                _logNamespace.error("ChimeraCleaners: runDelete :" , ee);
            }

            //wait... _refreshInterval ... after each run
            try{
            	synchronized( _sleepLock ){
                 _sleepLock.wait( _refreshInterval ) ;
                 }

                }catch( InterruptedException ie ){
                	_logNamespace.error("Cleaner thread interrupted");
                	break ;
                }

        }

    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    static void tryToClose(ResultSet o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            _logNamespace.error("tryToClose ResultSet", e);
        }
    }

    /**
     * database resource cleanup
     *
     * @param o
     */
    static void tryToClose(PreparedStatement o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            _logNamespace.error("tryToClose PreparedStatement", e);
        }
    }

    /**
     * database resource cleanup
     */
    static void tryToClose(Connection o) {
        try {
            if (o != null)
                o.close();
        } catch (SQLException e) {
            _logNamespace.error("Failed to close Connection: " + e);
        }
    }

    private static final String sqlGetPoolList = "SELECT DISTINCT ilocation "
            + "FROM t_locationinfo_trash WHERE itype=1";

    /**
     * getPoolList
     * returns a list of pools (pool names) from the trash-table
     *
     * @param dbConnection
     * @throws java.sql.SQLException
     * @return list of pools (pool names)
     */

    List<String> getPoolList(Connection dbConnection) throws SQLException {

        List<String> poollist = new ArrayList<String>();

        ResultSet rs = null;
        PreparedStatement stGetPoolList = null;
        try {

            stGetPoolList = dbConnection.prepareStatement(sqlGetPoolList);

            rs = stGetPoolList.executeQuery();

            while (rs.next()) {

                String poolname = rs.getString("ilocation");

                poollist.add(poolname);
            }

        } finally {
            tryToClose(rs);
            tryToClose(stGetPoolList);
        }

        return poollist;
    }

    private static final String sqlGetPoolsForFile = "SELECT ilocation FROM t_locationinfo_trash "
        + "WHERE ipnfsid=? ORDER BY iatime";

    private static final String sqlGetFileListForPool = "SELECT ipnfsid FROM t_locationinfo_trash "
            + "WHERE ilocation=? ORDER BY iatime";

    private static final String sqlRemoveFiles = "DELETE FROM t_locationinfo_trash "
            + "WHERE ilocation=? AND ipnfsid=? AND itype=1";

    /**
     * removeFiles
     * Delete entries from the trash-table.
     * Pool name and the file names are input parameters.
     *
     * @param dbConnection
     * @param poolname name of the pool
     * @param filelist file list for this pool
     * @throws java.sql.SQLException
     *
     */

    void removeFiles(Connection dbConnection, String poolname, String[] filelist) throws SQLException {

        /*
         * FIXME: we send remove to the broadcaster even if we failed to
         * remove a record from the DB.
         */
        informBroadcaster(filelist);

        PreparedStatement stRemoveFiles = null;

        for (String filename: filelist) {

            try {

                stRemoveFiles = dbConnection.prepareStatement(sqlRemoveFiles);

                stRemoveFiles.setString(1, poolname);
                stRemoveFiles.setString(2, filename);
                int rc = stRemoveFiles.executeUpdate();

            } finally {
                tryToClose(stRemoveFiles);
            }

        }
    }

    /**
     * runDelete
     * Delete files on each pool from the poolList.
     *
     * @param poolList list of pools
     * @throws java.sql.SQLException
     * @throws java.lang.InterruptedException
     */
    private void runDelete(String[] poolList) throws SQLException, InterruptedException {

        for (int i = 0; (i < poolList.length) && !Thread.interrupted(); i++) {

            String thisPool = poolList[i];

            _logNamespace.info("runDelete(): Now processing pool : "+ thisPool);

            //only if pool is not in poolsBlackPool start cleaning
            if (! _poolsBlackList.containsKey(thisPool)) {
            cleanPoolComplete(_dbConnection, thisPool);
            }

        }
    }

    /**
     * sendRemoveToPoolCleaner
     * removes set of files from the pool
     *
     * @param poolName name of the pool
     * @param removeList list of files to be removed from this pool
     * @return list of successfully REMOVED files or 'null' in case NO ONE FILE has been removed.
     * (If the returned list is empty, then NO ONE FILE has been removed.)
     * @throws java.lang.InterruptedException
     */

    private String[] sendRemoveToPoolCleaner(String poolName,
            List<String> removeList) throws InterruptedException {

        if (_logNamespace.isDebugEnabled()) {
            _logNamespace.debug("sendRemoveToPoolCleaner: poolName="+ poolName);
            _logNamespace.debug("sendRemoveToPoolCleaner: removeList="+ removeList);
        }
        PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(poolName);
        String[] pnfsList = removeList.toArray(new String[removeList.size()]);

        msg.setFiles(pnfsList);

        CellMessage cellMessage = new CellMessage(new CellPath(poolName), msg);

        try {

            cellMessage = sendAndWait(cellMessage, _replyTimeout);
        } catch (NoRouteToCellException nrt) {
            // put poolName into BlackPoolList

            _poolsBlackList.put(poolName, Long.valueOf(System.currentTimeMillis()));

            if (_logNamespace.isDebugEnabled())
            {
                _logNamespace.debug("put in blackPool poolName= "+ poolName);
                _logNamespace.debug(" and time="+ System.currentTimeMillis());
            }

            _logNamespace.error(" NoRouteToCellException: "+nrt);
            return null;
        }

        if (cellMessage == null) {

        	 _logNamespace.error("remove message to " + poolName + " timed out");

            return null;
        }

        Object reply = cellMessage.getMessageObject();

        if (reply == null) {

        	 _logNamespace.error("reply message from " + poolName + " didn't contain messageObject");

            return null;
        }

        if (!(reply instanceof PoolRemoveFilesMessage)) {
        	 _logNamespace.error("got unexpected reply class : " + reply.getClass().getName());

            return null;
        }
        //
        // if return code is ok. we assume that all files have been
        // deleted from the pool.
        //
        PoolRemoveFilesMessage prfm = (PoolRemoveFilesMessage) reply;
        if (prfm.getReturnCode() == 0) {

             _logNamespace.info("submitted to sendRemoveToPoolCleaner files were removed from "+ poolName);


            return pnfsList;
        }
        Object o = prfm.getErrorObject();
        //
        // if return code is not ok, the error object should
        // either contain some exception, or it should contain
        // a list of files which coudn't be removed, so that we
        // try again later.
        //
        if (o instanceof String[]) {

            String[] notRemoved = (String[]) o;
            _logNamespace.error("sendRemoveToPoolCleaner : " + notRemoved.length
                    + " files couldn't be removed from " + poolName);

            if (_logNamespace.isDebugEnabled()) {
            	_logNamespace.debug("SOME files could not be removed from pool "+poolName);
            	_logNamespace.debug("not removed files:");
            	for (int x = 0; x < notRemoved.length; x++) {
        			_logNamespace.debug("notRemoved[i=" + x + "]="+ notRemoved[x]);
            		}
            }

            // find out which files were successfully removed from the pool and store them in 'okRemoved'
            Collection<String> okRemoved_coll = new ArrayList<String>(Arrays.asList(pnfsList));

            okRemoved_coll.removeAll(Arrays.asList(notRemoved));

            String[] okRemoved = okRemoved_coll.toArray(new String[okRemoved_coll.size()]);

            //INFO: if all files from the original list (pnfsList) are now in the 'notRemoved', then 'okRemoved' is empty

            return okRemoved;

        } else if (o == null) {

        	_logNamespace.error("sendRemoveToPoolCleaner : reply from " + poolName + " [null]");

            return null;
        } else {

        	_logNamespace.error("sendRemoveToPoolCleaner : reply from " + poolName + " ["
                    + o.getClass().getName() + "]=" + o.toString());

            return null;
        }

    }

    @Override
    public void messageArrived(CellMessage message) {
        Object obj = message.getMessageObject();
        if (obj instanceof PoolManagerPoolUpMessage) {
        	PoolManagerPoolUpMessage poolManagerPoolUpMessage = (PoolManagerPoolUpMessage) obj;
            String poolName = poolManagerPoolUpMessage.getPoolName();

            //if pool is Enabled now and is in poolsBlackList, then remove this pool from poolsBlackList
            if ( poolManagerPoolUpMessage.getPoolMode().isEnabled() && _poolsBlackList.containsKey(poolName) ) {
                _poolsBlackList.remove(poolName);
                }

            //if pool is Disabled now and it is not in poolsBlackList, then put this pool into poolsBlackList
            if ( poolManagerPoolUpMessage.getPoolMode().isDisabled() && !_poolsBlackList.containsKey(poolName) ) {
                _poolsBlackList.put(poolName, Long.valueOf(System.currentTimeMillis()));
                }
            return;
        } else if (obj instanceof BroadcastRegisterMessage) {
            return;
        }
        _logNamespace.error("Unexpected message arrived from : " + message.getSourcePath()
                + " " + obj.getClass().getName() + " " + obj.toString());
    }

    /**
     * cleanPoolComplete
     * delete all files from the pool 'poolName' found in the trash-table for this pool
     *
     * @param dbConnection
     * @param poolName name of the pool
     * @throws java.sql.SQLException
     * @throws java.lang.InterruptedException
     */
    void cleanPoolComplete(Connection dbConnection, String poolName) throws SQLException, InterruptedException {

        List<String> filePartList = new ArrayList<String>();

        if(_logNamespace.isDebugEnabled()) {
        	_logNamespace.debug("CleanPoolComplete(): poolname= " + poolName);
        }

        ResultSet rs = null;
        PreparedStatement stGetFileListForPool = null;
        try {

            stGetFileListForPool = dbConnection.prepareStatement(sqlGetFileListForPool);

            stGetFileListForPool.setString(1, poolName);


            rs = stGetFileListForPool.executeQuery();

            int counter = 0;
            while (rs.next()) {

                String filename = rs.getString("ipnfsid");

                filePartList.add(filename);
                counter++;
                if(_logNamespace.isDebugEnabled()) {
                    _logNamespace.debug("filename=" + filename);
                    _logNamespace.debug("counter=" + counter);
                }
                if (counter == _processAtOnce || rs.isLast()) {

                    String[] okRemoved = sendRemoveToPoolCleaner(poolName,filePartList);

                    if ((okRemoved != null) && (okRemoved.length > 0)) {

                        // remove these files (okRemoved) from trash_table
                        if(_logNamespace.isDebugEnabled()) {
                        	_logNamespace.debug(" INFO for pool "+ poolName);
                            _logNamespace.debug(" number of deleted files: okRemoved.length="+ okRemoved.length);
                            _logNamespace.debug(" files that WERE DELETED from this pool : ");
                            for (int x = 0; x < okRemoved.length; x++) {
                    			_logNamespace.debug("okRemoved[i=" + x + "]="+ okRemoved[x]);
                            }

                        }
                        removeFiles(dbConnection, poolName, okRemoved);
                    } else if (okRemoved == null) {
                        _poolsBlackList.put(poolName, Long.valueOf(System.currentTimeMillis()));
                        if(_logNamespace.isDebugEnabled()) {
                            _logNamespace.debug("cleanPoolComplete: pool "+ poolName + "added into black list");
                    	}
                        /*
                         * we can'tremove files from this pool. Stop processing.
                         */
                        break;
                    }

                    counter = 0;
                    filePartList.clear();
                }

            }

        } finally {
            tryToClose(rs);
            tryToClose(stGetFileListForPool);
        }

    }

    /**
     * send list of removed files to broadcaster
     *
     * @param fileList list of files to be removed
     */
    private void informBroadcaster(String[] fileList){

        if( fileList == null || fileList.length == 0 ) return;

        String broadcast = _broadcastCellName  ;
        if( broadcast == null )return ;

        PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(broadcast) ;
        msg.setFiles( fileList ) ;

        /*
         * no rely required
         */
        msg.setReplyRequired(false);

        try{
            sendMessage( new CellMessage( new CellPath(broadcast) , msg )  ) ;
            _logNamespace.debug("have broadcasted 'remove files' message to "+broadcast);
        }catch(NoRouteToCellException ee ){
            _logNamespace.error("Problems sending 'remove files' message to "+broadcast+" : "+ee.getMessage());
        }
     }

    ////////////////////////////////////////////////////////////////////////////
    public static String hh_rundelete = " # run Cleaner ";
    public String ac_rundelete(Args args) throws Exception {

        List<String> tmpPoolList = getPoolList(_dbConnection);
        runDelete(tmpPoolList.toArray(new String[tmpPoolList.size()]));

        return "";
    }

    public static String hh_show_info = " # show info ";
    public String ac_show_info(Args args) throws Exception {

        StringBuilder sb = new StringBuilder();

            sb.append("Refresh Interval (ms) : ").append(_refreshInterval).append("\n");
            sb.append("Reply Timeout (ms): ").append(_replyTimeout).append("\n");
            sb.append("Recover Timer (min): ").append(_recoverTimer/1000L/60L).append("\n");
            sb.append("Number of files processed at once: ").append(_processAtOnce);


        return sb.toString();
    }


    public static String hh_ls_blacklist = " # list pools in the 'black pool list'";
    public String ac_ls_blacklist(Args args) throws Exception {


        StringBuilder sb = new StringBuilder();

        for( String pool : _poolsBlackList.keySet() ) {
            sb.append(pool).append("\n");
        }

        return sb.toString();
    }

    public static String hh_remove_from_blacklist = "<poolName> # remove this pool from the black list";
    public String ac_remove_from_blacklist_$_1(Args args) throws Exception {

        String poolName = args.argv(0);
        if(_poolsBlackList.remove(poolName) !=null) {
        	return "Pool "+poolName+" is removed from the black list ";
        }

        return "Pool "+poolName+" was not found in the black list ";
    }

    public static String hh_remove_file = "<pnfsID> # remove this file ";
    public String ac_remove_file_$_1(Args args) throws Exception {

        String filePnfsID = args.argv(0);

        List<String> removeFile = new ArrayList<String>();
        removeFile.add(filePnfsID);

        ResultSet rs = null;
        PreparedStatement stGetPoolsForFile = null;
        try {

            stGetPoolsForFile = _dbConnection.prepareStatement(sqlGetPoolsForFile);
            stGetPoolsForFile.setString(1, filePnfsID);
            rs = stGetPoolsForFile.executeQuery();

            while (rs.next()) {

                String poolName = rs.getString("ilocation");
                String[] okFileRemoved=sendRemoveToPoolCleaner(poolName, removeFile);

                if ((okFileRemoved != null) && (okFileRemoved.length > 0)) {

                    removeFiles(_dbConnection, poolName, okFileRemoved);

                    if (_logNamespace.isInfoEnabled()) {
                    	_logNamespace.info("ac_remove_file: File "+ Arrays.toString(okFileRemoved) );
                    	_logNamespace.info("ac_remove_file: was successfully removed from the pool "+poolName);
                    }

                    return "Submitted file was successfully removed from the pool "+poolName;
                }

                _logNamespace.debug(" submitted in ac_remove_file file could NOT be removed from pool "+poolName);
		        if (_poolsBlackList.containsKey(poolName)) {
		        	return "Submitted file could NOT be removed: it is on the pool which is in the black list now. Pool= "+poolName;
		        }

		        return "Submitted file could NOT be removed from pool "+poolName;
         	}


        } finally {
            tryToClose(rs);
            tryToClose(stGetPoolsForFile);
        }
      return "";

    }

    public static String hh_clean_pool = "<poolName> # clean this pool ";
    public String ac_clean_pool_$_1(Args args) throws Exception {

        String poolName = args.argv(0);
        if (! _poolsBlackList.containsKey(poolName)) {
        cleanPoolComplete(_dbConnection, poolName);
        return "";
        } else {
            return "This pool is not available for the moment and therefore will not be cleaned.";
    }
    }

    public static String hh_set_refresh = "<refreshTimeInSeconds> # > 5 [-wakeup] (Time must be greater than 5 seconds)" ;
    public String ac_set_refresh_$_0_1( Args args ) throws Exception{

       if( args.argc() > 0 ){
          long newRefresh = Long.parseLong(args.argv(0)) * 1000L;
          if( newRefresh < 5000L ) {
             throw new
             IllegalArgumentException("Time must be greater than 5 seconds");
          }
          _refreshInterval = newRefresh ;
       }
       if( args.getOpt("wakeup") != null ){
          synchronized( _sleepLock ){
              _sleepLock.notifyAll() ;
          }
       }
       return "Refresh set to "+(_refreshInterval/1000L)+" seconds" ;
    }

    public static String hh_set_processedAtOnce = "<processedAtOnce> # max number of files sent to pool for processing at once " ;
    public String ac_set_processedAtOnce_$_1( Args args ) throws Exception {

       if( args.argc() > 0 ){
          int processAtOnce = Integer.valueOf(args.argv(0));
          if( processAtOnce == 0 ) {
             throw new
             IllegalArgumentException("Number of files must be greater than 0 ");
          }
          _processAtOnce = processAtOnce ;
       }

       return "Number of files processed at once set to "+ _processAtOnce ;
    }


    @Override
    public CellVersion getCellVersion(){ return new CellVersion(diskCacheV111.util.Version.getVersion(),"$Revision: 1.23 $" ); }
    @Override
    public void getInfo( PrintWriter pw ){
        pw.println("ChimeraCleaner $Revision: 1.23 $");
    }

}
