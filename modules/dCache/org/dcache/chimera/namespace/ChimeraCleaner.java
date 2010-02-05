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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import dmg.cells.nucleus.CellNucleus;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellVersion;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.Args;

import org.dcache.chimera.DbConnectionInfo;
import org.dcache.chimera.XMLconfig;
import org.dcache.services.hsmcleaner.BroadcastRegistrationTask;

import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.commons.util.SqlHelper;

import javax.sql.DataSource;

import com.mchange.v2.c3p0.DataSources;
/**
 * @author Irina Kozlova
 * @version 22 Oct 2007
 *
 * ChimeraCleaner: takes file names from the table public.t_locationinfo_trash,
 * removes them from the corresponding pools and then from the table as well.
 * @since 1.8
 */

public class ChimeraCleaner extends AbstractCell implements Runnable {

    private final String _cellName;
    private final CellNucleus _nucleus;
    private final Args _args;
    private long _refreshInterval = 300000; // see actual value in "refresh"
    private long _replyTimeout = 10000; // 10 seconds

    private Thread _cleanerThread = null;

    private final Object _sleepLock = new Object();

    private long _recoverTimer = 1800000L; // 30 min in milliseconds

    // how many files will be processed at once, default 100 :
    private int _processAtOnce = 100;

    private DataSource _dbConnectionDataSource;

    private final static String POOLUP_MESSAGE =
        diskCacheV111.vehicles.PoolManagerPoolUpMessage.class.getName();

    /**
     * Task for periodically registering the cleaner at the broadcast
     * to receive pool up messages.
     */
    private final BroadcastRegistrationTask _broadcastRegistration;

    /**
     * List of pools that are excluded from cleanup
     */
    private final ConcurrentHashMap<String, Long> _poolsBlackList = new ConcurrentHashMap<String, Long>();

    /**
     * Logger
     */
    private static final Logger _logNamespace = Logger.getLogger("logger.org.dcache.namespace."+ ChimeraCleaner.class.getName());

    /**
     * cell communication stub to Broadcaster cell.
     */
    private final CellStub _broadcasterStub;

    /**
     * cell communication stub to pools.
     */
    private final CellStub _poolStub;

    public ChimeraCleaner(String cellName, String args) throws Exception {

        super(cellName, args);

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
         * make use of c3po db connection pool
         */
        this.dbInit(dbinfo.getDBurl(), dbinfo.getDBdrv(), dbinfo.getDBuser(), dbinfo.getDBpass() );

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
                 _broadcasterStub = new CellStub();
                 _broadcasterStub.setCellEndpoint(this);
                 _broadcasterStub.setDestination(reportTo);
                _logNamespace.debug("Remove report sent to " + reportTo);
            }else{
                 _broadcasterStub = null;
                _logNamespace.debug("Remove report disabled");
            }


               if (_logNamespace.isDebugEnabled()) {
                    _logNamespace.debug("Refresh Interval set to (in milliseconds): " + _refreshInterval);
                    _logNamespace.debug("Number of files processed at once : "+ _processAtOnce);
               }

            (_cleanerThread = _nucleus.newThread(this, "Cleaner")).start();

            _poolStub = new CellStub();
            _poolStub.setCellEndpoint(this);
            _poolStub.setTimeout(_replyTimeout);

        } catch (Exception e) {
            _logNamespace.error("Exception occurred while running cleaner constructor: "+ e);
            start();
            kill();
            throw e;
        }
        start();
    }

    void dbInit(String jdbcUrl, String jdbcClass, String user, String pass ) throws SQLException {

        if ((jdbcUrl == null) || (jdbcClass == null) || (user == null)
                || (pass == null) ) {
            throw new IllegalArgumentException("Not enough arguments to Init SQL database");
        }

        try {

            // Add driver to JDBC
            Class.forName(jdbcClass);

            DataSource unpooled = DataSources.unpooledDataSource(jdbcUrl, user, pass);
            _dbConnectionDataSource = DataSources.pooledDataSource( unpooled );

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
                List<String> poolListDB = getPoolList();

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
                            		_logNamespace.debug("Remove the following pool from the Black List : "+ poolName);
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

    private static final String sqlGetPoolList = "SELECT DISTINCT ilocation "
            + "FROM t_locationinfo_trash WHERE itype=1";

    /**
     * getPoolList
     * returns a list of pools (pool names) from the trash-table
     *
     * @throws java.sql.SQLException
     * @return list of pools (pool names)
     */

    List<String> getPoolList() throws SQLException {

        Connection dbConnection = null;
        List<String> poollist = new ArrayList<String>();

        ResultSet rs = null;
        PreparedStatement stGetPoolList = null;
        try {
            dbConnection = _dbConnectionDataSource.getConnection();
            stGetPoolList = dbConnection.prepareStatement(sqlGetPoolList);

            rs = stGetPoolList.executeQuery();

            while (rs.next()) {

                String poolname = rs.getString("ilocation");

                poollist.add(poolname);
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetPoolList);
            SqlHelper.tryToClose(dbConnection);
        }

        return poollist;
    }

    private static final String sqlGetPoolsForFile = "SELECT ilocation FROM t_locationinfo_trash "
        + "WHERE ipnfsid=? AND itype=1 ORDER BY iatime";

    private static final String sqlGetFileListForPool = "SELECT ipnfsid FROM t_locationinfo_trash "
            + "WHERE ilocation=? ORDER BY iatime";

    private static final String sqlRemoveFiles = "DELETE FROM t_locationinfo_trash "
            + "WHERE ilocation=? AND ipnfsid=? AND itype=1";

    /**
     * removeFiles
     * Delete entries from the trash-table.
     * Pool name and the file names are input parameters.
     *
     * @param poolname name of the pool
     * @param filelist file list for this pool
     *
     */

    void removeFiles(String poolname, String[] filelist) {

        /*
         * FIXME: we send remove to the broadcaster even if we failed to
         * remove a record from the DB.
         */
        informBroadcaster(filelist);
        Connection dbConnection = null;
        PreparedStatement stRemoveFiles = null;

        for (String filename: filelist) {

            try {
                dbConnection = _dbConnectionDataSource.getConnection();
                stRemoveFiles = dbConnection.prepareStatement(sqlRemoveFiles);

                stRemoveFiles.setString(1, poolname);
                stRemoveFiles.setString(2, filename);
                int rc = stRemoveFiles.executeUpdate();
            } catch (SQLException e) {
                _logNamespace.error("Failed to remove entries frm DB: " + e.getMessage());
            } finally {
                SqlHelper.tryToClose(stRemoveFiles);
                SqlHelper.tryToClose(dbConnection);
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
            cleanPoolComplete(thisPool);
            }

        }
    }

    /**
     * sendRemoveToPoolCleaner
     * removes set of files from the pool
     *
     * @param poolName name of the pool
     * @param removeList list of files to be removed from this pool
     * @throws java.lang.InterruptedException
     */

    private void sendRemoveToPoolCleaner(String poolName,
            List<String> removeList) throws InterruptedException {

        if (_logNamespace.isDebugEnabled()) {
            _logNamespace.debug("sendRemoveToPoolCleaner: poolName="+ poolName);
            _logNamespace.debug("sendRemoveToPoolCleaner: removeList="+ removeList);
        }
        PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage(poolName);
        String[] pnfsList = removeList.toArray(new String[removeList.size()]);

        msg.setFiles(pnfsList);

        MessageCallback<PoolRemoveFilesMessage> callback =
                new RemoveMessageCallback(poolName, pnfsList);

        /*
         * we may use sendAndWait here. Unfortunately, PoolRemoveFilesMessage
         * returns an array of not removed files as a error object.
         * SendAndWait will convert it into exception.
         *
         * As a work around that we simulate synchronous behavior.
         */
        synchronized(callback) {
            _poolStub.send(new CellPath(poolName), msg, PoolRemoveFilesMessage.class, callback);
            callback.wait(_replyTimeout);
        }
    }

    public void messageArrived(PoolManagerPoolUpMessage poolUpMessage) {

        String poolName = poolUpMessage.getPoolName();

        /*
         * Keep track of pools statuses:
         *     remove pool from the black list in case of new status is enabled.
         *     put a pool into black list if new status is disabled.
         */

        if ( poolUpMessage.getPoolMode().isEnabled() ) {
            _poolsBlackList.remove(poolName);
        }

        if ( poolUpMessage.getPoolMode().isDisabled() ) {
            _poolsBlackList.putIfAbsent(poolName, Long.valueOf(System.currentTimeMillis()));
        }
    }

    /**
     * cleanPoolComplete
     * delete all files from the pool 'poolName' found in the trash-table for this pool
     *
     * @param poolName name of the pool
     * @throws java.sql.SQLException
     * @throws java.lang.InterruptedException
     */
    void cleanPoolComplete(String poolName) throws SQLException, InterruptedException {

        Connection dbConnection = null;
        List<String> filePartList = new ArrayList<String>();

        if(_logNamespace.isDebugEnabled()) {
        	_logNamespace.debug("CleanPoolComplete(): poolname= " + poolName);
        }

        ResultSet rs = null;
        PreparedStatement stGetFileListForPool = null;
        try {
            dbConnection = _dbConnectionDataSource.getConnection();
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
                    sendRemoveToPoolCleaner(poolName,filePartList);
                    counter = 0;
                    filePartList.clear();
                }

            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetFileListForPool);
            SqlHelper.tryToClose(dbConnection);
        }

    }

    /**
     * send list of removed files to broadcaster
     *
     * @param fileList list of files to be removed
     */
    private void informBroadcaster(String[] fileList){

        if( fileList == null || fileList.length == 0 ) return;

        if( _broadcasterStub == null ) return ;

        try {
            PoolRemoveFilesMessage msg = new PoolRemoveFilesMessage("") ;
            msg.setFiles( fileList ) ;

            /*
             * no rely required
             */
            msg.setReplyRequired(false);

            _broadcasterStub.send( msg ) ;
            _logNamespace.debug("have broadcasted 'remove files' message to " +
                                _broadcasterStub.getDestinationPath());
        } catch (NoRouteToCellException e) {
            _logNamespace.debug("Failed to broadcast 'remove files' message: " +
                                e.getMessage());
        }
     }

    ////////////////////////////////////////////////////////////////////////////
    public static String hh_rundelete = " # run Cleaner ";
    public String ac_rundelete(Args args) throws Exception {

        List<String> tmpPoolList = getPoolList();
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


    public static String hh_ls_blacklist = " # list pools in the Black List";
    public String ac_ls_blacklist(Args args) throws Exception {


        StringBuilder sb = new StringBuilder();

        for( String pool : _poolsBlackList.keySet() ) {
            sb.append(pool).append("\n");
        }

        return sb.toString();
    }

    public static String hh_remove_from_blacklist = "<poolName> # remove this pool from the Black List";
    public String ac_remove_from_blacklist_$_1(Args args) throws Exception {

        String poolName = args.argv(0);
        if(_poolsBlackList.remove(poolName) !=null) {
        	return "Pool "+poolName+" is removed from the Black List ";
        }

        return "Pool "+poolName+" was not found in the Black List ";
    }

    private String adminCleanFileDisk(String filePnfsID) throws SQLException, InterruptedException {
        Connection dbConnection = null;
        List<String> removeFile = new ArrayList<String>(1);
        removeFile.add(filePnfsID);

        ResultSet rs = null;
        PreparedStatement stGetPoolsForFile = null;
        try {
            dbConnection = _dbConnectionDataSource.getConnection();
            stGetPoolsForFile = dbConnection.prepareStatement(sqlGetPoolsForFile);
            stGetPoolsForFile.setString(1, filePnfsID);
            rs = stGetPoolsForFile.executeQuery();

            while (rs.next()) {
                String poolName = rs.getString("ilocation");
                sendRemoveToPoolCleaner(poolName, removeFile);
            }

        } finally {
            SqlHelper.tryToClose(rs);
            SqlHelper.tryToClose(stGetPoolsForFile);
            SqlHelper.tryToClose(dbConnection);
        }
      return "";

    }

    public static String hh_clean_file = "<pnfsID> # clean this file (file will be deleted from DISK)";
    public String ac_clean_file_$_1(Args args) throws Exception {

        return adminCleanFileDisk(args.argv(0));

    }

    public static String hh_clean_pool = "<poolName> # clean this pool ";
    public String ac_clean_pool_$_1(Args args) throws Exception {

        String poolName = args.argv(0);
        if (! _poolsBlackList.containsKey(poolName)) {
        cleanPoolComplete(poolName);
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

    private class RemoveMessageCallback
            implements MessageCallback<PoolRemoveFilesMessage> {

        private final String _poolName;
        private final String[] _filesToRemove;

        RemoveMessageCallback(String poolName, String[] filesToRemove) {
            _poolName = poolName;
            _filesToRemove = filesToRemove;
        }

        @Override
        public synchronized void success(PoolRemoveFilesMessage message) {
            try {
                removeFiles(_poolName, _filesToRemove );
            }finally{
                notifyAll();
            }
        }

        @Override
        public synchronized void failure(int rc, Object o) {
            try {
                if( o instanceof String[] ) {
                    String[] notRemoved = (String[])o;
                    // find out which files were successfully removed from the pool and store them in 'okRemoved'
                    Collection<String> filesToBeRemoved = Arrays.asList(_filesToRemove); //A
                    Collection<String> filesNotRemoved = Arrays.asList(notRemoved); //B
                    Collection<String> okRemoved_coll = new ArrayList<String>(); //C

                    Iterator<String> iter = filesToBeRemoved.iterator();
                    while(iter.hasNext()) {
                        String currentFile = iter.next();
                        if ( !filesNotRemoved.contains(currentFile) ){
                            okRemoved_coll.add(currentFile);  //C=A-B
                        }
                    }

                    String[] okRemoved = okRemoved_coll.toArray(new String[okRemoved_coll.size()]);
                    removeFiles(_poolName, okRemoved);
                }
            } finally {
                notifyAll();
            }
        }

        @Override
        public synchronized void noroute() {
            try {
                 _logNamespace.warn("Pool " + _poolName + " is down.");
                _poolsBlackList.put(_poolName, System.currentTimeMillis());
            } finally {
                notifyAll();
            }
        }

        @Override
        public synchronized void timeout() {
            try {
                _logNamespace.warn("remove message to " + _poolName + " timed out.");
                _poolsBlackList.put(_poolName, System.currentTimeMillis());
            } finally {
                notifyAll();
            }
        }
    }
}
