package org.dcache.chimera.namespace;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import org.dcache.cells.AbstractCell;
import org.dcache.cells.CellStub;
import org.dcache.db.AlarmEnabledDataSource;
import org.dcache.services.hsmcleaner.PoolInformationBase;
import org.dcache.services.hsmcleaner.RequestTracker;
import org.dcache.util.Args;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.Option;

import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

/**
 * @author Irina Kozlova
 * @version 22 Oct 2007
 *
 * ChimeraCleaner: takes file names from the table public.t_locationinfo_trash,
 * removes them from the corresponding pools and then from the table as well.
 * @since 1.8
 */

public class ChimeraCleaner extends AbstractCell implements Runnable
{
    private static final Logger _log =
        LoggerFactory.getLogger(ChimeraCleaner.class);

    @Option(
        name="refresh",
        description="Refresh interval",
        required=true
    )
    protected long _refreshInterval;

    @Option(
            name="refreshUnit",
            description="Refresh interval unit",
            required=true
    )
    protected TimeUnit _refreshIntervalUnit;

    @Option(
        name="recover",
        description="",
        required=true
    )
    protected long _recoverTimer;

    @Option(
        name="recoverUnit",
        description="",
        required=true
    )
    protected TimeUnit _recoverTimerUnit;

    @Option(
        name="poolTimeout",
        description="",
        required=true
    )
    protected long _replyTimeout;

    @Option(
        name="poolTimeoutUnit",
        description="",
        required=true
    )
    protected TimeUnit _replyTimeoutUnit;

    @Option(
        name="processFilesPerRun",
        description="The number of files to process at once",
        required=true,
        unit="files"
    )
    protected int _processAtOnce;

    @Option(
        name="reportRemove",
        description="The cells to report removes to"
    )
    protected String _reportTo;

    @Option(
        name="hsmCleaner",
        description="Whether to enable the HSM cleaner",
        required=true
    )
    protected boolean _hsmCleanerEnabled;

    @Option(
        name="hsmCleanerRequest",
        description="Maximum number of files to include in a single request",
        required=true,
        unit="files"
    )
    protected int _hsmCleanerRequest;

    @Option(
        name="hsmCleanerTimeout",
        description="Timeout in milliseconds for delete requests send to HSM-pools",
        required=true
    )
    protected long _hsmTimeout;

    @Option(
        name="hsmCleanerTimeoutUnit",
        description="Timeout in milliseconds for delete requests send to HSM-pools",
        required=true
    )
    protected TimeUnit _hsmTimeoutUnit;

    @Option(
        name="threads",
        description="Size of thread pool",
        required=true
    )
    protected int _threadPoolSize;

    private CellPath[] _deleteNotificationTargets;

    private final ConcurrentHashMap<String, Long> _poolsBlackList =
        new ConcurrentHashMap<>();

    private RequestTracker _requests;
    private ScheduledExecutorService _executor;
    private ScheduledFuture<?> _cleanerTask;
    private PoolInformationBase _pools = new PoolInformationBase();
    private AlarmEnabledDataSource _dataSource;
    private JdbcTemplate _db;
    private CellStub _notificationStub;
    private CellStub _poolStub;

    public ChimeraCleaner(String cellName, String args)
    {
        super(cellName, args);
    }

    @Override
    protected void starting()
        throws Exception
    {
        super.starting();
        useInterpreter(true);

        _notificationStub = new CellStub(this);
        _deleteNotificationTargets =
                Splitter.on(",")
                        .omitEmptyStrings()
                        .splitToList(Strings.nullToEmpty(_reportTo))
                        .stream()
                        .map(CellPath::new)
                        .toArray(CellPath[]::new);

        _executor = Executors.newScheduledThreadPool(_threadPoolSize);

        dbInit(getArgs().getOpt("chimera.db.url"),
               getArgs().getOpt("chimera.db.user"), getArgs().getOpt("chimera.db.password"));

        if (_hsmCleanerEnabled) {
            _requests = new RequestTracker();
            _requests.setMaxFilesPerRequest(_hsmCleanerRequest);
            _requests.setTimeout(_hsmTimeoutUnit.toMillis(_hsmTimeout));
            _requests.setPoolStub(new CellStub(this));
                _requests.setPoolInformationBase(_pools);
            _requests.setSuccessSink(uri -> _executor.execute(() -> onSuccess(uri)));
            _requests.setFailureSink(uri -> _executor.execute(() -> onFailure(uri)));
            addMessageListener(_requests);
            addCommandListener(_requests);
        }

        addMessageListener(_pools);
        addCommandListener(_pools);

        _poolStub = new CellStub(this, null, _replyTimeout, _replyTimeoutUnit);
    }

    @Override
    protected void started()
    {
        _cleanerTask =
                _executor.scheduleWithFixedDelay(this,
                                                 _refreshInterval,
                                                 _refreshInterval,
                                                 _refreshIntervalUnit);
    }

    void dbInit(String jdbcUrl, String user, String pass )
        throws ClassNotFoundException
    {
        if (jdbcUrl == null || user == null || pass == null) {
            throw new IllegalArgumentException("Not enough arguments to Init SQL database");
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(jdbcUrl);
        config.setUsername(user);
        config.setPassword(pass);
        config.setMinimumIdle(1);
        config.setMaximumPoolSize(10);

        _dataSource = new AlarmEnabledDataSource(jdbcUrl,
                                                 ChimeraCleaner.class.getSimpleName(),
                                                 new HikariDataSource(config));
        _db = new JdbcTemplate(_dataSource);

        _log.info("Database connection with jdbcUrl={}; user={}",
                  jdbcUrl, user);
    }

    @Override
    protected void stopped()
    {
        if (_requests != null) {
            _requests.shutdown();
        }
        if (_executor != null) {
            _executor.shutdownNow();
        }
        if (_dataSource != null) {
            try {
                _dataSource.close();
            } catch (IOException e) {
                _log.debug("Failed to shutdown database connection pool: {}", e.getMessage());
            }
        }
    }

    @Override
    public void run() {

        try {
            _log.info("*********NEW_RUN*************");

            _log.debug("INFO: Refresh Interval : {} {}", _refreshInterval, _refreshIntervalUnit);
            _log.debug("INFO: Number of files processed at once: {}", _processAtOnce);

            // get list of pool names from the trash_table
            List<String> poolList = getPoolList();

            _log.debug("List of Pools from the trash-table : {}", poolList);

            // if there are some pools in the blackPoolList (i.e.,
            //pools that are down/do not exist), extract them from the
            //poolList
            if (!_poolsBlackList.isEmpty()) {
                _log.debug("{} pools are currently blacklisted.", _poolsBlackList.size());

                for (Map.Entry<String, Long> blackListEntry : _poolsBlackList.entrySet()) {
                    String poolName = blackListEntry.getKey();
                    long valueTime = blackListEntry.getValue();

                    //check, if it is time to remove pool from the black list
                    if ((valueTime != 0)
                        && (_recoverTimer > 0)
                        && ((System.currentTimeMillis() - valueTime) > _recoverTimerUnit.toMillis(_recoverTimer))) {
                        _poolsBlackList.remove(poolName);
                        _log.debug("Removed the following pool from the black list: {}", poolName);}
                }

                poolList.removeAll(_poolsBlackList.keySet());
            }

            if (!poolList.isEmpty()) {
                _log.debug("The following pools are cleaned: {}", poolList);
                runDelete(poolList);
            }

            //HSM part
            if (_hsmCleanerEnabled){
                runDeleteHSM();
                runNotification();
            }
        } catch (DataAccessException e) {
            _log.error("Database failure: {}", e.getMessage());
        } catch (InterruptedException e) {
            _log.info("Cleaner was interrupted");
        } catch (RuntimeException e) {
            _log.error("Bug detected" , e);
        }
    }

    /**
     * Returns a list of dinstinctpool names from the trash-table.
     *
     * @return list of pool names
     */
    List<String> getPoolList()
    {
        return _db.query("SELECT DISTINCT ilocation FROM t_locationinfo_trash WHERE itype=1",
                         (rs, rowNum) -> rs.getString("ilocation"));
    }

    /**
     * Delete entries from the trash-table.
     * Pool name and the file names are input parameters.
     *
     * @param poolname name of the pool
     * @param filelist file list for this pool
     *
     */
    void removeFiles(final String poolname, final List<String> filelist)
    {
        _db.batchUpdate("DELETE FROM t_locationinfo_trash WHERE ilocation=? AND ipnfsid=? AND itype=1",
                        new BatchPreparedStatementSetter()
                        {
                            @Override
                            public int getBatchSize()
                            {
                                return filelist.size();
                            }

                            @Override
                            public void setValues(PreparedStatement ps, int i)
                                    throws SQLException
                            {
                                ps.setString(1, poolname);
                                ps.setString(2, filelist.get(i));
                            }
                        });
    }

    /**
     * runDelete
     * Delete files on each pool from the poolList.
     *
     * @param poolList list of pools
     * @throws InterruptedException
     */
    private void runDelete(List<String> poolList)
        throws InterruptedException
    {
        for (String pool: poolList) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Cleaner interrupted");
            }

            runDelete(pool);

            // Notify other components that we are done deleting
            runNotification();
        }
    }

    private void runDelete(String pool) throws InterruptedException
    {
        _log.info("runDelete(): Now processing pool {}", pool);
        if (!_poolsBlackList.containsKey(pool)) {
            try {
                cleanPoolComplete(pool);
            } catch (NoRouteToCellException | CacheException e) {
                _log.warn("Failed to remove files from {}: {}", pool, e.getMessage());
            }
        }
    }

    /**
     * sendRemoveToPoolCleaner
     * removes set of files from the pool
     *
     * @param poolName name of the pool
     * @param removeList list of files to be removed from this pool
     * @throws InterruptedException
     */

    private void sendRemoveToPoolCleaner(String poolName, List<String> removeList)
            throws InterruptedException, CacheException, NoRouteToCellException
    {
        _log.trace("sendRemoveToPoolCleaner: poolName={} removeList={}", poolName, removeList);

        try {
            PoolRemoveFilesMessage msg =
                    CellStub.get(_poolStub.send(new CellPath(poolName),
                                                new PoolRemoveFilesMessage(poolName, removeList)));
            if (msg.getReturnCode() == 0) {
                removeFiles(poolName, removeList);
            } else if (msg.getReturnCode() == 1 && msg.getErrorObject() instanceof String[]) {
                Set<String> notRemoved =
                        new HashSet<>(Arrays.asList((String[]) msg.getErrorObject()));
                List<String> removed = new ArrayList<>(removeList);
                removed.removeAll(notRemoved);
                removeFiles(poolName, removed);
            } else {
                throw CacheExceptionFactory.exceptionOf(msg);
            }
        } catch (NoRouteToCellException | CacheException e) {
            _poolsBlackList.put(poolName, System.currentTimeMillis());
            throw e;
        }
    }

    public void messageArrived(NoRouteToCellException e)
    {
        _log.warn(e.getMessage());
    }

    public void messageArrived(PoolManagerPoolUpMessage poolUpMessage)
    {
        String poolName = poolUpMessage.getPoolName();
        if (poolUpMessage.getPoolMode().isEnabled() ) {
            _poolsBlackList.remove(poolName);
        } else {
            _poolsBlackList.put(poolName, System.currentTimeMillis());
        }
    }

    private void runNotification()
            throws InterruptedException
    {
        final String QUERY =
                "SELECT ipnfsid FROM t_locationinfo_trash t1 " +
                "WHERE itype=2 AND NOT EXISTS (SELECT 1 FROM t_locationinfo_trash t2 WHERE t2.ipnfsid=t1.ipnfsid AND t2.itype <> 2)";
        for (String id : _db.queryForList(QUERY, String.class)) {
            try {
                sendDeleteNotifications(new PnfsId(id)).get();
                _db.update("DELETE FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=2", id);
            } catch (ExecutionException e) {
                _log.warn(e.getCause().getMessage());
            }
        }
    }

    private ListenableFuture<List<PnfsDeleteEntryNotificationMessage>> sendDeleteNotifications(PnfsId pnfsId)
    {
        BiFunction<CellPath, Exception, CacheException> failureFor =
                (path, e) -> new CacheException("Failed to notify " + path + " about deletion of " + pnfsId + ": " + e.getMessage(), e);
        return allAsList(
                Arrays.stream(_deleteNotificationTargets)
                        .map(a -> Futures.catchingAsync(_notificationStub.send(a, new PnfsDeleteEntryNotificationMessage(pnfsId)),
                                                        Exception.class, e -> immediateFailedFuture(failureFor.apply(a, e))))
                        .collect(toList()));
    }

    /**
     * cleanPoolComplete
     * delete all files from the pool 'poolName' found in the trash-table for this pool
     *
     * @param poolName name of the pool
     */
    void cleanPoolComplete(final String poolName) throws InterruptedException, CacheException, NoRouteToCellException
    {
        _log.trace("CleanPoolComplete(): poolname={}", poolName);

        try {
            List<String> files = new ArrayList<>(_processAtOnce);
            _db.query("SELECT ipnfsid FROM t_locationinfo_trash WHERE ilocation=? AND itype=1 ORDER BY iatime",
                      rs -> {
                          try {
                              files.add(rs.getString("ipnfsid"));
                              if (files.size() >= _processAtOnce || rs.isLast()) {
                                  sendRemoveToPoolCleaner(poolName, files);
                                  files.clear();
                              }
                          } catch (InterruptedException | CacheException | NoRouteToCellException e) {
                              throw new UncheckedExecutionException(e);
                          }
                      },
                      poolName);
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), CacheException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            throw Throwables.propagate(e.getCause());
        }
    }

    /**
     * Delete files stored on tape (HSM).
     */
    private void runDeleteHSM()
    {
        _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE itype=0",
                  rs -> {
                      try {
                          URI uri = new URI(rs.getString("ilocation"));
                          _log.debug("Submitting a request to delete a file: {}", uri);
                          _requests.submit(uri);
                      } catch (URISyntaxException e) {
                          throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                      }
                  });
    }

    ////////////////////////////////////////////////////////////////////////////
    @Command(name = "rundelete",
            hint = "run cleaner",
            description = "Delete all files found in the trash-table irrespective of the pool.")
    public class RundeleteCommand implements Callable<String>
    {
        @Override
        public String call() throws InterruptedException
        {
            runDelete(getPoolList());
            return "";
        }
    }

    @Command(name = "show info",
            hint = "get cleaner service information")
    public class ShowInfoCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            StringBuilder sb = new StringBuilder();

            sb.append("Refresh Interval: ").append(_refreshInterval).append(" ").append(_refreshIntervalUnit).append("\n");
            sb.append("Reply Timeout: ").append(_replyTimeout).append(" ").append(_replyTimeoutUnit).append("\n");
            sb.append("Recover Timer: ").append(_recoverTimer).append(" ").append(_recoverTimerUnit).append("\n");
            sb.append("Number of files processed at once: ").append(_processAtOnce);
            if ( _hsmCleanerEnabled ) {
                sb.append("\n HSM Cleaner enabled. Info : \n");
                sb.append("Timeout for cleaning requests to HSM-pools: ").append(_hsmTimeout).append(" ").append(_hsmTimeoutUnit).append("\n");
                sb.append("Maximal number of concurrent requests to a single HSM : ").append(_hsmCleanerRequest);
            } else {
                sb.append("\n HSM Cleaner disabled.");
            }
            sb.append("\nDelete notification targets: ").append(Arrays.toString(_deleteNotificationTargets));
            return sb.toString();
        }
    }

    @Command(name = "ls blacklist",
            hint = "list blacklisted pools",
            description = "Show a list of blacklisted pools. Blacklisted pool is a " +
                    "pool that is down or do not exist.")
    public class LsBlacklistCommand implements Callable<String>
    {
        @Override
        public String call()
        {
            StringBuilder sb = new StringBuilder();
            for (String pool : _poolsBlackList.keySet()) {
                sb.append(pool).append("\n");
            }
            return sb.toString();
        }
    }

    @Command(name = "remove from blacklist",
            hint = "remove a pool from the blacklist")
    public class RemoveFromBlacklistCommand implements Callable<String>
    {
        @Argument(usage = "The name of the pool to be removed from the blacklist.")
        String poolName;

        @Override
        public String call()
        {
            if (_poolsBlackList.remove(poolName) != null) {
                return "Pool " + poolName + " is removed from the Black List ";
            }
            return "Pool " + poolName + " was not found in the Black List ";
        }
    }

    public static final String hh_clean_file =
        "<pnfsID> # clean this file (file will be deleted from DISK)";
    public String ac_clean_file_$_1(Args args) throws InterruptedException, CacheException, NoRouteToCellException
    {
        try {
            String pnfsid = args.argv(0);
            List<String> removeFile = Collections.singletonList(pnfsid);
            _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=1 ORDER BY iatime",
                      rs -> {
                          String pool = rs.getString("ilocation");
                          try {
                              sendRemoveToPoolCleaner(pool, removeFile);
                          } catch (CacheException | InterruptedException | NoRouteToCellException e) {
                              throw new UncheckedExecutionException(e);
                         }
                      },
                      pnfsid);
        } catch (UncheckedExecutionException e) {
            Throwables.propagateIfInstanceOf(e.getCause(), InterruptedException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), CacheException.class);
            Throwables.propagateIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            throw Throwables.propagate(e.getCause());
        }
        return "";
    }

    public static final String hh_clean_pool = "<poolName> # clean this pool ";
    public String ac_clean_pool_$_1(Args args) throws CacheException, InterruptedException, NoRouteToCellException
    {
        String poolName = args.argv(0);
        if (_poolsBlackList.containsKey(poolName)) {
            return "This pool is not available for the moment and therefore will not be cleaned.";
        }
        cleanPoolComplete(poolName);
        return "";
    }

    public static final String hh_set_refresh = "[<refreshTimeInSeconds>]";
    public static final String fh_set_refresh =
        "Alters refresh rate and triggers a new run. Maximum rate is every 5 seconds.";
    public String ac_set_refresh_$_0_1(Args args)
    {
        if (args.argc() > 0) {
            long newRefresh = Long.parseLong(args.argv(0));
            if (newRefresh < 5) {
                throw new IllegalArgumentException("Time must be greater than 5 seconds");
            }

            _refreshInterval = newRefresh;
            _refreshIntervalUnit = SECONDS;

            if (_cleanerTask != null) {
                _cleanerTask.cancel(true);
            }
            _cleanerTask =
                _executor.scheduleWithFixedDelay(this,
                                                 0,
                                                 _refreshInterval,
                                                 _refreshIntervalUnit);
        }
        return "Refresh set to " + _refreshInterval + " " + _refreshIntervalUnit;
    }

    public static final String hh_set_processedAtOnce = "<processedAtOnce> # max number of files sent to pool for processing at once ";
    public String ac_set_processedAtOnce_$_1(Args args)
    {
        if (args.argc() > 0) {
            int processAtOnce = Integer.parseInt(args.argv(0));
            if (processAtOnce <= 0) {
                throw new IllegalArgumentException("Number of files must be greater than 0 ");
            }
            _processAtOnce = processAtOnce;
        }

        return "Number of files processed at once set to " + _processAtOnce;
    }

    /////  HSM admin commands /////

    public static final String hh_rundelete_hsm = " # run HSM Cleaner";
    public String ac_rundelete_hsm(Args args)
    {
        if (!_hsmCleanerEnabled) {
            return "HSM Cleaner is disabled.";
        }

        runDeleteHSM();
        return "";
    }

    //explicitly clean HSM-file
    public static final String hh_clean_file_hsm =
        "<pnfsID> # clean this file on HSM (file will be deleted from HSM)";
    public String ac_clean_file_hsm_$_1(Args args)
    {
        if (!_hsmCleanerEnabled) {
            return "HSM Cleaner is disabled.";
        }

        _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=0 ORDER BY iatime",
                  rs -> {
                      try {
                          _requests.submit(new URI(rs.getString("ilocation")));
                      } catch (URISyntaxException e) {
                          throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                      }
                  },
                  args.argv(0));

        return "";
    }

    public static final String hh_hsm_set_MaxFilesPerRequest = "<number> # maximal number of concurrent requests to a single HSM";

    public String ac_hsm_set_MaxFilesPerRequest_$_1(Args args) throws NumberFormatException
    {
        if (!_hsmCleanerEnabled) {
            return "HSM Cleaner is disabled.";
        }
        if (args.argc() > 0) {
            int maxFilesPerRequest = Integer.parseInt(args.argv(0));
            if (maxFilesPerRequest == 0) {
                throw new
                        IllegalArgumentException("The number must be greater than 0 ");
            }
            _hsmCleanerRequest = maxFilesPerRequest;
        }

        return "Maximal number of concurrent requests to a single HSM is set to " + _hsmCleanerRequest;
    }

    public static final String hh_hsm_set_TimeOut = "<seconds> # cleaning request timeout in seconds (for HSM-pools)";

    public String ac_hsm_set_TimeOut_$_1(Args args) throws NumberFormatException
    {
        if (!_hsmCleanerEnabled) {
            return "HSM Cleaner is disabled.";
        }
        if (args.argc() > 0) {
            _hsmTimeout = Long.parseLong(args.argv(0));
            _hsmTimeoutUnit = SECONDS;
        }
        return "Timeout for cleaning requests to HSM-pools is set to " + _hsmTimeout + " " + _hsmTimeoutUnit;
    }

    /**
     * Called when a file was successfully deleted from the HSM.
     */
    protected void onSuccess(URI uri)
    {
        try {
            _log.debug("HSM-ChimeraCleaner: remove entries from the trash-table. ilocation={}", uri);
            _db.update("DELETE FROM t_locationinfo_trash WHERE ilocation=? AND itype=0", uri.toString());
        } catch (DataAccessException e) {
            _log.error("Error when deleting from the trash-table: {}", e.getMessage());
        }
    }

    /**
     * Called when a file could not be deleted from the HSM.
     */
    protected void onFailure(URI uri)
    {
        _log.info("Failed to delete a file {} from HSM. Will try again later.", uri);
    }
}
