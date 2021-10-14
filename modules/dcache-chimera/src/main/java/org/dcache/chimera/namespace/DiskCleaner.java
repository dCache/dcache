package org.dcache.chimera.namespace;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static dmg.util.CommandException.checkCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.dcache.cells.HAServiceLeadershipManager.HA_NOT_LEADER_MSG;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.UncheckedExecutionException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.io.PrintWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import org.dcache.cells.CellStub;
import org.dcache.util.CacheExceptionFactory;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;

/**
 * @author Irina Kozlova
 * @version 22 Oct 2007
 * <p>
 * DiskCleaner: takes file names from the table public.t_locationinfo_trash, removes them from the
 * corresponding pools and then from the table as well.
 * @since 1.8
 */
public class DiskCleaner extends AbstractCleaner implements CellCommandListener, CellInfoProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(DiskCleaner.class);

    private final ConcurrentHashMap<String, Long> _poolsBlackList = new ConcurrentHashMap<>();

    private ScheduledFuture<?> _cleanerTask;

    private long _recoverTimer;
    private TimeUnit _recoverTimerUnit;
    private int _processAtOnce;
    private CellStub _notificationStub;

    @Required
    public void setRecoverTimer(long recoverTimer) {
        _recoverTimer = recoverTimer;
    }

    @Required
    public void setRecoverTimerUnit(TimeUnit recoverTimerUnit) {
        _recoverTimerUnit = recoverTimerUnit;
    }

    @Required
    public void setProcessAtOnce(int processAtOnce) {
        _processAtOnce = processAtOnce;
    }

    @Required
    public void setNotificationStub(CellStub stub) {
        _notificationStub = stub;
    }

    /**
     * runDelete Delete files on each pool from the poolList.
     *
     * @throws InterruptedException
     */
    @Override
    protected void runDelete() throws InterruptedException {
        try {
            LOGGER.info("*********NEW_RUN*************");

            LOGGER.debug("INFO: Refresh Interval : {} {}", _refreshInterval, _refreshIntervalUnit);
            LOGGER.debug("INFO: Number of files processed at once: {}", _processAtOnce);

            // get list of pool names from the trash_table
            List<String> poolList = getPoolList();

            LOGGER.debug("List of Pools from the trash-table : {}", poolList);

            // if there are some pools in the blackPoolList (i.e.,
            // pools that are down/do not exist), extract them from the
            // poolList
            if (!_poolsBlackList.isEmpty()) {
                LOGGER.debug("{} pools are currently blacklisted.", _poolsBlackList.size());

                for (Map.Entry<String, Long> blackListEntry : _poolsBlackList.entrySet()) {
                    String poolName = blackListEntry.getKey();
                    long valueTime = blackListEntry.getValue();

                    // check, if it is time to remove pool from the black list
                    if ((valueTime != 0)
                          && (_recoverTimer > 0)
                          && ((System.currentTimeMillis() - valueTime) > _recoverTimerUnit.toMillis(
                          _recoverTimer))) {
                        _poolsBlackList.remove(poolName);
                        LOGGER.debug("Removed the following pool from the black list: {}",
                              poolName);
                    }
                }

                poolList.removeAll(_poolsBlackList.keySet());
            }

            if (!poolList.isEmpty()) {
                LOGGER.debug("The following pools are cleaned: {}", poolList);
                runDelete(poolList);
            }
        } catch (DataAccessException e) {
            LOGGER.error("Database failure: {}", e.getMessage());
        } catch (InterruptedException e) {
            LOGGER.info("Cleaner was interrupted");
        } catch (RuntimeException e) {
            LOGGER.error("Bug detected", e);
        }
    }

    /**
     * runDelete Delete files on each pool from the poolList.
     *
     * @param poolList list of pools
     * @throws InterruptedException
     */
    private void runDelete(List<String> poolList) throws InterruptedException {
        boolean runAsync = _executor.getCorePoolSize() > 1;
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String pool : poolList) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Cleaner interrupted");
            }

            if (runAsync) {
                CompletableFuture<Void> cf = CompletableFuture.runAsync(
                      () -> {
                          runDelete(pool);
                          runNotification();
                          LOGGER.info("Finished deleting from pool {}", pool);
                      }, _executor);
                futures.add(cf);
            } else {
                runDelete(pool);
                runNotification();
                LOGGER.info("Finished deleting from pool {}", pool);
            }
        }
        if (runAsync) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        }
    }

    private void runDelete(String pool) {
        LOGGER.info("runDelete(): Now processing pool {}", pool);
        if (!_poolsBlackList.containsKey(pool)) {
            try {
                cleanPoolComplete(pool);
            } catch (NoRouteToCellException | CacheException e) {
                LOGGER.warn("Failed to remove files from {}: {}", pool, e.getMessage());
            } catch (InterruptedException e) {
                LOGGER.warn("Cleaner was interrupted while deleting files from pool {}: {}", pool,
                      e.getMessage());
            }
        }
    }

    /**
     * Returns a list of dinstinct pool names from the trash-table.
     *
     * @return list of pool names
     */
    List<String> getPoolList() {
        return _db.query("SELECT DISTINCT ilocation FROM t_locationinfo_trash WHERE itype=1",
              (rs, rowNum) -> rs.getString("ilocation"));
    }

    /**
     * Delete entries from the trash-table. Pool name and the file names are input parameters.
     *
     * @param poolname name of the pool
     * @param filelist file list for this pool
     */
    void removeFiles(final String poolname, final List<String> filelist) {
        _db.batchUpdate(
              "DELETE FROM t_locationinfo_trash WHERE ilocation=? AND ipnfsid=? AND itype=1",
              new BatchPreparedStatementSetter() {
                  @Override
                  public int getBatchSize() {
                      return filelist.size();
                  }

                  @Override
                  public void setValues(PreparedStatement ps, int i) throws SQLException {
                      ps.setString(1, poolname);
                      ps.setString(2, filelist.get(i));
                  }
              });
    }

    /**
     * sendRemoveToPoolCleaner removes set of files from the pool
     *
     * @param poolName   name of the pool
     * @param removeList list of files to be removed from this pool
     * @return number of successful removes
     * @throws InterruptedException
     */
    private int sendRemoveToPoolCleaner(String poolName, List<String> removeList)
          throws InterruptedException, CacheException, NoRouteToCellException {
        LOGGER.trace("sendRemoveToPoolCleaner: poolName={} removeList={}", poolName, removeList);

        try {
            PoolRemoveFilesMessage msg =
                  CellStub.get(_poolStub.send(new CellPath(poolName),
                        new PoolRemoveFilesMessage(poolName, removeList)));
            if (msg.getReturnCode() == 0) {
                removeFiles(poolName, removeList);
                return removeList.size();
            } else if (msg.getReturnCode() == 1 && msg.getErrorObject() instanceof String[]) {
                Set<String> notRemoved =
                      new HashSet<>(Arrays.asList((String[]) msg.getErrorObject()));
                List<String> removed = new ArrayList<>(removeList);
                removed.removeAll(notRemoved);
                removeFiles(poolName, removed);
                return removed.size();
            } else {
                throw CacheExceptionFactory.exceptionOf(msg);
            }
        } catch (NoRouteToCellException | CacheException e) {
            _poolsBlackList.put(poolName, System.currentTimeMillis());
            throw e;
        }
    }

    public void messageArrived(NoRouteToCellException e) {
        LOGGER.warn(e.getMessage());
    }

    public void messageArrived(PoolManagerPoolUpMessage poolUpMessage) {
        String poolName = poolUpMessage.getPoolName();
        if (poolUpMessage.getPoolMode().isEnabled()) {
            _poolsBlackList.remove(poolName);
        } else {
            _poolsBlackList.put(poolName, System.currentTimeMillis());
        }
    }

    private void runNotification() {
        final String QUERY =
              "SELECT ipnfsid FROM t_locationinfo_trash t1 " +
                    "WHERE itype=2 AND NOT EXISTS (SELECT 1 FROM t_locationinfo_trash t2 WHERE t2.ipnfsid=t1.ipnfsid AND t2.itype <> 2)";
        for (String id : _db.queryForList(QUERY, String.class)) {
            try {
                sendDeleteNotifications(new PnfsId(id)).get();
                _db.update("DELETE FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=2", id);
            } catch (ExecutionException e) {
                LOGGER.warn(e.getCause().getMessage());
            } catch (InterruptedException e) {
                LOGGER.warn("Cleaner interruption: {}", e.getMessage());
            }
        }
    }

    private ListenableFuture<List<PnfsDeleteEntryNotificationMessage>> sendDeleteNotifications(
          PnfsId pnfsId) {
        BiFunction<CellPath, Exception, CacheException> failureFor =
              (path, e) -> new CacheException(
                    "Failed to notify " + path + " about deletion of " + pnfsId + ": "
                          + e.getMessage(), e);
        return allAsList(
              Arrays.stream(_deleteNotificationTargets)
                    .map(a -> Futures.catchingAsync(
                          _notificationStub.send(a, new PnfsDeleteEntryNotificationMessage(pnfsId)),
                          Exception.class, e -> immediateFailedFuture(failureFor.apply(a, e))))
                    .collect(toList()));
    }

    /**
     * cleanPoolComplete delete all files from the pool 'poolName' found in the trash-table for this
     * pool
     *
     * @param poolName name of the pool
     */
    void cleanPoolComplete(final String poolName)
          throws InterruptedException, CacheException, NoRouteToCellException {
        LOGGER.trace("CleanPoolComplete(): poolname={}", poolName);

        try {
            List<String> files = new ArrayList<>(_processAtOnce);
            Timestamp graceTime = Timestamp.from(
                  Instant.now().minusSeconds(_gracePeriod.getSeconds()));

            String lastSeenIpnfsid = "";
            int removed = 0;
            while (true) {
                _db.query(
                      "SELECT ipnfsid FROM t_locationinfo_trash WHERE ilocation=? AND itype=1 AND ictime<? AND ipnfsid>? ORDER BY ipnfsid ASC LIMIT ?",
                      rs -> {
                          files.add(rs.getString("ipnfsid"));
                      },
                      poolName,
                      graceTime,
                      lastSeenIpnfsid,
                      _processAtOnce
                );
                if (files.isEmpty()) {
                    break;
                }
                lastSeenIpnfsid = files.get(files.size() - 1);
                removed += sendRemoveToPoolCleaner(poolName, files);
                files.clear();
            }
            LOGGER.info("Removed {} files from pool {} deleted before {}", removed, poolName,
                  graceTime);
        } catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), InterruptedException.class);
            throwIfInstanceOf(e.getCause(), CacheException.class);
            throwIfInstanceOf(e.getCause(), NoRouteToCellException.class);
            throw new RuntimeException(e.getCause());
        }
    }

    ////////////////////////////////////////////////////////////////////////////

    @Command(name = "rundelete",
          hint = "run cleaner",
          description = "Delete all files found in the trash-table irrespective of the pool.")
    public class RundeleteCommand implements Callable<String> {

        @Override
        public String call() throws InterruptedException, CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            runDelete(getPoolList());
            return "";
        }
    }

    @Command(name = "ls blacklist",
          hint = "list blacklisted pools",
          description = "Show a list of blacklisted pools. A blacklisted pool is a pool that is down or does not exist.")
    public class LsBlacklistCommand implements Callable<String> {

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            for (String pool : _poolsBlackList.keySet()) {
                sb.append(pool).append("\n");
            }
            return sb.toString();
        }
    }

    @Command(name = "remove from blacklist",
          hint = "remove a pool from the blacklist")
    public class RemoveFromBlacklistCommand implements Callable<String> {

        @Argument(usage = "The name of the pool to be removed from the blacklist.")
        String poolName;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (_poolsBlackList.remove(poolName) != null) {
                return "Pool " + poolName + " is removed from the Black List ";
            }
            return "Pool " + poolName + " was not found in the Black List ";
        }
    }

    @Command(name = "clean file",
          hint = "clean this file (file will be deleted from DISK)")
    public class CleanFileCommand implements Callable<String> {

        @Argument(usage = "pnfsid of the file to clean")
        String pnfsid;

        @Override
        public String call()
              throws InterruptedException, CacheException, NoRouteToCellException, CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            try {
                List<String> removeFile = Collections.singletonList(pnfsid);
                _db.query(
                      "SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=1 ORDER BY iatime",
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
                throwIfInstanceOf(e.getCause(), InterruptedException.class);
                throwIfInstanceOf(e.getCause(), CacheException.class);
                throwIfInstanceOf(e.getCause(), NoRouteToCellException.class);
                throw new RuntimeException(e.getCause());
            }
            return "";
        }
    }

    @Command(name = "clean pool",
          hint = "clean this pool")
    public class CleanPoolCommand implements Callable<String> {

        @Argument(usage = "name of the pool to be cleaned")
        String poolName;

        @Override
        public String call()
              throws CacheException, InterruptedException, NoRouteToCellException, CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (_poolsBlackList.containsKey(poolName)) {
                return "This pool is not available for the moment and therefore will not be cleaned.";
            }
            runDelete(Arrays.asList(new String[]{poolName}));
            return "";
        }
    }

    @Command(name = "set refresh",
          hint = "Alters refresh rate and triggers a new run. Minimum rate is every 5 seconds." +
                "If no time is provided, the old one is kept.")
    public class SetRefreshCommand implements Callable<String> {

        @Argument(required = false, usage = "refresh time in seconds")
        Long refreshInterval;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (refreshInterval == null) {
                return "Refresh interval unchanged: " + _refreshInterval + " "
                      + _refreshIntervalUnit;
            }
            if (refreshInterval < 5) {
                throw new IllegalArgumentException("Time must be greater than 5 seconds");
            }

            setRefreshInterval(refreshInterval);
            setRecoverTimerUnit(SECONDS);

            if (_cleanerTask != null) {
                _cleanerTask.cancel(true);
            }
            _cleanerTask = _executor.scheduleWithFixedDelay(() -> {
                try {
                    runDelete();
                } catch (InterruptedException e) {
                    LOGGER.info("Cleaner was interrupted");
                }
            }, _refreshInterval, _refreshInterval, _refreshIntervalUnit);
            return "Refresh set to " + _refreshInterval + " " + _refreshIntervalUnit;
        }
    }

    @Command(name = "set processedAtOnce",
          hint = "Changes the number of files sent to a pool for processing at once.")
    public class SetProcessedAtOnceCommand implements Callable<String> {

        @Argument(usage = "max number of files sent to a pool for processing at once")
        int processAtOnce;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (processAtOnce <= 0) {
                throw new IllegalArgumentException("Number of files must be greater than 0 ");
            }
            _processAtOnce = processAtOnce;
            return "Number of files processed at once set to " + _processAtOnce;
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("Refresh Interval: %s\n", _refreshInterval);
        pw.printf("Refresh Interval Unit: %s\n", _refreshIntervalUnit);
        pw.printf("Cleanup grace period: %s\n", TimeUtils.describe(_gracePeriod).orElse("-"));
        pw.printf("Reply Timeout:  %d\n", _poolStub.getTimeout());
        pw.printf("Number of files processed at once:  %d\n", _processAtOnce);
        pw.printf("Delete notification targets:  %s\n",
              Arrays.toString(_deleteNotificationTargets));
        int threadPoolSize = _executor.getCorePoolSize();
        pw.printf("Cleaning up to %d pools in parallel\n",
              threadPoolSize == 1 ? 1 : threadPoolSize - 1);
    }

}
