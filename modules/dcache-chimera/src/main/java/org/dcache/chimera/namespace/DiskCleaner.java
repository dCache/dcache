package org.dcache.chimera.namespace;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.util.concurrent.Futures.allAsList;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static dmg.util.CommandException.checkCommand;
import static java.util.stream.Collectors.toList;
import static org.dcache.cells.HAServiceLeadershipManager.HA_NOT_LEADER_MSG;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.UncheckedExecutionException;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
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

    private final ConcurrentHashMap<String, Long> _poolsBeingCleaned = new ConcurrentHashMap<>();

    protected CellPath[] _deleteNotificationTargets;
    private CellStub _notificationStub;

    private int _processAtOnce;

    @Required
    public void setProcessAtOnce(int processAtOnce) {
        _processAtOnce = processAtOnce;
    }

    @Required
    public void setNotificationStub(CellStub stub) {
        _notificationStub = stub;
    }


    @Required
    public void setReportRemove(String[] reportRemove) {
        _deleteNotificationTargets = Arrays.stream(reportRemove)
              .filter(t -> !t.isEmpty())
              .map(CellPath::new)
              .toArray(CellPath[]::new);
    }

    /**
     * runDelete Delete files on each pool from the poolList.
     */
    @Override
    protected void runDelete() {
        if (!_hasHaLeadership) {
            LOGGER.warn("Delete run triggered despite not having leadership. "
                  + "We assume this is a transient problem.");
            return;
        }
        try {
            LOGGER.info("New run...");

            final List<String> poolList = getPoolList();
            if (poolList.isEmpty()) {
                return;
            }
            List<String> pools = _pools.getPools().stream().filter(p -> !p.isDisabled())
                  .map(PoolInformation::getName).filter(poolList::contains)
                  .collect(Collectors.toList());

            if (!pools.isEmpty()) {
                LOGGER.debug("The following pools are cleaned: {}", pools);
                runDelete(pools);
            } else {
                LOGGER.info("No pools available for cleaning.");
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
        // Expensive operation, thus triggered once per runDelete rather than per pool
        deleteInodeEntries();

        boolean runAsync = _executor.getCorePoolSize() > 1;
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (String pool : poolList) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Cleaner interrupted");
            }

            if (runAsync) {
                CompletableFuture<Void> cf = CompletableFuture.runAsync(
                      () -> {
                          _poolsBeingCleaned.put(pool, System.currentTimeMillis());
                          try {
                              runDelete(pool);
                          } finally {
                              _poolsBeingCleaned.remove(pool);
                              LOGGER.info("Finished deleting from pool {}", pool);
                          }
                      }, _executor);
                futures.add(cf);
            } else {
                runDelete(pool);
                LOGGER.info("Finished deleting from pool {}", pool);
            }
        }
        if (runAsync) {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[futures.size()])).join();
        }
    }

    private void runDelete(String pool) {
        if (_pools.isPoolAvailable(pool)) {
            try {
                cleanPoolComplete(pool);
            } catch (NoRouteToCellException | CacheException e) {
                LOGGER.warn("failed to remove files from pool {}: {}", pool,
                      e.getMessage());
            } catch (InterruptedException e) {
                LOGGER.warn("cleaner was interrupted while deleting files from pool {}: {}", pool,
                      e.getMessage());
            }
        }
    }

    /**
     * Returns a list of distinct pool names from the trash-table.
     *
     * @return list of pool names
     */
    List<String> getPoolList() {
        return _db.query("SELECT DISTINCT ilocation FROM t_locationinfo_trash WHERE itype=1",
              (rs, rowNum) -> rs.getString("ilocation"));
    }

    /**
     * Delete all entries from the trash-table for the given pool.
     *
     * @param poolname name of the pool
     */
    int forgetTargetsOnPool(final String poolname) {
        return _db.update("DELETE FROM t_locationinfo_trash WHERE ilocation=? AND itype=1",
              poolname);
    }

    /**
     * Delete entries from the trash-table. Pool name and the file names are input parameters.
     *
     * @param poolname name of the pool
     * @param filelist file list for this pool
     */
    void removeFiles(final String poolname, final List<String> filelist) {
        if (filelist == null || filelist.isEmpty()) {
            LOGGER.info("unexpected empty delete file list.");
            return;
        }
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
        LOGGER.trace("sending {} remove targets to pool {}", removeList.size(), poolName);

        try {
            PoolRemoveFilesMessage msg = CellStub.get(_poolStub.send(new CellPath(poolName),
                  new PoolRemoveFilesMessage(poolName, removeList)));
            if (msg.getReturnCode() == 0) {
                removeFiles(poolName, List.copyOf(removeList));
                return removeList.size();
            } else if (msg.getReturnCode() == 1 && msg.getErrorObject() instanceof String[]) {
                Set<String> notRemoved = new HashSet<>(
                      Arrays.asList((String[]) msg.getErrorObject()));
                List<String> removed = new ArrayList<>(removeList);
                removed.removeAll(notRemoved);
                removeFiles(poolName, List.copyOf(removed));
                return removed.size();
            } else {
                throw CacheExceptionFactory.exceptionOf(msg);
            }
        } catch (NoRouteToCellException | CacheException e) {
            LOGGER.info("blacklisting pool {}", poolName);
            _pools.remove(poolName);
            throw e;
        }
    }

    public void messageArrived(NoRouteToCellException e) {
        LOGGER.warn(e.getMessage());
    }

    /**
     * Deletes all pnfsid entries from the trash table that are of itype=2 (inode) and for which
     * there are no other trash table entries on disk or hsm (types 0 and 1). As this is the final
     * delete operation for a pnfsid, it also sends a delete notification for each.
     */
    private void deleteInodeEntries() {
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
                LOGGER.warn("cleaner interruption: {}", e.getMessage());
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
                          Exception.class, e -> immediateFailedFuture(failureFor.apply(a, e)),
                          MoreExecutors.directExecutor()))
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
        LOGGER.info("processing pool {}", poolName);
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
            if (!_pools.isPoolAvailable(poolName)) {
                return "This pool is not available for the moment and therefore will not be cleaned.";
            }
            runDelete(Arrays.asList(poolName));
            return "";
        }
    }

    @Command(name = "forget pool",
          hint = "Let cleaner forget the pool",
          description = "Forget this pool: remove all trash table entries for this pool, which no longer exists.")
    public class ForgetPoolCommand implements Callable<String> {

        @Argument(usage = "name of the pool to be forgotten")
        String poolName;

        @Option(name = "f", usage = "force forget pool, even when it is not currently blacklisted")
        boolean force;

        @Override
        public String call()
              throws CacheException, InterruptedException, NoRouteToCellException, CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (!force && _pools.isPoolAvailable(poolName)) {
                return
                      "This pool is not currently blacklisted due to being unavailable. If you really "
                            + "want to forget the pool nonetheless, please use the 'force' option.";
            }
            int removed = forgetTargetsOnPool(poolName);
            String info = "Removed " + removed + " delete targets from pool " + poolName
                  + "  from the trash table.";
            LOGGER.info(info);
            return info;
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
        pw.printf("Cleaning Interval: %s %s\n", _refreshInterval, _refreshIntervalUnit);
        pw.printf("Cleanup grace period: %s\n", TimeUtils.describe(_gracePeriod).orElse("-"));
        pw.printf("Pool reply timeout:  %d %s\n", _poolStub.getTimeout(),
              _poolStub.getTimeoutUnit());
        pw.printf("Number of files processed at once:  %d\n", _processAtOnce);
        pw.printf("Delete notification targets:  %s\n",
              Arrays.toString(_deleteNotificationTargets));
        int threadPoolSize = _executor.getCorePoolSize();
        pw.printf("Cleaning up to %d pools in parallel\n",
              threadPoolSize == 1 ? 1 : threadPoolSize - 1);
        pw.printf("Pools currently being cleaned: %d [%s]\n", _poolsBeingCleaned.size(),
              String.join(", ", _poolsBeingCleaned.keySet()));
    }

}
