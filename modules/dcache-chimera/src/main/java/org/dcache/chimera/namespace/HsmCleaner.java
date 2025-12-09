package org.dcache.chimera.namespace;

import static dmg.util.CommandException.checkCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.cells.HAServiceLeadershipManager.HA_NOT_LEADER_MSG;

import com.google.common.base.Preconditions;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

/**
 * This class encapsulates the interaction with pools.
 * <p>
 * At the abstract level it provides a method for submitting file deletions. Notification of success
 * or failure is provided asynchronously via two sinks.
 * <p>
 * To reduce the load on pools, files are deleted in batches. For each pool, at most one request is
 * sent at a time. The class defines an upper limit on the size of a request.
 */
public class HsmCleaner extends AbstractCleaner implements CellMessageReceiver, CellCommandListener,
      CellInfoProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HsmCleaner.class);

    /**
     * Queues of locations to delete grouped by hsm.
     */
    private final ConcurrentHashMap<String, Set<URI>> _locationsToDeletePerHsm = new ConcurrentHashMap<>();

    /**
     * Queues of requested delete locations grouped by pool.
     */
    private final ConcurrentHashMap<String, Set<URI>> _activeDeletesPerPool = new ConcurrentHashMap<>();

    /**
     * Timeout for delete request per active pool.
     */
    private final ConcurrentHashMap<String, Timeout> _requestTimeoutPerPool = new ConcurrentHashMap<>();

    private Consumer<URI> _failureSink;
    private Consumer<URI> _successSink;
    private int _maxCachedDeleteLocations = 12000;
    private int _maxFilesPerRequest = 100;

    private long _hsmTimeout;
    private TimeUnit _hsmTimeoutUnit;

    /**
     * The latest file creation time seen while iterating over the database, which is used to order
     * the delete locations.
     */
    private Timestamp _dbLastSeenTimestamp = new Timestamp(0);

    private final Timer _timer = new Timer("Request tracker timeout");

    class Timeout extends TimerTask {

        final String _hsm;
        final String _pool;

        Timeout(String hsm, String pool) {
            _hsm = hsm;
            _pool = pool;
        }

        @Override
        public void run() {
            timeout(_hsm, _pool);
        }

        public String getPool() {
            return _pool;
        }
    }

    public synchronized void setMaxCachedDeleteLocations(int value) {
        _maxCachedDeleteLocations = value;
    }

    public synchronized void setMaxFilesPerRequest(int value) {
        _maxFilesPerRequest = value;
    }

    @Required
    public void setHsmTimeoutUnit(TimeUnit hsmTimeoutUnit) {
        _hsmTimeoutUnit = hsmTimeoutUnit;
    }

    @Required
    public void setHsmTimeout(long hsmTimeout) {
        _hsmTimeout = hsmTimeout;
    }

    public synchronized void setSuccessSink(Consumer<URI> sink) {
        _successSink = sink;
    }

    public synchronized void setFailureSink(Consumer<URI> sink) {
        _failureSink = sink;
    }

    protected void addLocationsToDeletePerHsm(String hsm, Set<URI> locations) {
        Set<URI> pending = _locationsToDeletePerHsm.computeIfAbsent(hsm, k -> new HashSet<>());
        pending.addAll(locations);
    }

    protected HashMap<String, Set<URI>> getLocationsToDeletePerHsm() {
        return new HashMap<>(_locationsToDeletePerHsm);
    }

    protected void addActiveDeletesPerPool(String pool, Set<URI> locations) {
        Set<URI> active = _activeDeletesPerPool.computeIfAbsent(pool, k -> new HashSet<>());
        active.addAll(locations);
    }

    protected HashMap<String, Set<URI>> getActiveDeletesPerPool() {
        return new HashMap<>(_activeDeletesPerPool);
    }

    private Set<String> getActiveCleaningPools() {
        return _requestTimeoutPerPool.entrySet().stream()
              .filter(entry -> entry.getValue() != null)
              .map(Map.Entry::getKey)
              .collect(Collectors.toSet());
    }

    private void removePoolRequestTimeout(String pool) {
        Timeout timeout = _requestTimeoutPerPool.remove(pool);
        if (timeout != null) {
            timeout.cancel();
        }
    }

    protected void onSuccess(URI uri) {
        try {
            LOGGER.debug("Removing entry from the trash-table. ilocation={}", uri);
            _db.update("DELETE FROM t_locationinfo_trash WHERE ilocation=? AND itype=0",
                  uri.toString());
        } catch (DataAccessException e) {
            LOGGER.error("Error when deleting from the trash-table: {}", e.getMessage());
        }
    }


    protected void onFailure(URI uri) {
        LOGGER.info("Failed to delete a file {} from HSM. Will try again later.", uri);
    }

    private void markLocationsAsActiveDeletes(String hsm, String pool, Collection<URI> locations) {
        Set<URI> pending = _locationsToDeletePerHsm.get(hsm);
        if (pending == null) {
            return;
        }
        Set<URI> active = _activeDeletesPerPool.computeIfAbsent(pool, k -> new HashSet<>());
        for (URI uri : locations) {
            if (pending.remove(uri)) {
                active.add(uri);
            }
        }
        if (pending.isEmpty()) {
            _locationsToDeletePerHsm.remove(hsm);
        }
    }

    private void moveLocationsBackToPending(String hsm, String pool, Collection<URI> locations) {
        Set<URI> active = _activeDeletesPerPool.get(pool);
        if (active == null) {
            return;
        }
        Set<URI> pending = _locationsToDeletePerHsm.computeIfAbsent(hsm, k -> new HashSet<>());
        for (URI uri : locations) {
            if (active.remove(uri)) {
                pending.add(uri);
            }
        }
        if (active.isEmpty()) {
            _activeDeletesPerPool.remove(pool);
        }
    }

    private int countPendingLocations() {
        return _locationsToDeletePerHsm.values().stream().mapToInt(Set::size).sum();
    }

    private int countActiveLocations() {
        return _activeDeletesPerPool.values().stream().mapToInt(Set::size).sum();
    }

    /**
     * Submits a request to delete a file.
     * <p>
     * The request may not be submitted right away. It may be queued and submitted together with
     * other requests.
     *
     * @param location the URI of the file to delete
     */
    public synchronized void submit(URI location) {
        String hsm = location.getAuthority();
        Set<URI> locations = _locationsToDeletePerHsm.computeIfAbsent(hsm,
              k -> new HashSet<>());
        locations.add(location);
    }

    /**
     * Submits requests queued for a given HSM to a not already pending pool.
     *
     * @param hsm the name of an HSM instance
     */
    private synchronized void flush(String hsm) {
        if (!_hasHaLeadership) {
            return;
        }

        Collection<URI> locations = _locationsToDeletePerHsm.get(hsm);
        if (locations == null || locations.isEmpty()) {
            return;
        }

        List<URI> batch = locations.stream()
              .limit(_maxFilesPerRequest)
              .collect(Collectors.toList());

        List<PoolInformation> availablePoolsForHsm = _pools.getAvailablePoolsWithHSM(hsm);
        if (availablePoolsForHsm.isEmpty()) {
            /* If there is no available pool for that HSM, then we report failure on all files. */
            LOGGER.warn("No new pools attached to HSM {} are available", hsm);

            Iterator<URI> i = _locationsToDeletePerHsm.get(hsm).iterator();
            while (i.hasNext()) {
                URI location = i.next();
                assert location.getAuthority().equals(hsm);
                _failureSink.accept(location);
                i.remove();
            }
            _locationsToDeletePerHsm.remove(hsm);
            return;
        }

        Set<String> activeCleaningPools = getActiveCleaningPools();
        Optional<PoolInformation> newPool = availablePoolsForHsm.stream()
              .filter(pool -> !activeCleaningPools.contains(pool.getName()))
              .findFirst();
        if (newPool.isEmpty()) {
            return;
        }

        String poolName = newPool.get().getName();
        markLocationsAsActiveDeletes(hsm, poolName, batch);

        LOGGER.info("Sending {} delete locations for HSM {} to pool {}", batch.size(), hsm,
              poolName);
        PoolRemoveFilesFromHSMMessage msg = new PoolRemoveFilesFromHSMMessage(poolName, hsm, batch);
        _poolStub.notify(new CellPath(poolName), msg);

        Timeout timeout = new Timeout(hsm, poolName);
        _timer.schedule(timeout, _hsmTimeoutUnit.toMillis(_hsmTimeout));
        _requestTimeoutPerPool.put(poolName, timeout);
    }

    /**
     * Called when a request to a pool has timed out. We remove the pool from out list of known
     * pools and resubmit the request.
     * <p>
     * One may worry that in case of problems we end up resubmit the same requests over and over. A
     * timeout will however only happen if either the pool crashed or in case of a bug in the pool.
     * In the first case we will end up trying another pool. In the second case, we should simply
     * fix the bug in the pool.
     */
    private synchronized void timeout(String hsm, String pool) {
        LOGGER.error("Timeout deleting files on HSM {} via pool {}", hsm, pool);
        removePoolRequestTimeout(pool);
        _pools.remove(pool);
        Set<URI> locations = _activeDeletesPerPool.get(pool);
        moveLocationsBackToPending(hsm, pool, locations);
        flush(hsm);
    }

    /**
     * Message handler for responses from pools.
     */
    public synchronized void messageArrived(PoolRemoveFilesFromHSMMessage msg) {
        // In case of failure we rely on the timeout to invalidate the entries.
        if (msg.getReturnCode() != 0) {
            LOGGER.error("received failure from pool: {}", msg.getErrorObject());
            return;
        }

        String hsm = msg.getHsm();
        String poolName = msg.getPoolName();
        Collection<URI> poolLocations = _activeDeletesPerPool.get(poolName);

        boolean isStaleReply = false;
        HsmCleaner.Timeout currPoolTimeout = _requestTimeoutPerPool.get(poolName);
        if (currPoolTimeout == null) {
            LOGGER.warn("Received a remove reply from pool {}, which was not awaited. "
                  + "The cleaner pool timeout might be too small.", poolName);
            isStaleReply = true;
        }

        if (poolLocations == null) {
            /* Seems we got a reply for something this instance did not request.
            We log this as a warning, but otherwise ignore it. */
            LOGGER.warn("Received confirmation from a pool that is not expected.");
            return;
        }

        Collection<URI> successes = msg.getSucceeded();
        Collection<URI> failures = msg.getFailed();

        LOGGER.info(
              "HSM delete reply from pool {} connected to HSM {}: {} successes, {} failures",
              poolName, hsm, successes.size(), failures.size());

        for (URI location : successes) {
            assert location.getAuthority().equals(hsm);
            if (poolLocations.remove(location)) {
                _successSink.accept(location);
            }
        }
        for (URI location : failures) {
            assert location.getAuthority().equals(hsm);
            /* remove even failed locations from cache, as the failure might not be transient
            It will at some point be fetched from the database again and retried. */
            if (poolLocations.remove(location)) {
                _failureSink.accept(location);
            }
        }
        if (poolLocations.isEmpty()) {
            _activeDeletesPerPool.remove(poolName);
        }
        if (!isStaleReply) {
            removePoolRequestTimeout(poolName);
        }
    }

    /**
     * Refills the cached locations to delete. Then, for each HSM, triggers file deletions.
     */
    @Override
    protected synchronized void runDelete() {
        if (!_hasHaLeadership) {
            LOGGER.warn("Delete run triggered despite not having leadership. "
                  + "We assume this is a transient problem.");
            return;
        }
        LOGGER.info("New run...");

        int numLoationsCached = countPendingLocations() + countActiveLocations();
        int queryLimit = _maxCachedDeleteLocations - numLoationsCached;

        LOGGER.debug("Locations cached: {} (max cached: {}), query limit: {}, offset: {}",
              numLoationsCached, _maxCachedDeleteLocations, queryLimit, _dbLastSeenTimestamp);

        Set<String> hsms = Set.copyOf(_locationsToDeletePerHsm.keySet());
        if (queryLimit <= 0) {
            LOGGER.debug("The number of cached HSM locations is already the maximum "
                  + "permissible size. Not adding further entries.");
            hsms.forEach(this::flush);
            return;
        }

        AtomicInteger numRequestsCollected = new AtomicInteger();
        Timestamp graceTime = Timestamp.from(
              Instant.now().minusSeconds(_gracePeriod.getSeconds()));

        _db.query(
              "SELECT ilocation, ictime FROM t_locationinfo_trash WHERE itype=0 AND ictime<? AND ictime>? ORDER BY ictime ASC LIMIT ?",
              rs -> {
                  try {
                      Preconditions.checkState(_hasHaLeadership,
                            "HA leadership was lost while reading from trashtable. Aborting operation.");

                      URI uri = new URI(rs.getString("ilocation"));
                      submit(uri);
                      _dbLastSeenTimestamp = rs.getTimestamp("ictime");
                      numRequestsCollected.getAndIncrement();

                  } catch (URISyntaxException e) {
                      throw new DataIntegrityViolationException(
                            "Invalid URI in database: " + e.getMessage(), e);
                  }
              },
              graceTime, _dbLastSeenTimestamp, queryLimit);

        hsms.forEach(this::flush);

        if (_dbLastSeenTimestamp.getTime() != 0 && numRequestsCollected.get() < queryLimit) {
            // We have reached the end of the database and should start at the beginning next run
            _dbLastSeenTimestamp = new Timestamp(0);
        }
    }

    protected Map<String, Long> getDeleteLocationCountPerHsm() {
        HashMap<String, Long> deleteLocationsPerHsm = new HashMap<>();
        Timestamp graceTime = Timestamp.from(Instant.now().minusSeconds(_gracePeriod.getSeconds()));
        _db.query("SELECT ilocation, ictime FROM t_locationinfo_trash WHERE itype=0 AND ictime<?",
              rs -> {
                  try {
                      URI uri = new URI(rs.getString("ilocation"));
                      String theHsm = uri.getAuthority();

                      long newCount = deleteLocationsPerHsm.getOrDefault(theHsm, 0L) + 1L;
                      deleteLocationsPerHsm.put(theHsm, newCount);

                  } catch (URISyntaxException e) {
                      LOGGER.warn("Invalid URI in database: {}", e.getMessage());
                      long newUnknownCount = deleteLocationsPerHsm.getOrDefault(null, 0L) + 1L;
                      deleteLocationsPerHsm.put(null, newUnknownCount);
                  }
              },
              graceTime);
        return deleteLocationsPerHsm;
    }

    public void init() {
        setSuccessSink(uri -> _executor.execute(() -> onSuccess(uri)));
        setFailureSink(uri -> _executor.execute(() -> onFailure(uri)));
    }

    public void shutdown() {
        _timer.cancel();
    }

    @Override
    public synchronized void notLeader() {
        super.notLeader();
        // All not yet sent but cached requests can be cleared
        _locationsToDeletePerHsm.clear();
        _dbLastSeenTimestamp = new Timestamp(0);
    }

    /// ///////////////////////////////////////////////////////////////////////// //  HSM admin
    /// commands /////

    @Command(name = "requests count",
          hint = "Counts delete requests per hsm.")
    public class RequestsCountCommand implements Callable<String> {

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("%-15s %s\n",
                  "HSM Instance", "Files"));

            Map<String, Long> deleteLocationsPerHsm = getDeleteLocationCountPerHsm();

            for (Map.Entry<String, Long> e : deleteLocationsPerHsm.entrySet()) {
                String theHsm = e.getKey() == null ? "unknown" : e.getKey();
                sb.append(String.format("%-15s %5d\n",
                      theHsm,
                      e.getValue()));
            }
            return sb.toString();
        }
    }

    @Command(name = "set maxCachedDeleteLocations",
          hint = "Changes the maximum number of cached hsm delete locations.")
    public class HsmSetMaxCachedDeleteLocationsCommand implements Callable<String> {

        @Argument(usage = "maximal number of cached HSM delete locations")
        int maxCachedDeleteLocations;

        @Override
        public String call() throws CommandException, IllegalArgumentException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (maxCachedDeleteLocations <= 0) {
                throw new IllegalArgumentException("The number must be greater than 0.");
            }

            _maxCachedDeleteLocations = maxCachedDeleteLocations;
            return "Maximal number of cached HSM delete locations set to "
                  + _maxCachedDeleteLocations;
        }
    }

    @Command(name = "set maxFilesPerRequest",
          hint = "Changes the number of files sent to a HSM instance for processing at once.")
    public class HsmSetMaxFilesPerRequestCommand implements Callable<String> {

        @Argument(usage = "maximal number of concurrent requests to a single HSM")
        int maxFilesPerRequest;

        @Override
        public String call() throws CommandException, IllegalArgumentException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (maxFilesPerRequest <= 0) {
                throw new IllegalArgumentException("The number must be greater than 0.");
            }

            _maxFilesPerRequest = maxFilesPerRequest;
            return "Maximal number of files per request to a single HSM is set to "
                  + _maxFilesPerRequest;
        }
    }

    @Command(name = "set timeOut",
          hint = "Changes the timeout for delete requests sent to an HSM pool.")
    public class HsmSetTimeOutCommand implements Callable<String> {

        @Argument(usage = "cleaning request timeout for HSM pools")
        long hsmTimeout;

        @Option(name = "unit",
              valueSpec = "MILLISECONDS|SECONDS|MINUTES|HOURS|DAYS",
              usage = "timeout unit (default is SECONDS)")
        TimeUnit hsmTimeoutUnit = SECONDS;

        @Override
        public String call() throws CommandException, IllegalArgumentException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (hsmTimeout <= 0) {
                throw new IllegalArgumentException("The number must be greater than 0.");
            }

            _hsmTimeout = hsmTimeout;
            _hsmTimeoutUnit = hsmTimeoutUnit;
            return "Timeout for cleaning requests from HSM-pools is set to " + _hsmTimeout + " "
                  + _hsmTimeoutUnit;
        }
    }

    @Command(name = "rundelete",
          hint = "Runs the HSM Cleaner.")
    public class RundeleteHsmCommand implements Callable<String> {

        @Override
        public String call() throws InterruptedException, CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            runDelete();
            return "";
        }
    }

    // explicitly clean HSM-file
    @Command(name = "clean file",
          hint = "Delete this file from HSM if it is in the trash table")
    public class CleanFileHsmCommand implements Callable<String> {

        @Argument(usage = "pnfsid of the file to clean")
        String pnfsId;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            LOGGER.debug("Admin: pre-cleaning");
            _db.query(
                  "SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=0 ORDER BY iatime",
                  rs -> {
                      try {
                          submit(new URI(rs.getString("ilocation")));
                      } catch (URISyntaxException e) {
                          throw new DataIntegrityViolationException(
                                "Invalid URI in database: " + e.getMessage(), e);
                      }
                  },
                  pnfsId);
            return "";
        }
    }

    @Command(name = "forget pnfsid",
          hint = "Let cleaner forget the given hsm-resident pnfsid",
          description = "Removes the given pnfsid from the hsm cleaner's trash table.")
    public class ForgetPnfsidCommand implements Callable<String> {

        @Argument(usage = "pnfsid of the file to clean")
        String pnfsId;

        @Override
        public String call() throws CommandException {
            int returnVal;
            try {
                LOGGER.debug("Admin-triggered remove of pnfsid from hsm cleaner {}", pnfsId);
                returnVal = _db.update(
                      "DELETE FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=0", pnfsId);
            } catch (DataAccessException e) {
                return "Error when deleting pnfsid " + pnfsId + " from the hsm trash table: "
                      + e.getMessage();
            }
            return returnVal == 0 ? "PnfsId " + pnfsId + " not found in the hsm trash table."
                  : "Successfully deleted pnfsid " + pnfsId + " from the hsm trash table.";
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("Cleaning Interval: %s %s\n", _refreshInterval, _refreshIntervalUnit);
        pw.printf("Cleanup grace period: %s\n", TimeUtils.describe(_gracePeriod).orElse("-"));
        pw.printf("Timeout for cleaning requests to HSM-pools: %s %s\n", _hsmTimeout,
              _hsmTimeoutUnit);
        pw.printf("Maximum number of cached delete locations:   %d\n", _maxCachedDeleteLocations);
        pw.printf("Maximum number of files to include in a single request:   %d\n",
              _maxFilesPerRequest);

        int pendingDeletes = countPendingLocations();
        pw.printf("Pending cached delete locations: %d\n", pendingDeletes);

        int activeDeletes = countActiveLocations();
        pw.printf("In-flight delete locations: %d\n", activeDeletes);

        Set<String> activePools = Set.copyOf(getActiveCleaningPools());
        int activePoolCount = activePools.size();
        String activePoolsString = activePoolCount == 0 ? "0" : activePoolCount + " " + activePools;
        pw.printf("Pools currently waited for: %s\n", activePoolsString);
    }
}