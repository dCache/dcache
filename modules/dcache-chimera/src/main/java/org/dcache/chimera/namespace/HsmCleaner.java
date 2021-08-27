package org.dcache.chimera.namespace;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import dmg.util.command.Option;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;

import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.util.CommandException;
import dmg.util.command.Argument;
import dmg.util.command.Command;

import static dmg.util.CommandException.checkCommand;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.cells.HAServiceLeadershipManager.HA_NOT_LEADER_MSG;

/**
 * This class encapsulates the interaction with pools.
 *
 * At the abstract level it provides a method for submitting file
 * deletions. Notification of success or failure is provided
 * asynchronously via two sinks.
 *
 * To reduce the load on pools, files are deleted in batches. For each
 * HSM, at most one request is sent at a time. The class defines an
 * upper limit on the size of a request.
 */
public class HsmCleaner extends AbstractCleaner implements CellMessageReceiver, CellCommandListener, CellInfoProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(HsmCleaner.class);

    /**
     * Utility class to keep track of timeouts.
     */
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

    /**
     * The latest file creation time seen while iterating over the database, which is used
     * to order the delete locations.
     */
    private Timestamp _dbLastSeenTimestamp = new Timestamp(0);

    /**
     * Timeout for delete request per HSM.
     *
     * For each HSM, we have at most one outstanding remove request.
     */
    private final Map<String,Timeout> _requestTimeoutPerHsm = new HashMap<>();

    /**
     * A simple queue of locations to delete, grouped by HSM.
     *
     * The main purpose is to allow bulk removal of files, thus not
     * spamming the pools with a large number of small delete
     * requests. For each HSM, there will be at most one outstanding
     * remove request; new entries during that period will be queued.
     */
    private final Map<String,Set<URI>> _locationsToDelete = new HashMap<>();

    /**
     * Locations that could not be deleted are pushed to this sink.
     */
    private Consumer<URI> _failureSink;

    /**
     * Locations that were deleted are pushed to this sink.
     */
    private Consumer<URI> _successSink;

    /**
     * Maximum number of cached locations to delete.
     */
    private int _maxCachedDeleteLocations = 12000;

    /**
     * Maximum number of files to include in a single request.
     */
    private int _maxFilesPerRequest = 100;

    /**
     * Timeout for delete requests sent to HSM pools.
     */
    private long _hsmTimeout;
    private TimeUnit _hsmTimeoutUnit;

    /**
     * Timer used for implementing timeouts.
     */
    private final Timer _timer = new Timer("Request tracker timeout");

    /**
     * Set maximum number of cached locations to delete.
     */
    public synchronized void setMaxCachedDeleteLocations(int value) {
        _maxCachedDeleteLocations = value;
    }
    /**
     * Set maximum number of files to include in a single request.
     */
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

    private void removeHsmRequestTimeout(String hsm) {
        Timeout timeout = _requestTimeoutPerHsm.remove(hsm);
        if (timeout != null) {
            timeout.cancel();
        }
        if(!_hasHaLeadership) {
            // Remove all remaining cached requests for this HSM from
            // the cache, which may be outdated after regaining leadership.
            _locationsToDelete.remove(hsm);
        }
    }

    /**
     * Sets the sink to which success to delete a file is reported.
     */
    public synchronized void setSuccessSink(Consumer<URI> sink) {
        _successSink = sink ;
    }

    /**
     * Sets the sink to which failure to delete a file is reported.
     */
    public synchronized void setFailureSink(Consumer<URI> sink) {
        _failureSink = sink;
    }

    /**
     * Called when a file was successfully deleted from the HSM.
     */
    protected void onSuccess(URI uri) {
        try {
            LOGGER.debug("HSM-ChimeraCleaner: remove entries from the trash-table. ilocation={}", uri);
            _db.update("DELETE FROM t_locationinfo_trash WHERE ilocation=? AND itype=0", uri.toString());
        } catch (DataAccessException e) {
            LOGGER.error("Error when deleting from the trash-table: {}", e.getMessage());
        }
    }

    /**
     * Called when a file could not be deleted from the HSM.
     */
    protected void onFailure(URI uri) {
        LOGGER.info("Failed to delete a file {} from HSM. Will try again later.", uri);
    }

    /**
     * Submits a request to delete a file.
     *
     * The request may not be submitted right away. It may be queued
     * and submitted together with other requests.
     * This method is only called when having the HA group leadership role.
     *
     * @param location the URI of the file to delete
     */
    public synchronized void submit(URI location) {
        String hsm = location.getAuthority();
        Set<URI> locations = _locationsToDelete.get(hsm);
        if (locations == null) {
            locations = new HashSet<>();
            _locationsToDelete.put(hsm, locations);
        }
        locations.add(location);
        flush(hsm);
    }

    /**
     * Submits requests queued for a given HSM if there is not already a pending request registered for this HSM.
     *
     * @param hsm the name of an HSM instance
     */
    private synchronized void flush(String hsm) {
        // Don't allow flushing when not having leadership
        if (!_hasHaLeadership) {
            return;
        }

        Collection<URI> locations = _locationsToDelete.get(hsm);
        if (locations == null || locations.isEmpty()) {
            return;
        }

        if (_requestTimeoutPerHsm.containsKey(hsm)) {
            return;
        }

        /* To avoid excessively large requests, we limit the number
         * of files per request.
         */
        if (locations.size() > _maxFilesPerRequest) {
            Collection<URI> subset = new ArrayList<>(_maxFilesPerRequest);
            Iterator<URI> iterator = locations.iterator();
            for (int i = 0; i < _maxFilesPerRequest; i++) {
                subset.add(iterator.next());
            }
            locations = subset;
        }

        PoolInformation pool = _pools.getPoolWithHSM(hsm);
        if (pool != null) {
            String name = pool.getName();
            PoolRemoveFilesFromHSMMessage message = new PoolRemoveFilesFromHSMMessage(name, hsm, locations);

            _poolStub.notify(new CellPath(name), message);

            Timeout timeout = new Timeout(hsm, name);
            _timer.schedule(timeout, _hsmTimeoutUnit.toMillis(_hsmTimeout));
            _requestTimeoutPerHsm.put(hsm, timeout);
        } else {
            /* If there is no available pool, then we report failure on
             * all files.
             */
            LOGGER.warn("No pools attached to {} are available", hsm );

            Iterator<URI> i = _locationsToDelete.get(hsm).iterator();
            while (i.hasNext()) {
                URI location = i.next();
                assert location.getAuthority().equals(hsm);
                _failureSink.accept(location);
                i.remove();
            }
        }
    }

    /**
     * Called when a request to a pool has timed out. We remove the
     * pool from out list of known pools and resubmit the request.
     *
     * One may worry that in case of problems we end up resubmit the
     * same requests over and over. A timeout will however only happen
     * if either the pool crashed or in case of a bug in the pool. In
     * the first case we will end up trying another pool. In the
     * second case, we should simply fix the bug in the pool.
     */
    private synchronized void timeout(String hsm, String pool) {
        LOGGER.error("Timeout deleting files on HSM {} attached to {}", hsm, pool);
        removeHsmRequestTimeout(hsm);
        _pools.remove(pool);
        flush(hsm);
    }

    /**
     * Message handler for responses from pools.
     */
    public synchronized void messageArrived(PoolRemoveFilesFromHSMMessage msg) {
        /* In case of failure we rely on the timeout to invalidate the
         * entries.
         */
        if (msg.getReturnCode() != 0) {
            LOGGER.error("Received failure from pool: {}", msg.getErrorObject());
            return;
        }

        String hsm = msg.getHsm();
        Collection<URI> locations = _locationsToDelete.get(hsm);
        Collection<URI> success = msg.getSucceeded();
        Collection<URI> failures = msg.getFailed();

        if (locations == null) {
            /* Seems we got a reply for something this instance did
             * not request. We log this as a warning, but otherwise
             * ignore it.
             */
            LOGGER.warn("Received confirmation from a pool, for an action this cleaner did not request.");
            return;
        }

        if (!failures.isEmpty()) {
            LOGGER.warn("Failed to delete {} files from HSM {}. Will try again later.", failures.size(), hsm);
        }

        for (URI location : success) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location)) {
                _successSink.accept(location);
            }
        }

        for (URI location : failures) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location)) {
                _failureSink.accept(location);
            }
        }

        removeHsmRequestTimeout(hsm);
        flush(hsm);
    }

    /**
     * Delete files stored on tape (HSM).
     */
    @Override
    protected void runDelete() throws InterruptedException {
        int locationsCached = _locationsToDelete.values().stream().map(Set::size).reduce(0, Integer::sum);
        int queryLimit = _maxCachedDeleteLocations - locationsCached;

        LOGGER.debug("Locations cached: {} (max cached: {}), query limit: {}, offset: {}", locationsCached, _maxCachedDeleteLocations, queryLimit, _dbLastSeenTimestamp);

        if (queryLimit <= 0) {
            LOGGER.debug("The number of cached hsm locations is already the maximum permissible size. " +
                    "Not adding further entries.");
            _locationsToDelete.keySet().forEach(h -> flush(h)); // avoid not processing the remaining requests and being stuck
            return;
        }

        AtomicInteger noRequestsCollected = new AtomicInteger();
        Timestamp graceTime = Timestamp.from(Instant.now().minusSeconds(_gracePeriod.getSeconds()));

        _db.query("SELECT ilocation, ictime FROM t_locationinfo_trash WHERE itype=0 AND ictime<? AND ictime>? ORDER BY ictime ASC LIMIT ?",
                rs -> {
                    try {
                        Preconditions.checkState(_hasHaLeadership,
                                "HA leadership was lost while reading from trashtable. Aborting operation.");

                        URI uri = new URI(rs.getString("ilocation"));
                        submit(uri);

                        Timestamp ctime = rs.getTimestamp("ictime");
                        _dbLastSeenTimestamp = ctime;

                        noRequestsCollected.getAndIncrement();

                    } catch (URISyntaxException e) {
                        throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                    }
                },
                graceTime, _dbLastSeenTimestamp, queryLimit);

        if (_dbLastSeenTimestamp.getTime() != 0 && noRequestsCollected.get() < queryLimit) {
            // We have reached the end of the database and should start at the beginning next run
            _dbLastSeenTimestamp = new Timestamp(0);
        }
    }

    public void init() {
        setSuccessSink(uri -> _executor.execute(() -> onSuccess(uri)));
        setFailureSink(uri -> _executor.execute(() -> onFailure(uri)));
    }

    public void shutdown() {
        _timer.cancel();
    }

    @Override
    public void notLeader() {
        super.notLeader();
        // All not yet sent but cached requests can be cleared
        Iterator<String> keyIterator = _locationsToDelete.keySet().iterator();
        while(keyIterator.hasNext()) {
            String hsm = keyIterator.next();
            if(!_requestTimeoutPerHsm.containsKey(hsm)) {
                keyIterator.remove();
            }
        }
        _dbLastSeenTimestamp =  new Timestamp(0);
    }

    ////////////////////////////////////////////////////////////////////////////

    @Command(name = "requests ls",
            hint = "Lists delete requests.")
    public class RequestsLsCommand implements Callable<String> {
        @Argument(required = false, usage = "HSM instance for which to list delete requests")
        String hsm;

        @Override
        public String call() {
            StringBuilder sb = new StringBuilder();
            if (Strings.isNullOrEmpty(hsm)) {
                sb.append(String.format("%-15s %s %s\n",
                        "HSM Instance", "Files", "Pool"));
                for (Map.Entry<String,Set<URI>> e: _locationsToDelete.entrySet()) {
                    Timeout timeout = _requestTimeoutPerHsm.get(e.getKey());

                    if (timeout == null) {
                        sb.append(String.format("%-15s %5d\n",
                                e.getKey(),
                                e.getValue().size()));
                    } else {
                        sb.append(String.format("%-15s %5d %s\n",
                                e.getKey(),
                                e.getValue().size(),
                                timeout.getPool()));
                    }
                }
            } else {
                Collection<URI> locations = _locationsToDelete.get(hsm);
                if (locations != null) {
                    for (URI location: locations) {
                        sb.append(location).append('\n');
                    }
                }
            }
            return sb.toString();
        }
    }

    @Command(name = "hsm set maxCachedDeleteLocations",
            hint = "Changes the maximum number of cached hsm delete locations.")
    public class HsmSetMaxCachedDeleteLocationsCommand implements Callable<String> {
        @Argument(usage = "maximal number of cached hsm delete locations")
        int maxCachedDeleteLocations;

        @Override
        public String call() throws CommandException, IllegalArgumentException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (maxCachedDeleteLocations <= 0) throw new IllegalArgumentException("The number must be greater than 0.");

            _maxCachedDeleteLocations = maxCachedDeleteLocations;
            return "Maximal number of cached hsm delete locations set to " + _maxCachedDeleteLocations;
        }
    }

    @Command(name = "hsm set maxFilesPerRequest",
            hint = "Changes the number of files sent to a HSM instance for processing at once.")
    public class HsmSetMaxFilesPerRequestCommand implements Callable<String> {
        @Argument(usage = "maximal number of concurrent requests to a single HSM")
        int maxFilesPerRequest;

        @Override
        public String call() throws CommandException, IllegalArgumentException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            if (maxFilesPerRequest <= 0) throw new IllegalArgumentException("The number must be greater than 0.");

            _maxFilesPerRequest = maxFilesPerRequest;
            return "Maximal number of files per request to a single HSM is set to " + _maxFilesPerRequest;
        }
    }

    @Command(name = "hsm set timeOut",
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
            if (hsmTimeout <= 0) throw new IllegalArgumentException("The number must be greater than 0.");

            _hsmTimeout = hsmTimeout;
            _hsmTimeoutUnit = hsmTimeoutUnit;
            return "Timeout for cleaning requests from HSM-pools is set to " + _hsmTimeout + " " + _hsmTimeoutUnit;
        }
    }

    /////  HSM admin commands /////

    @Command(name = "rundelete hsm",
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
    @Command(name = "clean file hsm",
            hint = "Clean this file on HSM (file will be deleted from HSM)")
    public class CleanFileHsmCommand implements Callable<String> {
        @Argument(usage = "pnfsid of the file to clean")
        String pnfsId;

        @Override
        public String call() throws CommandException {
            checkCommand(_hasHaLeadership, HA_NOT_LEADER_MSG);
            _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=0 ORDER BY iatime",
                    rs -> {
                        try {
                            submit(new URI(rs.getString("ilocation")));
                        } catch (URISyntaxException e) {
                            throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                        }
                    },
                    pnfsId);
            return "";
        }
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("Timeout for cleaning requests to HSM-pools: %s\n", _hsmTimeout);
        pw.printf("Timeout Unit for cleaning requests to HSM-pools: %s\n", _hsmTimeoutUnit);
        pw.printf("Maximum number of cached delete locations:   %d\n", _maxCachedDeleteLocations);
        pw.printf("Maximum number of files to include in a single request:   %d\n", _maxFilesPerRequest);
        pw.printf("Delete notification targets:  %s\n", Arrays.toString(_deleteNotificationTargets));
    }
}