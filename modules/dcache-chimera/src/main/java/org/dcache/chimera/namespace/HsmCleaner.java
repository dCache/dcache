package org.dcache.chimera.namespace;



import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellInfoProvider;

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
import java.util.function.Consumer;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;

import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;

import org.dcache.util.Args;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class encapsulates the interaction with pools.
 *
 * At the abstract level it provides a method for submitting file
 * deletions. Notifcation of success or failure is provided
 * asynchronously via two sinks.
 *
 * To reduce the load on pools, files are deleted in batches. For each
 * HSM, at most one request is send at a time. The class defines an
 * upper limit on the size of a request.
 */

public class HsmCleaner extends AbstractCleaner implements CellMessageReceiver, CellCommandListener, CellInfoProvider
{
    private static final Logger _log =
        LoggerFactory.getLogger(HsmCleaner.class);


    /**
     * Utility class to keep track of timeouts.
     */
    class Timeout extends TimerTask
    {
        final String _hsm;
        final String _pool;

        Timeout(String hsm, String pool)
        {
            _hsm = hsm;
            _pool = pool;
        }



        @Override
        public void run()
        {
            timeout(_hsm, _pool);
        }

        public String getPool()
        {
            return _pool;
        }
    }

    /**
     * Timeout for delete request.
     *
     * For each HSM, we have at most one outstanding remove request.
     */
    private final Map<String,Timeout> _poolRequests =
        new HashMap<>();

    /**
     * A simple queue of locations to delete, grouped by HSM.
     *
     * The main purpose is to allow bulk removal of files, thus not
     * spamming the pools with a large number of small delete
     * requests. For each HSM, there will be at most one outstanding
     * remove request; new entries during that period will be queued.
     */
    private final Map<String,Set<URI>> _locationsToDelete =
        new HashMap<>();

    /**
     * Locations that could not be deleted are pushed to this sink.
     */
    private Consumer<URI> _failureSink;

    /**
     * Locations that were deleted are pushed to this sink.
     */
    private Consumer<URI> _successSink;

    /**
     * Maximum number of files to include in a single request.
     */
    private int _maxFilesPerRequest = 100;

    /**
     * Timeout in milliseconds for delete requests send to pools.
     */
    private long _timeout = 60000;

    /**
     * Timer used for implementing timeouts.
     */
    private final Timer _timer = new Timer("Request tracker timeout");

    private int _hsmCleanerRequest;
    private long _hsmTimeout;
    private TimeUnit _hsmTimeoutUnit;

    /**
     * Set maximum number of files to include in a single request.
     */
    public synchronized void setMaxFilesPerRequest(int value)
    {
        _maxFilesPerRequest = value;
    }
    /**
     * Set timeout in milliseconds for delete requests send to pools.
     */
    public synchronized void setTimeout(long timeout)
    {
        _timeout = timeout;
    }

    /**
     * Returns timeout in milliseconds for delete requests send to
     * pools.
     */
    public synchronized long getTimeout()
    {
        return _timeout;
    }

    @Required
    public void setHsmCleanerRequest(int hsmCleanerRequest)
    {
        _hsmCleanerRequest = hsmCleanerRequest;
    }

    @Required
    public void setHsmTimeoutUnit(TimeUnit hsmTimeoutUnit)
    {
        _hsmTimeoutUnit = hsmTimeoutUnit;
    }

    @Required
    public void setHsmTimeout(long hsmTimeout)
    {
        _hsmTimeout = hsmTimeout;
    }

    /**
     * Sets the sink to which success to delete a file is reported.
     */
    public synchronized void setSuccessSink(Consumer<URI> sink)
    {
        _successSink = sink ;
    }

    /**
     * Sets the sink to which failure to delete a file is reported.
     */
    public synchronized void setFailureSink(Consumer<URI> sink)
    {
        _failureSink = sink;
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

    /**
     * Submits a request to delete a file.
     *
     * The request may not be submitted right away. It may be queued
     * and submitted together with other requests.
     *
     * @param location the URI of the file to delete
     */
    public synchronized void submit(URI location)
    {
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
     * Submits requests queued for a given HSM.
     *
     * @param hsm the name of an HSM instance
     */
    private synchronized void flush(String hsm)
    {
        Collection<URI> locations = _locationsToDelete.get(hsm);
        if (locations == null || locations.isEmpty()) {
            return;
        }

        if (_poolRequests.containsKey(hsm)) {
            return;
        }

        /* To avoid excessively large requests, we limit the number
         * of files per request.
         */
        if (locations.size() > _maxFilesPerRequest) {
            Collection<URI> subset =
                new ArrayList<>(_maxFilesPerRequest);
            Iterator<URI> iterator = locations.iterator();
            for (int i = 0; i < _maxFilesPerRequest; i++) {
                subset.add(iterator.next());
            }
            locations = subset;
        }

        PoolInformation pool = _pools.getPoolWithHSM(hsm);
        if (pool != null) {
            String name = pool.getName();
            PoolRemoveFilesFromHSMMessage message =
                new PoolRemoveFilesFromHSMMessage(name, hsm, locations);

            _poolStub.notify(new CellPath(name), message);

            Timeout timeout = new Timeout(hsm, name);
            _timer.schedule(timeout, _timeout);
            _poolRequests.put(hsm, timeout);
        } else {
            /* If there is no available pool, then we report failure on
             * all files.
             */
            _log.warn("No pools attached to {} are available", hsm );

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
     * if either the pool crashed or in case of a bug in the pool.  In
     * the first case we will end up trying another pool. In the
     * second case, we should simply fix the bug in the pool.
     */
    private synchronized void timeout(String hsm, String pool)
    {
        _log.error("Timeout deleting files on HSM {} attached to {}", hsm, pool);
        _poolRequests.remove(hsm);
        _pools.remove(pool);
        flush(hsm);
    }

    /**
     * Message handler for responses from pools.
     */
    public synchronized void messageArrived(PoolRemoveFilesFromHSMMessage msg)
    {
        /* In case of failure we rely on the timeout to invalidate the
         * entries.
         */
        if (msg.getReturnCode() != 0) {
            _log.error("Received failure from pool: {}", msg.getErrorObject());
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
            _log.warn("Received confirmation from a pool, for an action this cleaner did not request.");
            return;
        }

        if (!failures.isEmpty()) {
            _log.warn("Failed to delete {} files from HSM {}. Will try again later.", failures.size(), hsm );
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

        Timeout timeout = _poolRequests.remove(hsm);
        if (timeout != null) {
            timeout.cancel();
        }

        flush(hsm);
    }

    public void init()
    {
        super.init();
        setSuccessSink(uri -> _executor.execute(() -> onSuccess(uri)));
        setFailureSink(uri -> _executor.execute(() -> onFailure(uri)));

    }

    /**
     * Delete files stored on tape (HSM).
     */
    @Override
    protected void runDelete() throws InterruptedException
    {
        Timestamp graceTime = Timestamp.from(Instant.now().minusSeconds(_gracePeriod.getSeconds()));
        _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE itype=0 AND ictime<?",
                rs -> {
                    try {
                        URI uri = new URI(rs.getString("ilocation"));
                        _log.debug("Submitting a request to delete a file: {}", uri);
                        submit(uri);
                    } catch (URISyntaxException e) {
                        throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                    }
                },
                graceTime);

    }

    public static final String hh_requests_ls = "[hsm] # Lists delete requests";
    public synchronized String ac_requests_ls_$_0_1(Args args)
    {
        StringBuilder sb = new StringBuilder();
        if (args.argc() == 0) {
            sb.append(String.format("%-15s %s %s\n",
                                    "HSM Instance", "Files", "Pool"));
            for (Map.Entry<String,Set<URI>> e: _locationsToDelete.entrySet()) {
                Timeout timeout = _poolRequests.get(e.getKey());

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
            String hsm = args.argv(0);
            Collection<URI> locations = _locationsToDelete.get(hsm);
            if (locations != null) {
                for (URI location: locations) {
                    sb.append(location).append('\n');
                }
            }
        }

        return sb.toString();
    }

    public static final String hh_hsm_set_MaxFilesPerRequest = "<number> # maximal number of concurrent requests to a single HSM";

    public String ac_hsm_set_MaxFilesPerRequest_$_1(Args args) throws NumberFormatException
    {
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
        if (args.argc() > 0) {
            _hsmTimeout = Long.parseLong(args.argv(0));
            _hsmTimeoutUnit = SECONDS;
        }
        return "Timeout for cleaning requests to HSM-pools is set to " + _hsmTimeout + " " + _hsmTimeoutUnit;
    }

    /////  HSM admin commands /////

    public static final String hh_rundelete_hsm = " # run HSM Cleaner";
    public String ac_rundelete_hsm(Args args) throws InterruptedException {

        runDelete();
        return "";
    }

    //explicitly clean HSM-file
    public static final String hh_clean_file_hsm =
            "<pnfsID> # clean this file on HSM (file will be deleted from HSM)";
    public String ac_clean_file_hsm_$_1(Args args)
    {
        _db.query("SELECT ilocation FROM t_locationinfo_trash WHERE ipnfsid=? AND itype=0 ORDER BY iatime",
                rs -> {
                    try {
                        submit(new URI(rs.getString("ilocation")));
                    } catch (URISyntaxException e) {
                        throw new DataIntegrityViolationException("Invalid URI in database: " + e.getMessage(), e);
                    }
                },
                args.argv(0));

        return "";
    }

    /*
     * Cell specific
     */
    @Override
    public void getInfo(PrintWriter pw)
    {
        pw.printf("HSM Cleaner Info : \n");
        pw.printf("Timeout for cleaning requests to HSM-pools: %s\n", _hsmTimeout);
        pw.printf("Timeout Unit for cleaning requests to HSM-pools: %s\n", _hsmTimeoutUnit);
        pw.printf("Maximal number of concurrent requests to a single HSM:   %d\n", _hsmCleanerRequest);
        pw.printf("Maximum number of files to include in a single request:   %d\n", _maxFilesPerRequest);
        pw.printf("Delete notification targets:  %s\n", Arrays.toString(_deleteNotificationTargets));
    }

    public void shutdown()
    {
        _timer.cancel();
    }
}
