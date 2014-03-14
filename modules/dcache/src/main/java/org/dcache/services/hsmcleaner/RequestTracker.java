package org.dcache.services.hsmcleaner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;

import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.util.Args;

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
public class RequestTracker implements CellMessageReceiver
{
    private final static Logger _log =
        LoggerFactory.getLogger(RequestTracker.class);

    /**
     * Utility class to keep track of timeouts.
     */
    class Timeout extends TimerTask
    {
        String _hsm;
        String _pool;

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
     * CellStub used for sending messages to pools.
     */
    private CellStub _poolStub;

    /**
     * Timeout for delete request.
     *
     * For each HSM, we have at most one outstanding remove request.
     */
    private Map<String,Timeout> _poolRequests =
        new HashMap<>();

    /**
     * A simple queue of locations to delete, grouped by HSM.
     *
     * The main purpose is to allow bulk removal of files, thus not
     * spamming the pools with a large number of small delete
     * requests. For each HSM, there will be at most one outstanding
     * remove request; new entries during that period will be queued.
     */
    private Map<String,Set<URI>> _locationsToDelete =
        new HashMap<>();

    /**
     * Locations that could not be deleted are pushed to this sink.
     */
    private Sink<URI> _failureSink;

    /**
     * Locations that were deleted are pushed to this sink.
     */
    private Sink<URI> _successSink;

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
    private Timer _timer = new Timer("Request tracker timeout");

    /**
     * Pools currently available.
     */
    private PoolInformationBase _pools;

    /**
     * Sets the CellStub for communicating with pools.
     */
    synchronized public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    /**
     * Set PoolInformationBase from which the request tracker learns
     * about available pools.
     */
    synchronized public void setPoolInformationBase(PoolInformationBase pools)
    {
        _pools = pools;
    }

    /**
     * Set maximum number of files to include in a single request.
     */
    synchronized public void setMaxFilesPerRequest(int value)
    {
        _maxFilesPerRequest = value;
    }

    /**
     * Returns maximum number of files to include in a single request.
     */
    synchronized public int getMaxFilesPerRequest()
    {
        return _maxFilesPerRequest;
    }

    /**
     * Set timeout in milliseconds for delete requests send to pools.
     */
    synchronized public void setTimeout(long timeout)
    {
        _timeout = timeout;
    }

    /**
     * Returns timeout in milliseconds for delete requests send to
     * pools.
     */
    synchronized public long getTimeout()
    {
        return _timeout;
    }

    /**
     * Sets the sink to which success to delete a file is reported.
     */
    synchronized public void setSuccessSink(Sink<URI> sink)
    {
        _successSink = sink;
    }

    /**
     * Sets the sink to which failure to delete a file is reported.
     */
    synchronized public void setFailureSink(Sink<URI> sink)
    {
        _failureSink = sink;
    }

    /**
     * Submits a request to delete a file.
     *
     * The request may not be submitted right away. It may be queued
     * and submitted together with other requests.
     *
     * @param location the URI of the file to delete
     */
    synchronized public void submit(URI location)
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
    synchronized private void flush(String hsm)
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

        /* It may happen that our information about the pools is
         * outdated and that the pool is no longer
         * available. Therefore we may have to try several pools.
         */
        PoolInformation pool;
        while ((pool = _pools.getPoolWithHSM(hsm)) != null) {
            String name = pool.getName();
            try {
                PoolRemoveFilesFromHSMMessage message =
                    new PoolRemoveFilesFromHSMMessage(name, hsm, locations);

                _poolStub.notify(new CellPath(name), message);

                Timeout timeout = new Timeout(hsm, name);
                _timer.schedule(timeout, _timeout);
                _poolRequests.put(hsm, timeout);
                break;
            } catch (NoRouteToCellException e) {
                _log.error("Failed to send message to " + name
                           + ": e.getMessage()");
                _pools.remove(pool.getName());
            }
        }

        /* If there is no available pool, then we report failure on
         * all files.
         */
        if (pool == null) {
            _log.warn("No pools attached to " + hsm + " are available");

            Iterator<URI> i = _locationsToDelete.get(hsm).iterator();
            while (i.hasNext()) {
                URI location = i.next();
                assert location.getAuthority().equals(hsm);
                _failureSink.push(location);
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
    synchronized private void timeout(String hsm, String pool)
    {
        _log.error("Timeout deleting files on HSM " + hsm
                   + " attached to " + pool);
        _poolRequests.remove(hsm);
        _pools.remove(pool);
        flush(hsm);
    }

    /**
     * Message handler for responses from pools.
     */
    synchronized public void messageArrived(PoolRemoveFilesFromHSMMessage msg)
    {
        /* In case of failure we rely on the timeout to invalidate the
         * entries.
         */
        if (msg.getReturnCode() != 0) {
            _log.error("Received failure from pool: " + msg.getErrorObject());
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
            _log.warn("Failed to delete " + failures.size()
                    + " files from HSM " + hsm + ". Will try again later.");
        }

        for (URI location : success) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location)) {
                _successSink.push(location);
            }
        }

        for (URI location : failures) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location)) {
                _failureSink.push(location);
            }
        }

        Timeout timeout = _poolRequests.remove(hsm);
        if (timeout != null) {
            timeout.cancel();
        }

        flush(hsm);
    }

    public final static String hh_requests_ls = "[hsm] # Lists delete requests";
    synchronized public String ac_requests_ls_$_0_1(Args args)
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

    public void shutdown()
    {
        _timer.cancel();
    }
}
