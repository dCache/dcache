package org.dcache.services.hsmcleaner;

import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Collection;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Iterator;

import java.net.URI;

import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolRemoveFilesFromHSMMessage;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellNucleus;

/**
 * Encapsulates information about an HSM attached pool.
 */
class Pool
{
    /** The name of the pool. */
    private final String _name;

    /** The instance names of attached HSM systems. */
    private final Collection<String> _hsmInstances;

    /** Creation time. */
    private final long _created;

    public Pool(String name, Collection<String> hsmInstances)
    {
        _name = name;
        _hsmInstances = hsmInstances;
        _created = System.currentTimeMillis();
    }

    /** Returns the pool name. */
    public String getName() 
    {
        return _name;
    }

    /** Returns the names of HSM instances attached to the pool. */
    public Collection<String> getHSMInstances() 
    {
        return _hsmInstances;
    }

    /**
     * Returns true if the age of this record is below 5 minutes.
     */
    public boolean isValid()
    {
        return System.currentTimeMillis() - _created < 60000;
    }
}

/**
 * Maintains an index of available pools and HSM instances attached to
 * them. 
 *
 * This relies on the cleaner receiving PoolUp messages from the pool.
 */
class Pools
{
    /**
     * Map of all pools currently up.
     */
    private Map<String, Pool> _pools = 
        new HashMap<String, Pool>();

    /**
     * Map from HSM instance name to the set of pools attached to that
     * HSM.
     */
    private Map<String, Collection<Pool>> _hsmToPool = 
        new HashMap<String, Collection<Pool>>();

    /** 
     * Returns a pool attached to a given HSM instance.
     *
     * @param hsm An HSM instance name.
     */
    public Pool getPool(String hsm)
    {
        Collection<Pool> pools = _hsmToPool.get(hsm);
        if (pools == null) {
            return null;
        }

        Iterator<Pool> i = pools.iterator();
        while (i.hasNext()) {
            Pool pool = i.next();
            if (!pool.isValid()) {
                _pools.remove(pool.getName());
                i.remove();
            }
        }

        if (pools.isEmpty()) {
            return null;
        }

        return pools.iterator().next();
    }

    /**
     * Removes information about a pool. The pool will be readded next
     * time a pool up message is received.
     *
     * @param name A pool name.
     */
    public void remove(String name)
    {
        Pool pool = _pools.get(name);
        if (pool != null) {
            for (String hsm : pool.getHSMInstances()) {
                _hsmToPool.remove(hsm);
            }
            _pools.remove(name);
        }
    }

    /**
     * Adds information about a pool.
     *
     * @param name A pool name.
     * @param instances Names of HSM instances attached to the pool.
     */
    public void add(String name, Collection<String> instances)
    {
        Pool pool = new Pool(name, instances);
        _pools.put(name, pool);
        for (String hsm : instances) {
            Collection<Pool> pools = _hsmToPool.get(hsm);
            if (pools == null) {
                pools = new ArrayList<Pool>();
                _hsmToPool.put(hsm, pools);
            }
            pools.add(pool);
        }
    }

    /**
     * Message handler for PoolUp messages.
     */ 
    public void messageArrived(PoolManagerPoolUpMessage message)
    {
        String name = message.getPoolName();
        PoolV2Mode mode = message.getPoolMode();
        boolean disabled = 
            mode.getMode() == PoolV2Mode.DISABLED 
            || mode.isDisabled(PoolV2Mode.DISABLED_DEAD)
            || mode.isDisabled(PoolV2Mode.DISABLED_STRICT);

        remove(name);
        if (!disabled) {
            add(name, message.getHsmInstances());
        }            
    }
}

/**
 * This class encapsulates the interaction with pools.
 *
 * At the abstract level it provides a method for submitting file
 * deletions. Notifcation of success or failure is provided
 * asynchronously via two sinks. 
 *
 * An Pools instance is used to keep track of available pools for
 * accessing the HSMs.
 *
 * To reduce the load on pools, files are deleted in batches. For each
 * HSM, at most one request is send at a time. The class defines an
 * upper limit on the size of a request.
 */
class RequestTracker
{
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

        public void run()
        {
            timeout(_hsm, _pool);
        }
        
        public String getPool()
        {
            return _pool;
        }
    }

    private final static String POOLUP_MESSAGE =
        "diskCacheV111.vehicles.PoolManagerPoolUpMessage";

    /**
     * Task for periodically registering the request tracker to
     * receive pool up messages.
     */
    private final BroadcastRegistrationTask _broadcastRegistration;

    /**
     * Cell used for sending messages.
     */
    private AbstractCell _cell;

    /**
     * Timeout for delete request.
     *
     * For each HSM, we have at most one outstanding remove request.
     */
    private Map<String,Timeout> _poolRequests =
        new HashMap<String,Timeout>();
    
    /**
     * A simple queue of locations to delete, grouped by HSM. 
     *
     * The main purpose is to allow bulk removal of files, thus not
     * spamming the pools with a large number of small delete
     * requests. For each HSM, there will be at most one outstanding
     * remove request; new entries during that period will be queued.
     */
    private Map<String,Set<URI>> _locationsToDelete =
        new HashMap<String,Set<URI>>();

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
    private Timer _timer = new Timer();

    /**
     * Pools currently available.
     */
    private Pools _pools = new Pools();

    public RequestTracker(AbstractCell cell)
    {
        CellNucleus nucleus = cell.getNucleus();
        CellPath me = new CellPath(nucleus.getCellName(),
                                   nucleus.getCellDomainName());
        _cell = cell;
        _broadcastRegistration =
            new BroadcastRegistrationTask(cell, POOLUP_MESSAGE, me);
        _timer.schedule(_broadcastRegistration, 0, 300000); // 5 minutes
        _cell.addMessageListener(this);
    }

    /**
     * Set maximum number of files to include in a single request.
     */
    public void setMaxFilesPerRequest(int value)
    {
        _maxFilesPerRequest = value;
    }

    /**
     * Returns maximum number of files to include in a single request.
     */
    public int getMaxFilesPerRequest()
    {
        return _maxFilesPerRequest;
    }

    /**
     * Set timeout in milliseconds for delete requests send to pools.
     */
    public void setTimeout(long timeout)
    {
        _timeout = timeout;
    }

    /**
     * Returns timeout in milliseconds for delete requests send to
     * pools.
     */
    public long getTimeout()
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
            locations = new HashSet<URI>();
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
        if (locations == null || locations.isEmpty()) 
            return;

        if (_poolRequests.containsKey(hsm))
            return;

        /* To avoid excessively large requests, we limit the number
         * of files per request.
         */
        if (locations.size() > _maxFilesPerRequest) {
            Collection<URI> subset = 
                new ArrayList<URI>(_maxFilesPerRequest);
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
        Pool pool;
        while ((pool = _pools.getPool(hsm)) != null) {
            String name = pool.getName();
            try {
                PoolRemoveFilesFromHSMMessage message = 
                    new PoolRemoveFilesFromHSMMessage(name, hsm, locations);
                
                _cell.sendMessage(new CellMessage(new CellPath(name), message));

                Timeout timeout = new Timeout(hsm, name);
                _timer.schedule(timeout, _timeout);
                _poolRequests.put(hsm, timeout);
                break;
            } catch (Exception e) {
                _cell.error("Failed to send message to " + name
                            + ": e.getMessage()");
                _pools.remove(pool.getName());
            }
        }

        /* If there is no available pool, then we report failure on
         * all files.
         */
        if (pool == null) {
            _cell.warn("No pools attached to " + hsm + " are available");

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
        _cell.error("Timeout deleting files HSM " + hsm 
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
        String hsm = msg.getHsm();
        Collection<URI> success = msg.getSucceeded();
        Collection<URI> failures = msg.getFailed();
        Collection<URI> locations = _locationsToDelete.get(hsm);

        if (locations == null) {
            /* Seems we got a reply for something this instance did
             * not request. We log this as a warning, but otherwise
             * ignore it.
             */
            _cell.warn("Received confirmation from a pool, for an action this cleaner did not request.");
            return;
        }

        if (!failures.isEmpty())
            _cell.warn("Failed to delete " + failures.size() 
                       + " files from HSM " + hsm + ". Will try again later.");

        for (URI location : success) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location))
                _successSink.push(location);
        }

        for (URI location : failures) {
            assert location.getAuthority().equals(hsm);
            if (locations.remove(location))
                _failureSink.push(location);
        }        
        
        Timeout timeout = _poolRequests.remove(hsm);
        if (timeout != null) {
            timeout.cancel();
        }

        flush(hsm);
    }

    public void messageArrived(PoolManagerPoolUpMessage message)
    {
        _pools.messageArrived(message);
    }
}
