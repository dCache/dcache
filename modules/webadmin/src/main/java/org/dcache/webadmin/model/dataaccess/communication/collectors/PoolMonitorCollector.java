package org.dcache.webadmin.model.dataaccess.communication.collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;

import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolMonitorCollector extends Collector {

    private final static Logger _log = LoggerFactory.getLogger(PoolMonitorCollector.class);

    private void collectPoolSelectionUnit() throws InterruptedException {
        try {
            _log.debug("Retrieving Pool Monitor");
            PoolManagerGetPoolMonitor reply = _cellStub.sendAndWait(new PoolManagerGetPoolMonitor());
            _pageCache.put(ContextPaths.POOLMONITOR, reply.getPoolMonitor());
            _log.debug("Pool Monitor retrieved successfully");
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Pool Monitor ", ex);
            _pageCache.remove(ContextPaths.POOLMONITOR);
        }
    }

    @Override
    public Status call() throws Exception {
        try {
            collectPoolSelectionUnit();
            /*
             * add the write to RrdDb here if elapsed interval is 5 minutes. TODO
             */
        } catch (RuntimeException e) {
            _log.error(e.toString(), e);
            return Status.FAILURE;
        }
        return Status.SUCCESS;
    }
}
