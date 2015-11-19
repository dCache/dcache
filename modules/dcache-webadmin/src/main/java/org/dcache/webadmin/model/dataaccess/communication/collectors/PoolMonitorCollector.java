package org.dcache.webadmin.model.dataaccess.communication.collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;

import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.backoff.IBackoffAlgorithm.Status;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.dcache.webadmin.model.dataaccess.util.rrd4j.RrdPoolInfoAgent;

/**
 *
 * @author jans
 */
public class PoolMonitorCollector extends Collector {

    private static final Logger _log
        = LoggerFactory.getLogger(PoolMonitorCollector.class);

    private boolean plottingEnabled;
    private RrdPoolInfoAgent rrdAgent;

    private void collectPoolSelectionUnit() throws CacheException,
                    InterruptedException {
        _log.debug("Retrieving Pool Monitor");
        PoolManagerGetPoolMonitor reply
            = _cellStub.sendAndWait(new PoolManagerGetPoolMonitor());
        PoolMonitor monitor = reply.getPoolMonitor();
        _pageCache.put(ContextPaths.POOLMONITOR, monitor);
        if (plottingEnabled) {
            rrdAgent.notify(monitor);
        }
        _log.debug("Pool Monitor retrieved successfully");
    }

    @Override
    public Status call() throws InterruptedException {
        try {
            collectPoolSelectionUnit();
        } catch (CacheException ex) {
            _log.debug("Could not retrieve Pool Monitor ", ex);
            _pageCache.remove(ContextPaths.POOLMONITOR);
            return Status.FAILURE;
        }
        return Status.SUCCESS;
    }

    public void setPlottingEnabled(boolean plottingEnabled) {
        this.plottingEnabled = plottingEnabled;
    }

    public void setRrdAgent(RrdPoolInfoAgent rrdAgent) {
        this.rrdAgent = rrdAgent;
    }
}
