package org.dcache.webadmin.model.dataaccess.communication.collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private boolean isPoolMonitorCached = false;
    private boolean plottingEnabled;
    private RrdPoolInfoAgent rrdAgent;
    private PoolMonitor poolMonitor;

    @Override
    public Status call() throws InterruptedException {
        /*
         * The PageInfoCache injects itself into this object inside the former's
         * init method, which follows the collector initialization,
         * so unfortunately the poolMonitor cannot be added
         * during the latter routine.
         */
        if (!isPoolMonitorCached) {
            _pageCache.put(ContextPaths.POOLMONITOR, poolMonitor);
            isPoolMonitorCached = true;
        }
        if (plottingEnabled) {
            rrdAgent.notify(poolMonitor);
        }
        return Status.SUCCESS;
    }

    public void setPlottingEnabled(boolean plottingEnabled) {
        this.plottingEnabled = plottingEnabled;
    }

    public void setRrdAgent(RrdPoolInfoAgent rrdAgent) {
        this.rrdAgent = rrdAgent;
    }

    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }
}
