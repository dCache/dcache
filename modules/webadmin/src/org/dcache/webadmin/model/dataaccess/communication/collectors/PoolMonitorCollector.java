package org.dcache.webadmin.model.dataaccess.communication.collectors;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class PoolMonitorCollector extends Collector {

    private final static Logger _log = LoggerFactory.getLogger(PoolMonitorCollector.class);

    @Override
    public void run() {
        try {
            for (;;) {
                try {
                    collectPoolSelectionUnit();
//                  catch everything - maybe next round it works out
                } catch (RuntimeException e) {
                    _log.error(e.toString(), e);
                }
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            _log.info("PoolMonitorCollector Collector interrupted");
        }
    }

    private void collectPoolSelectionUnit() throws InterruptedException {
        try {
            _log.debug("Retrieving Pool Monitor");
            PoolManagerGetPoolMonitor reply = _cellStub.sendAndWait(
                    new PoolManagerGetPoolMonitor());
            _pageCache.put(ContextPaths.POOLMONITOR,
                    reply.getPoolMonitor());
            _log.debug("Pool Monitor retrieved successfully");
        } catch (CacheException ex) {
            _log.debug("Could not retrieve poolselectionunit ", ex);
        }
    }
}
