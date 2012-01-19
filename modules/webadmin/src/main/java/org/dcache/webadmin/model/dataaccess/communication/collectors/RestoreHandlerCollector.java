package org.dcache.webadmin.model.dataaccess.communication.collectors;

import com.google.common.collect.ImmutableSet;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.RestoreHandlerInfo;
import dmg.cells.nucleus.CellPath;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.dcache.webadmin.model.businessobjects.RestoreInfo;
import org.dcache.webadmin.model.dataaccess.communication.ContextPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author jans
 */
public class RestoreHandlerCollector extends Collector {

    private String _poolManagerName;
    private static final long CONSIDERED_NEW_INTERVAL = TimeUnit.MINUTES.toMillis(2L);
    private static final Logger _log = LoggerFactory.getLogger(RestoreHandlerCollector.class);

    @Override
    public void run() {
        try {
            for (;;) {
                collectRestores();
                Thread.sleep(10000);
            }
        } catch (InterruptedException e) {
            _log.info("RestoreHandler collector interrupted");
        }
    }

    private void collectRestores()
            throws InterruptedException {
        try {
            RestoreHandlerInfo[] restores =
                    _cellStub.sendAndWait(new CellPath(_poolManagerName),
                    "xrc ls", RestoreHandlerInfo[].class);
            List<RestoreInfo> agedList = filterOutNewRestores(restores);
            _pageCache.put(ContextPaths.RESTORE_INFOS, ImmutableSet.copyOf(agedList));
        } catch (CacheException ex) {
            _log.debug("Could not retrieve restoreHandlerInfos from {}", _poolManagerName);
            _pageCache.remove(ContextPaths.RESTORE_INFOS);
        }
    }

    private List<RestoreInfo> filterOutNewRestores(RestoreHandlerInfo[] restores) {
        List<RestoreInfo> aged = new ArrayList<RestoreInfo>();
        long cut = System.currentTimeMillis() - (CONSIDERED_NEW_INTERVAL);
        for (RestoreHandlerInfo info : restores) {
            if ((info.getStartTime() < cut)) {
                RestoreInfo restoreInfo = new RestoreInfo(info);
                aged.add(restoreInfo);
            }
        }
        return aged;
    }

    public void setPoolManagerName(String poolManagerName) {
        _poolManagerName = poolManagerName;
    }
}
