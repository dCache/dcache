package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.PnfsGetCacheLocationsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.dcache.cells.CellStub;
import org.dcache.pool.classic.IoQueueManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileLocationsPoolList implements RefreshablePoolList {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileLocationsPoolList.class);

    private final PnfsId pnfsId;
    private final CellStub pnfsManager;

    private List<PoolManagerPoolInformation> locations;

    public FileLocationsPoolList(PnfsId pnfsId, CellStub pnfsManager) {
        this.pnfsId = pnfsId;
        this.pnfsManager = pnfsManager;
        this.locations = new ArrayList<>();
    }

    @Override
    public ImmutableList<PoolManagerPoolInformation> getPools() {
        return ImmutableList.copyOf(locations);
    }

    @Override
    public ImmutableList<String> getOfflinePools() {
        return ImmutableList.of();
    }

    @Override
    public boolean isValid() {
        return true;
    }

    @Override
    public void refresh() {
        try {
            PnfsGetCacheLocationsMessage message =
                    pnfsManager.sendAndWait(new PnfsGetCacheLocationsMessage(pnfsId), TimeUnit.MINUTES.toMillis(1));
            Collection<String> newLocations = message.getCacheLocations();
            if (newLocations == null) {
                newLocations = Collections.emptyList();
            }

            List<PoolManagerPoolInformation> newPoolInfo = new ArrayList<>();
            for (String location : newLocations) {
                newPoolInfo.add(new PoolManagerPoolInformation(location,
                        new PoolCostInfo(location, IoQueueManager.DEFAULT_QUEUE), 0));
            }
            this.locations = newPoolInfo;
        } catch (Exception e) {
            LOGGER.warn("Failed to get file locations for {}: {}", pnfsId, e.getMessage());
            locations = new ArrayList<>();
        }
    }

    @Override
    public String toString() {
        return locations.toString();
    }
}
