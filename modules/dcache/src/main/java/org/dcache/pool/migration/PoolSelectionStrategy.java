package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerPoolInformation;
import java.util.List;
import javax.annotation.Nullable;

/**
 * A strategy for selecting a pool from a list.
 */
public interface PoolSelectionStrategy {

    @Nullable
    PoolManagerPoolInformation select(List<PoolManagerPoolInformation> pools);
}
