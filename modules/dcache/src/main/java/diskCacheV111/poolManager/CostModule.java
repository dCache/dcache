// $Id: CostModule.java,v 1.2 2006-10-10 13:50:50 tigran Exp $

package diskCacheV111.poolManager;

import diskCacheV111.pools.PoolCostInfo;
import java.util.Collection;
import java.util.Map;
import javax.annotation.Nullable;
import org.dcache.poolmanager.PoolInfo;


/**
 * A Class that implements CostModule assigned a cost to each known pool.  This cost is in the form
 * of a PoolCostInfo object
 */
public interface CostModule {

    /**
     * Obtain the PoolCostInfo associated with the named pool.  If the pool is unknown then null is
     * returned.
     *
     * @param poolName the name of a pool
     * @return PostCostInfo corresponding to the named pool.
     */
    @Nullable
    PoolCostInfo getPoolCostInfo(String poolName);

    /**
     * Obtain the PoolInfo associated with a named pool.
     */
    @Nullable
    PoolInfo getPoolInfo(String pool);

    /**
     * Obtain the PoolInfo associated with a named list of pools.
     * <p>
     * Unknown or disabled pools are skipped.
     *
     * @param pools pool names
     * @return Map from pool name to PostInfo corresponding to the named pools
     */
    Map<String, PoolInfo> getPoolInfoAsMap(Iterable<String> pools);

    /**
     * Obtain the n-percentile performance cost, that is the cost of the nth pool when they have
     * been sorted in increasing performance cost and n = floor( fraction * numberOfPools).
     *
     * @return the n-th percentile performance cost, or 0 if no pools are known.
     * @throws IllegalArgumentException if fraction <= 0 or >= 1
     */
    double getPoolsPercentilePerformanceCost(double fraction);

    /**
     * Obtain PoolCostInfo of all known pools
     *
     * @return Collection of all PoolCostInfos
     */
    Collection<PoolCostInfo> getPoolCostInfos();
}
