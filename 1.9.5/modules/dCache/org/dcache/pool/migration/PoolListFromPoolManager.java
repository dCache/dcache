package org.dcache.pool.migration;


import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import java.util.List;
import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;

import dmg.cells.nucleus.CellPath;

import org.apache.log4j.Logger;

public abstract class PoolListFromPoolManager
    implements RefreshablePoolList,
               MessageCallback<PoolManagerGetPoolsMessage>
{
    private static final Logger _log =
        Logger.getLogger(PoolListFromPoolManager.class);

    private final double _spaceFactor;
    private final double _cpuFactor;
    protected final Collection<Pattern> _exclude;
    protected List<PoolCostPair> _pools = Collections.emptyList();

    public PoolListFromPoolManager(Collection<Pattern> exclude,
                                   double spaceFactor,
                                   double cpuFactor)
    {
        _exclude = exclude;
        _spaceFactor = spaceFactor;
        _cpuFactor = cpuFactor;
    }

    synchronized public List<PoolCostPair> getPools()
    {
        return _pools;
    }

    synchronized protected void setPools(List<PoolCostPair> pools)
    {
        _pools = Collections.unmodifiableList(pools);
    }

    private boolean isExcluded(String pool)
    {
        for (Pattern pattern: _exclude) {
            if (pattern.matcher(pool).matches()) {
                return true;
            }
        }
        return false;
    }

    public void success(PoolManagerGetPoolsMessage msg)
    {
        List<PoolCostPair> pools =
            new ArrayList<PoolCostPair>(msg.getPools().size());
        for (PoolManagerPoolInformation pool: msg.getPools()) {
            String name = pool.getName();
            if (!isExcluded(name)) {
                double space =
                    (_spaceFactor > 0 ? pool.getSpaceCost() * _spaceFactor : 0);
                double cpu =
                    (_cpuFactor > 0 ? pool.getCpuCost() * _cpuFactor : 0);
                pools.add(new PoolCostPair(new CellPath(pool.getName()),
                                           space + cpu));
            }
        }
        setPools(pools);
    }

    public void failure(int rc, Object error)
    {
        _log.error("Failed to query pool manager "
                   + error + ")");
    }

    public void noroute()
    {
        _log.error("No route to pool manager");
    }

    public void timeout()
    {
        _log.error("Pool manager timeout");
    }
}
