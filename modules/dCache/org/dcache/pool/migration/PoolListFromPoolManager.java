package org.dcache.pool.migration;

import java.util.List;
import java.util.ArrayList;

import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.util.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PoolListFromPoolManager
    implements RefreshablePoolList,
               MessageCallback<PoolManagerGetPoolsMessage>
{
    private static final Logger _log =
        LoggerFactory.getLogger(PoolListFromPoolManager.class);

    protected ImmutableList<PoolManagerPoolInformation> _pools =
        new ImmutableList(new ArrayList<PoolManagerPoolInformation>());

    synchronized public ImmutableList<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }

    public void success(PoolManagerGetPoolsMessage msg)
    {
        _pools = new ImmutableList(new ArrayList(msg.getPools()));
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
