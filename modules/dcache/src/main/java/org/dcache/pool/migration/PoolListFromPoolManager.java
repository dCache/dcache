package org.dcache.pool.migration;

import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;
import org.dcache.cells.AbstractMessageCallback;

import com.google.common.collect.ImmutableList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class PoolListFromPoolManager
    extends AbstractMessageCallback<PoolManagerGetPoolsMessage>
    implements RefreshablePoolList
{
    private static final Logger _log =
        LoggerFactory.getLogger(PoolListFromPoolManager.class);

    protected ImmutableList<PoolManagerPoolInformation> _pools =
        ImmutableList.of();

    protected boolean _isValid;

    @Override
    synchronized public boolean isValid()
    {
        return _isValid;
    }

    @Override
    synchronized public ImmutableList<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }

    @Override
    synchronized public void success(PoolManagerGetPoolsMessage msg)
    {
        _pools = ImmutableList.copyOf(msg.getPools());
        _isValid = true;
    }

    @Override
    public void failure(int rc, Object error)
    {
        _log.error("Failed to query pool manager (" + error + ")");
    }
}
