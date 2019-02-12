package org.dcache.pool.migration;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.vehicles.PoolManagerGetPoolsMessage;
import diskCacheV111.vehicles.PoolManagerPoolInformation;

import org.dcache.cells.AbstractMessageCallback;

public abstract class PoolListFromPoolManager
    extends AbstractMessageCallback<PoolManagerGetPoolsMessage>
    implements RefreshablePoolList
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(PoolListFromPoolManager.class);

    protected ImmutableList<PoolManagerPoolInformation> _pools =
        ImmutableList.of();

    protected ImmutableList<String> _offlinePools =
        ImmutableList.of();

    protected boolean _isValid;

    @Override
    public synchronized boolean isValid()
    {
        return _isValid;
    }

    @Override
    public synchronized ImmutableList<String> getOfflinePools()
    {
        return _offlinePools;
    }

    @Override
    public synchronized ImmutableList<PoolManagerPoolInformation> getPools()
    {
        return _pools;
    }

    @Override
    public synchronized void success(PoolManagerGetPoolsMessage msg)
    {
        _pools = ImmutableList.copyOf(msg.getPools());
        _offlinePools = ImmutableList.copyOf(msg.getOfflinePools());
        _isValid = true;
    }

    @Override
    public void failure(int rc, Object error)
    {
        LOGGER.error("Failed to query pool manager ({})", error );
    }
}
