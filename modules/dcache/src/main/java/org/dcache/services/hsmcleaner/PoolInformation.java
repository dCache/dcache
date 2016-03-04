package org.dcache.services.hsmcleaner;

import java.util.Collection;
import java.util.Map;

import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolV2Mode;
import diskCacheV111.vehicles.PoolManagerPoolUpMessage;

/**
 * Encapsulates information about an HSM attached pool.
 */
public class PoolInformation
{
    /** Creation time. */
    private final long _created;

    /** Last PoolUp message received from the pool. */
    final PoolManagerPoolUpMessage _poolup;

    public PoolInformation(PoolManagerPoolUpMessage message)
    {
        _created = System.currentTimeMillis();
        _poolup = message;
    }

    /** Returns the pool name. */
    public String getName()
    {
        return _poolup.getPoolName();
    }

    /** Returns the pool mode. */
    public PoolV2Mode getMode()
    {
        return _poolup.getPoolMode();
    }

    /**
     * Returns the human readable status message of the pool. May be
     * null.
     */
    public String getMessage()
    {
        return _poolup.getMessage();
    }

    /**
     * Returns the machine interpretable status code of the
     * pool. Returns 0 if the status code has not been set.
     */
    public int getCode()
    {
        return _poolup.getCode();
    }

    /** Returns the names of HSM instances attached to the pool. */
    public Collection<String> getHsmInstances()
    {
        return _poolup.getHsmInstances();
    }

    /**
     * Returns the age in milliseconds of the pool information.
     */
    public long getAge()
    {
        return System.currentTimeMillis() - _created;
    }

    public Map<String, String> getTagMap()
    {
        return _poolup.getTagMap();
    }

    public PoolCostInfo getPoolCostInfo()
    {
        return _poolup.getPoolCostInfo();
    }

    public boolean isDisabled(int mask)
    {
        return _poolup.getPoolMode().isDisabled(mask);
    }

    /**
     * Returns if and only if the pool has been disabled.
     */
    public boolean isDisabled()
    {
        PoolV2Mode mode = getMode();
        return mode.getMode() == PoolV2Mode.DISABLED
            || mode.isDisabled(PoolV2Mode.DISABLED_DEAD)
            || mode.isDisabled(PoolV2Mode.DISABLED_STRICT);
    }
}
