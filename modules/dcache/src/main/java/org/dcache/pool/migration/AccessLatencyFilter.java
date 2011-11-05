package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

import diskCacheV111.util.AccessLatency;
import diskCacheV111.vehicles.StorageInfo;

/**
 * Repository entry filter which only accepts files with a certain
 * access lantecy.
 */
public class AccessLatencyFilter implements CacheEntryFilter
{
    private final AccessLatency _accessLatency;

    public AccessLatencyFilter(AccessLatency accessLatency)
    {
        _accessLatency = accessLatency;
    }

    public boolean accept(CacheEntry entry)
    {
        StorageInfo info = entry.getStorageInfo();
        return info != null && _accessLatency.equals(info.getAccessLatency());
    }
}