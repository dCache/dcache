package org.dcache.pool.migration;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;

import diskCacheV111.util.AccessLatency;
import org.dcache.vehicles.FileAttributes;

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

    @Override
    public boolean accept(CacheEntry entry)
    {
        FileAttributes attributes = entry.getFileAttributes();
        return attributes.isDefined(FileAttribute.ACCESS_LATENCY) && _accessLatency.equals(attributes.getAccessLatency());
    }
}
