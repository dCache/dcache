package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.util.Interval;

/**
 * Repository entry filter which only accepts entries with a size in a
 * given range.
 */
public class SizeFilter implements CacheEntryFilter
{
    private final Interval _size;

    public SizeFilter(Interval size)
    {
        _size = size;
    }

    public boolean accept(CacheEntry entry)
    {
        return _size.contains(entry.getReplicaSize());
    }
}