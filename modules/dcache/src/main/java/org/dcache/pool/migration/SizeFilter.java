package org.dcache.pool.migration;

import com.google.common.collect.Range;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter which only accepts entries with a size in a
 * given range.
 */
public class SizeFilter implements CacheEntryFilter
{
    private final Range<Long> _size;

    public SizeFilter(Range<Long> size)
    {
        _size = size;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        return _size.contains(entry.getReplicaSize());
    }
}
