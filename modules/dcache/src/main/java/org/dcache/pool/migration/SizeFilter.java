package org.dcache.pool.migration;

import com.google.common.collect.Range;
import java.util.function.Predicate;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter which only accepts entries with a size in a
 * given range.
 */
public class SizeFilter implements Predicate<CacheEntry>
{
    private final Range<Long> _size;

    public SizeFilter(Range<Long> size)
    {
        _size = size;
    }

    @Override
    public boolean test(CacheEntry entry)
    {
        return _size.contains(entry.getReplicaSize());
    }
}
