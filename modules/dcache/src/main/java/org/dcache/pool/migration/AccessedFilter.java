package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import com.google.common.collect.Range;

/**
 * Repository entry filter which only accepts entries accessed within
 * a given interval of time.
 */
public class AccessedFilter implements CacheEntryFilter
{
    private Range<Long> _time;

    /**
     * Creates a new instance. The interval is specified in seconds
     * and is relative to the current time at evaluation.
     */
    public AccessedFilter(Range<Long> time)
    {
        _time = time;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        long lastAccess =
            System.currentTimeMillis() - entry.getLastAccessTime();
        return _time.contains(lastAccess / 1000);
    }
}