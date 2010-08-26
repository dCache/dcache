package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.util.Interval;

/**
 * Repository entry filter which only accepts entries accessed within
 * a given interval of time.
 */
public class AccessedFilter implements CacheEntryFilter
{
    private Interval _time;

    /**
     * Creates a new instance. The interval is specified in seconds
     * and is relative to the current time at evaluation.
     */
    public AccessedFilter(Interval time)
    {
        _time = time;
    }

    public boolean accept(CacheEntry entry)
    {
        long lastAccess =
            System.currentTimeMillis() - entry.getLastAccessTime();
        return _time.contains(lastAccess / 1000);
    }
}