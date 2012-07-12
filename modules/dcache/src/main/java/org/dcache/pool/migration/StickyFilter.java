package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries that are sticky.
 */
public class StickyFilter implements CacheEntryFilter
{
    @Override
    public boolean accept(CacheEntry entry)
    {
        return entry.isSticky();
    }
}