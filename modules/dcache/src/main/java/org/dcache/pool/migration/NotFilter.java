package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter wrapping another filter. It accepts all
 * entries not accepted by the other filter.
 */
public class NotFilter implements CacheEntryFilter
{
    private final CacheEntryFilter _filter;

    public NotFilter(CacheEntryFilter filter)
    {
        _filter = filter;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        return !_filter.accept(entry);
    }
}