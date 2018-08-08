package org.dcache.pool.migration;

import java.util.function.Predicate;
import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter wrapping another filter. It accepts all
 * entries not accepted by the other filter.
 */
public class NotFilter implements Predicate<CacheEntry>
{
    private final Predicate<CacheEntry> _filter;

    public NotFilter(Predicate<CacheEntry> filter)
    {
        _filter = filter;
    }

    @Override
    public boolean test(CacheEntry entry)
    {
        return !_filter.test(entry);
    }
}
