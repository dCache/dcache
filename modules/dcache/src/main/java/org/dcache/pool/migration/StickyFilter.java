package org.dcache.pool.migration;

import java.util.function.Predicate;
import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries that are sticky.
 */
public class StickyFilter implements Predicate<CacheEntry>
{

    @Override
    public boolean test(CacheEntry entry)
    {
        return entry.isSticky();
    }
}
