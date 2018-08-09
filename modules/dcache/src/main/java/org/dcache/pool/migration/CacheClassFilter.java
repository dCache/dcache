package org.dcache.pool.migration;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.function.Predicate;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries with a particular cache
 * class.
 */
public class CacheClassFilter implements Predicate<CacheEntry>
{
    private final String _cc;

    public CacheClassFilter(@Nullable String cc)
    {
        _cc = cc;
    }

    @Override
    public boolean test(CacheEntry entry) {
        return Objects.equals(_cc, entry.getFileAttributes().getCacheClass());
    }
}
