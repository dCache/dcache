package org.dcache.pool.migration;

import javax.annotation.Nullable;

import java.util.Objects;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries with a particular cache
 * class.
 */
public class CacheClassFilter implements CacheEntryFilter
{
    private final String _cc;

    public CacheClassFilter(@Nullable String cc)
    {
        _cc = cc;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        return Objects.equals(_cc, entry.getFileAttributes().getCacheClass());
    }
}
