package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

/**
 * Repository entry filter accepting entries with a particular storage
 * class.
 */
public class StorageClassFilter implements CacheEntryFilter
{
    private final String _sc;

    public StorageClassFilter(String sc)
    {
        _sc = sc;
    }

    public boolean accept(CacheEntry entry)
    {
        return _sc.equals(entry.getStorageInfo().getStorageClass());
    }
}