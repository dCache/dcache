package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import diskCacheV111.vehicles.StorageInfo;

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
        StorageInfo info = entry.getStorageInfo();
        return info != null && _sc.equals(info.getStorageClass());
    }
}