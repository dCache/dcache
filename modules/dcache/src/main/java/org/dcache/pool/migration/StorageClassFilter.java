package org.dcache.pool.migration;

import diskCacheV111.vehicles.StorageInfo;

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

    @Override
    public boolean accept(CacheEntry entry)
    {
        StorageInfo info = entry.getFileAttributes().getStorageInfo();
        return _sc.equals(info.getStorageClass());
    }
}
