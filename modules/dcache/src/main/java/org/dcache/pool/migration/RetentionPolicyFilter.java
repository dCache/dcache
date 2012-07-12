package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

import diskCacheV111.util.RetentionPolicy;
import diskCacheV111.vehicles.StorageInfo;

/**
 * Repository entry filter which only accepts files with a certain
 * retention policy.
 */
public class RetentionPolicyFilter implements CacheEntryFilter
{
    private final RetentionPolicy _retentionPolicy;

    public RetentionPolicyFilter(RetentionPolicy retentionPolicy)
    {
        _retentionPolicy = retentionPolicy;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        StorageInfo info = entry.getStorageInfo();
        return info != null && _retentionPolicy.equals(info.getRetentionPolicy());
    }
}