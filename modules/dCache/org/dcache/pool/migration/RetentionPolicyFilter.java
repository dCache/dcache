package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

import diskCacheV111.util.RetentionPolicy;

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

    public boolean accept(CacheEntry entry)
    {
        return entry.getStorageInfo().getRetentionPolicy().equals(_retentionPolicy);
    }
}