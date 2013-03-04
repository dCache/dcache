package org.dcache.pool.migration;

import diskCacheV111.util.RetentionPolicy;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.vehicles.FileAttributes;

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
        FileAttributes fileAttributes = entry.getFileAttributes();
        return fileAttributes.isDefined(FileAttribute.RETENTION_POLICY) &&
                _retentionPolicy.equals(fileAttributes.getRetentionPolicy());
    }
}
