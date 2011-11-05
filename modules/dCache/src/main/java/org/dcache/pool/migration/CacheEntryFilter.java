package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;

/**
 * Interface for filtering repository entries.
 */
public interface CacheEntryFilter
{
    /**
     * Returns true if and only if the filter accepts the entry.
     */
    boolean accept(CacheEntry entry);
}