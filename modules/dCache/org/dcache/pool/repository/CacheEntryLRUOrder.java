package org.dcache.pool.repository;

import java.util.Comparator;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;

/**
 * Defined LRU order for cache entries.
 */
public class CacheEntryLRUOrder implements Comparator<CacheRepositoryEntry>
{
    public int compare(CacheRepositoryEntry e1, CacheRepositoryEntry e2)
    {
        try {
            long l1 = e1.getLastAccessTime();
            long l2 = e2.getLastAccessTime();
            return l1 == l2 
                ? e1.getPnfsId().compareTo(e2.getPnfsId()) 
                : l1 < l2 ? -1 : 1;
        } catch (CacheException e) {
            throw new RuntimeException("Bug. This should not happen.", e);
        }
    }
}
