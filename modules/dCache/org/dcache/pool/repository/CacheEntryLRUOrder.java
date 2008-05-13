package org.dcache.pool.repository;

import java.util.Comparator;
import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.util.CacheException;

/**
 * Defined LRU order for cache entries. Removable entries are ordered
 * before non-removable entries.
 */
public class CacheEntryLRUOrder implements Comparator<CacheRepositoryEntry>
{
    private boolean isRemovable(CacheRepositoryEntry entry)
    {
        try {
            synchronized (entry) {
                return !entry.isReceivingFromClient()
                    && !entry.isReceivingFromStore()
                    && !entry.isPrecious()
                    && !entry.isSticky()
                    && entry.isCached();
            }
        } catch (CacheException e) {
            /* Returning false is the safe option.
             */
            return false;
        }
    }

    public int compare(CacheRepositoryEntry e1, CacheRepositoryEntry e2)
    {
        try {
            boolean r1 = isRemovable(e1);
            boolean r2 = isRemovable(e2);

            if (r1 && !r2)
                return -1;
            if (!r1 && r2)
                return 1;

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
