package org.dcache.pool.migration;

import java.util.Comparator;

import org.dcache.pool.repository.CacheEntry;

class LruOrder implements Comparator<CacheEntry>
{
    @Override
    public int compare(CacheEntry e1, CacheEntry e2)
    {
        long diff = e1.getLastAccessTime() - e2.getLastAccessTime();
        return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    }
}
