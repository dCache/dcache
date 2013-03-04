package org.dcache.pool.migration;

import java.util.Comparator;

import org.dcache.pool.repository.CacheEntry;

class SizeOrder implements Comparator<CacheEntry>
{
    @Override
    public int compare(CacheEntry e1, CacheEntry e2)
    {
        long diff = e1.getReplicaSize() - e2.getReplicaSize();
        return diff < 0 ? -1 : (diff > 0 ? 1 : 0);
    }
}
