package org.dcache.pool.repository;

import java.util.Comparator;

/**
 * LRU order for cache entries.
 */
public class MetaDataLRUOrder implements Comparator<MetaDataRecord>
{
    @Override
    public int compare(MetaDataRecord e1, MetaDataRecord e2)
    {
        long l1 = e1.getLastAccessTime();
        long l2 = e2.getLastAccessTime();
        return l1 == l2
            ? e1.getPnfsId().compareTo(e2.getPnfsId())
            : l1 < l2 ? -1 : 1;
    }
}
