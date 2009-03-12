package org.dcache.pool.repository;

import java.util.Comparator;

/**
 * Defined LRU order for cache entries. Removable entries are ordered
 * before non-removable entries.
 */
public class MetaDataLRUOrder implements Comparator<MetaDataRecord>
{
    private final SpaceSweeperPolicy _policy;

    public MetaDataLRUOrder(SpaceSweeperPolicy policy)
    {
        _policy = policy;
    }

    public int compare(MetaDataRecord e1, MetaDataRecord e2)
    {
        boolean r1 = _policy.isRemovable(e1);
        boolean r2 = _policy.isRemovable(e2);

        if (r1 && !r2)
            return -1;
        if (!r1 && r2)
            return 1;

        long l1 = e1.getLastAccessTime();
        long l2 = e2.getLastAccessTime();
        return l1 == l2
            ? e1.getPnfsId().compareTo(e2.getPnfsId())
            : l1 < l2 ? -1 : 1;
    }
}
