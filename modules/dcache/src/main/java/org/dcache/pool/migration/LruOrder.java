package org.dcache.pool.migration;

import java.util.Comparator;
import org.dcache.pool.repository.CacheEntry;

class LruOrder implements Comparator<CacheEntry> {

    @Override
    public int compare(CacheEntry e1, CacheEntry e2) {
        return Long.compare(e1.getLastAccessTime(), e2.getLastAccessTime());
    }
}
