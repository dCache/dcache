package org.dcache.pool.migration;

import java.util.function.Predicate;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.StickyRecord;

/**
 * Repository entry filter accepting entries with sticky flags by a given owner.
 */
public class StickyOwnerFilter implements Predicate<CacheEntry> {

    private final String _owner;

    public StickyOwnerFilter(String owner) {
        _owner = owner;
    }

    @Override
    public boolean test(CacheEntry entry) {
        for (StickyRecord record : entry.getStickyRecords()) {
            if (record.owner().equals(_owner)) {
                return true;
            }
        }
        return false;
    }
}
