package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to a sticky record of a repository entry.
 */
public class StickyChangeEvent extends EntryChangeEvent {

    public StickyChangeEvent(String why, CacheEntry oldEntry, CacheEntry newEntry) {
        super(why, oldEntry, newEntry);
    }

    public String toString() {
        return String.format("StickyRecordChangeEvent [id=%s]", getPnfsId());
    }
}
