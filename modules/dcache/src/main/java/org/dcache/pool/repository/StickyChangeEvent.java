package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to a sticky record of a
 * repository entry.
 */
public class StickyChangeEvent extends EntryChangeEvent
{
    public StickyChangeEvent(CacheEntry oldEntry, CacheEntry newEntry)
    {
        super(oldEntry, newEntry);
    }

    public String toString()
    {
        return String.format("StickyRecordChangeEvent [id=%s]", getPnfsId());
    }
}
