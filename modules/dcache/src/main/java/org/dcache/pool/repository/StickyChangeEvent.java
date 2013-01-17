package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to a sticky record of a
 * repository entry.
 */
public class StickyChangeEvent extends EntryChangeEvent
{
    private final StickyRecord _sticky;

    public StickyChangeEvent(CacheEntry entry, StickyRecord sticky)
    {
        super(entry);
        _sticky = sticky;
    }

    /**
     * Returns the state of the change sticky record after the
     * update. Any sticky record with an expiration time in the past
     * should be considered removed from the entry.
     */
    public StickyRecord getStickyRecord()
    {
        return _sticky;
    }

    public String toString()
    {
        return
            String.format("StickyRecordChangeEvent [id=%s,sticky=%s]",
                          getPnfsId(), _sticky);
    }
}
