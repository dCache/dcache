package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

/**
 * A "EntryChange" event gets delivered whenever state, sticky flags
 * or access time of a repository entry changes.
 */
public class EntryChangeEvent
{
    protected final CacheEntry oldEntry;
    protected final CacheEntry newEntry;

    public EntryChangeEvent(CacheEntry oldEntry, CacheEntry newEntry)
    {
        this.oldEntry = oldEntry;
        this.newEntry = newEntry;
    }

    public PnfsId getPnfsId()
    {
        return newEntry.getPnfsId();
    }

    public CacheEntry getOldEntry()
    {
        return oldEntry;
    }

    public CacheEntry getNewEntry()
    {
        return newEntry;
    }

    public String toString()
    {
        return String.format("EntryChangeEvent [id=%s]", getPnfsId());
    }
}
