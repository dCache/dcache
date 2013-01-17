package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

/**
 * A "EntryChange" event gets delivered whenever state, sticky flags
 * or access time of a repository entry changes.
 */
public class EntryChangeEvent
{
    protected CacheEntry _entry;

    public EntryChangeEvent(CacheEntry entry)
    {
        _entry = entry;
    }

    public PnfsId getPnfsId()
    {
        return _entry.getPnfsId();
    }

    public CacheEntry getEntry()
    {
        return _entry;
    }

    public String toString()
    {
        return String.format("EntryChangeEvent [id=%s]", getPnfsId());
    }
}
