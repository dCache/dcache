package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;

/**
 * Repository entry filter accepting entries in a particular state.
 */
public class StateFilter implements CacheEntryFilter
{
    private final EntryState _state;

    public StateFilter(EntryState state)
    {
        _state = state;
    }

    public boolean accept(CacheEntry entry)
    {
        return entry.getState() == _state;
    }
}