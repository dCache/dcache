package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;

/**
 * Repository entry filter accepting entries in particular states.
 */
public class StateFilter implements CacheEntryFilter
{
    private final EntryState[] _states;

    public StateFilter(EntryState... states)
    {
        _states = states;
    }

    public boolean accept(CacheEntry entry)
    {
        for (EntryState state: _states) {
            if (entry.getState() == state) {
                return true;
            }
        }
        return false;
    }
}