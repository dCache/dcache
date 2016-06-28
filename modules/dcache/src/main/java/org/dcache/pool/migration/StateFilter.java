package org.dcache.pool.migration;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;

/**
 * Repository entry filter accepting entries in particular states.
 */
public class StateFilter implements CacheEntryFilter
{
    private final ReplicaState[] _states;

    public StateFilter(ReplicaState... states)
    {
        _states = states;
    }

    @Override
    public boolean accept(CacheEntry entry)
    {
        for (ReplicaState state: _states) {
            if (entry.getState() == state) {
                return true;
            }
        }
        return false;
    }
}
