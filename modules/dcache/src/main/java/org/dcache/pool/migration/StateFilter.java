package org.dcache.pool.migration;

import java.util.function.Predicate;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;

/**
 * Repository entry filter accepting entries in particular states.
 */
public class StateFilter implements Predicate<CacheEntry>
{
    private final ReplicaState[] _states;

    public StateFilter(ReplicaState... states)
    {
        _states = states;
    }

    @Override
    public boolean test(CacheEntry entry)
    {
        for (ReplicaState state: _states) {
            if (entry.getState() == state) {
                return true;
            }
        }
        return false;
    }
}
