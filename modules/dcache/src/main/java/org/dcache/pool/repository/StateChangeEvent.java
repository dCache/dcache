package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to the state of a
 * repository entry.
 */
public class StateChangeEvent extends EntryChangeEvent
{
    private final ReplicaState _oldState;
    private final ReplicaState _newState;

    public StateChangeEvent(CacheEntry oldEntry, CacheEntry newEntry, ReplicaState oldState, ReplicaState newState)
    {
        super(oldEntry, newEntry);
        _oldState = oldState;
        _newState = newState;
    }

    public ReplicaState getOldState()
    {
        return _oldState;
    }

    public ReplicaState getNewState()
    {
        return _newState;
    }

    public String toString()
    {
        return
            String.format("StateChangedEvent [id=%s,oldState=%s,newState=%s]",
                          getPnfsId(), _oldState, _newState);
    }
}
