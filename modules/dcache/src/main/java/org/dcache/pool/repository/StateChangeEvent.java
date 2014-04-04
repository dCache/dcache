package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to the state of a
 * repository entry.
 */
public class StateChangeEvent extends EntryChangeEvent
{
    private final EntryState _oldState;
    private final EntryState _newState;

    public StateChangeEvent(CacheEntry oldEntry, CacheEntry newEntry, EntryState oldState, EntryState newState)
    {
        super(oldEntry, newEntry);
        _oldState = oldState;
        _newState = newState;
    }

    public EntryState getOldState()
    {
        return _oldState;
    }

    public EntryState getNewState()
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
