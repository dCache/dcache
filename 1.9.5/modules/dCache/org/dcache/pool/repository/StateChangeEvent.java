package org.dcache.pool.repository;

/**
 * Encapsulates information describing changes to the state of a
 * repository entry.
 */
public class StateChangeEvent extends EntryChangeEvent
{
    private final EntryState _oldState;
    private final EntryState _newState;

    public StateChangeEvent(CacheEntry entry, EntryState oldState, EntryState newState)
    {
        super(entry);
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