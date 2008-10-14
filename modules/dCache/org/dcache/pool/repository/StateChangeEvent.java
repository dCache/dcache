package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

public class StateChangeEvent
{
    private final PnfsId _id;
    private final EntryState _oldState;
    private final EntryState _newState;

    public StateChangeEvent(PnfsId id, EntryState oldState, EntryState newState)
    {
        _id = id;
        _oldState = oldState;
        _newState = newState;
    }

    public PnfsId getPnfsId()
    {
        return _id;
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
                          _id, _oldState, _newState);
    }
}