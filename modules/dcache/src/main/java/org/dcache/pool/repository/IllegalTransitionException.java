package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

public class IllegalTransitionException
    extends Exception
{
    private static final long serialVersionUID = 3255241915388346655L;
    private final PnfsId _pnfsId;
    private final EntryState _source;
    private final EntryState _target;

    public IllegalTransitionException(PnfsId pnfsId,
                                      EntryState source, EntryState target)
    {
        _pnfsId = pnfsId;
        _source = source;
        _target = target;
    }

    public EntryState getSourceState()
    {
        return _source;
    }

    public EntryState getTargetState()
    {
        return _target;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    @Override
    public String getMessage()
    {
        return "Transition from " + _source + " to " + _target
            + " is not allowed";
    }
}
