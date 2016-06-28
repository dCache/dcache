package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.v3.RepositoryException;

public class IllegalTransitionException
    extends RepositoryException
{
    private static final long serialVersionUID = 3255241915388346655L;
    private final PnfsId _pnfsId;
    private final ReplicaState _source;
    private final ReplicaState _target;

    public IllegalTransitionException(PnfsId pnfsId,
                                      ReplicaState source, ReplicaState target)
    {
        super("Transition from " + source + " to " + target + " is not allowed");
        _pnfsId = pnfsId;
        _source = source;
        _target = target;
    }

    public ReplicaState getSourceState()
    {
        return _source;
    }

    public ReplicaState getTargetState()
    {
        return _target;
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }
}
