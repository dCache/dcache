package org.dcache.pool.repository;

public enum EntryState
{
    NEW(true),
    FROM_CLIENT(true),
    FROM_POOL(true),
    FROM_STORE(true),
    BROKEN(false),
    CACHED(false),
    PRECIOUS(false),
    REMOVED(false),
    DESTROYED(false);

    boolean isMutable;

    EntryState(boolean isMutable)
    {
        this.isMutable = isMutable;
    }

    public boolean isMutable()
    {
        return isMutable;
    }
}
