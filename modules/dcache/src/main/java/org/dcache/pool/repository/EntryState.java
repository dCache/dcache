package org.dcache.pool.repository;

public enum EntryState
{
    NEW,
    FROM_CLIENT,
    FROM_POOL,
    FROM_STORE,
    BROKEN,
    CACHED,
    PRECIOUS,
    REMOVED,
    DESTROYED;
}
