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
    @Deprecated
    DESTROYED    // Kept for backwards compatibility with pre-2.12 - drop after next golden + 1
}
