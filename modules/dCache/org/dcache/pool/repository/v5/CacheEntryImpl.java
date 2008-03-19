package org.dcache.pool.repository.v5;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import diskCacheV111.repository.CacheRepositoryEntry;

import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;

public class CacheEntryImpl implements CacheEntry
{
    private final PnfsId _id;
    private final long _size;
    private final StorageInfo _info;
    private final EntryState _state;
    private final long _created_at;
    private final long _accessed_at;

    public CacheEntryImpl(CacheRepositoryEntry entry, EntryState state)
        throws CacheException
    {
        _id = entry.getPnfsId();
        _size = entry.getSize();
        _info = entry.getStorageInfo();
        _created_at = entry.getCreationTime();
        _accessed_at = entry.getLastAccessTime();
        _state = state;
    }

    /**
     * Get the PnfsId of this entry.
     */
    public PnfsId getPnfsId()
    {
        return _id;
    }

    /**
     * Get the size of the replica.
     */
    public long getReplicaSize()
    {
        return _size;
    }

    /**
     * Get the storage info of the related entry.
     * @return storage info of the entry or null if storage info is not available yet.
     */
    public StorageInfo getStorageInfo()
    {
        return _info;
    }

    /**
     *
     * @return entry state
     */
    public EntryState getState()
    {
        return _state;
    }

    /**
     *
     * @return entry creation time in milliseconds
     */
    public long getCreationTime()
    {
        return _created_at;
    }

    /**
     *
     * @return entry last access time in milliseconds.
     */
    public long getLastAccessTime()
    {

        return _accessed_at;
    }
}