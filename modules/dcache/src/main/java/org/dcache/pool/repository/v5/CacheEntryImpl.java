package org.dcache.pool.repository.v5;

import java.util.Collection;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

public class CacheEntryImpl implements CacheEntry
{
    private final long _size;
    private final ReplicaState _state;
    private final long _created_at;
    private final long _accessed_at;
    private final int _linkCount;
    private final boolean _isSticky;
    private final Collection<StickyRecord> _sticky;
    private final FileAttributes _fileAttributes;

    public CacheEntryImpl(ReplicaRecord entry) throws CacheException
    {
        synchronized (entry) {
            _size = entry.getReplicaSize();
            _created_at = entry.getCreationTime();
            _accessed_at = entry.getLastAccessTime();
            _linkCount = entry.getLinkCount();
            _isSticky = entry.isSticky();
            _sticky = entry.stickyRecords();
            _state = entry.getState();
            _fileAttributes = entry.getFileAttributes();
        }
    }

    /**
     * @see CacheEntry#getPnfsId()
     */
    @Override
    public PnfsId getPnfsId()
    {
        return _fileAttributes.getPnfsId();
    }

    /**
     * @see CacheEntry#getReplicaSize()
     */
    @Override
    public long getReplicaSize()
    {
        return _size;
    }

    @Override
    public FileAttributes getFileAttributes()
    {
        return _fileAttributes;
    }

    /**
     * @see CacheEntry#getState()
     */
    @Override
    public ReplicaState getState()
    {
        return _state;
    }

    /**
     * @see CacheEntry#getCreationTime()
     */
    @Override
    public long getCreationTime()
    {
        return _created_at;
    }

    /**
     * @see CacheEntry#getLastAccessTime()
     */
    @Override
    public long getLastAccessTime()
    {
        return _accessed_at;
    }

    /**
     * @see CacheEntry#getLinkCount()
     */
    @Override
    public int getLinkCount()
    {
        return _linkCount;
    }

    /**
     * @see CacheEntry#isSticky()
     */
    @Override
    public boolean isSticky()
    {
        return _isSticky;
    }

    /**
     * @see CacheEntry#getStickyRecords()
     */
    @Override
    public Collection<StickyRecord> getStickyRecords()
    {
        return _sticky;
    }

    @Override
    public String toString()
    {

        return getPnfsId() +
               " <" +
               ((_state == ReplicaState.CACHED) ? "C" : "-") +
               ((_state == ReplicaState.PRECIOUS) ? "P" : "-") +
               ((_state == ReplicaState.FROM_CLIENT) ? "C" : "-") +
               ((_state == ReplicaState.FROM_STORE) ? "S" : "-") +
               "-" +
               "-" +
               ((_state == ReplicaState.REMOVED) ? "R" : "-") +
               ((_state == ReplicaState.DESTROYED) ? "D" : "-") +
               (isSticky() ? "X" : "-") +
               ((_state == ReplicaState.BROKEN) ? "E" : "-") +
               "-" +
               "L(0)[" + _linkCount + "]" +
               "> " +
               _size +
               " si={" +
               (_fileAttributes.isDefined(FileAttribute.STORAGECLASS)
                     ? _fileAttributes.getStorageClass()
                     : "<unknown>") +
               "}";
    }
}
