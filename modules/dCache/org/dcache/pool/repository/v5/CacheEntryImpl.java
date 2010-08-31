package org.dcache.pool.repository.v5;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.StickyRecord;

import java.util.Collection;

public class CacheEntryImpl implements CacheEntry
{
    private final PnfsId _id;
    private final long _size;
    private final StorageInfo _info;
    private final EntryState _state;
    private final long _created_at;
    private final long _accessed_at;
    private final int _linkCount;
    private final boolean _isSticky;
    private final Collection<StickyRecord> _sticky;

    public CacheEntryImpl(MetaDataRecord entry)
        throws CacheException
    {
        synchronized (entry) {
            _id = entry.getPnfsId();
            _size = entry.getSize();
            _info = entry.getStorageInfo();
            _created_at = entry.getCreationTime();
            _accessed_at = entry.getLastAccessTime();
            _linkCount = entry.getLinkCount();
            _isSticky = entry.isSticky();
            _sticky = entry.stickyRecords();
            _state = entry.getState();
        }
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getPnfsId()
     */
    @Override
    public PnfsId getPnfsId()
    {
        return _id;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getReplicaSize()
     */
    @Override
    public long getReplicaSize()
    {
        return _size;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getStorageInfo()
     */
    @Override
    public StorageInfo getStorageInfo()
    {
        return _info;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getState()
     */
    @Override
    public EntryState getState()
    {
        return _state;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getCreationTime()
     */
    @Override
    public long getCreationTime()
    {
        return _created_at;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getLastAccessTime()
     */
    @Override
    public long getLastAccessTime()
    {
        return _accessed_at;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getLinkCount()
     */
    @Override
    public int getLinkCount()
    {
        return _linkCount;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#isSticky()
     */
    @Override
    public boolean isSticky()
    {
        return _isSticky;
    }

    /**
     * @see org.dcache.pool.repository.CacheEntry#getStickyRecords()
     */
    @Override
    public Collection<StickyRecord> getStickyRecords()
    {
        return _sticky;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append(_id);

        sb.append(" <");
        sb.append((_state == EntryState.CACHED)      ? "C" : "-");
        sb.append((_state == EntryState.PRECIOUS)    ? "P" : "-");
        sb.append((_state == EntryState.FROM_CLIENT) ? "C" : "-");
        sb.append((_state == EntryState.FROM_STORE)  ? "S" : "-");
        sb.append("-");
        sb.append("-");
        sb.append((_state == EntryState.REMOVED)     ? "R" : "-");
        sb.append((_state == EntryState.DESTROYED)   ? "D" : "-");
        sb.append(isSticky()                         ? "X" : "-");
        sb.append((_state == EntryState.BROKEN)      ? "E" : "-");
        sb.append("-");
        sb.append("L(0)[").append(_linkCount).append("]");
        sb.append("> ");

        sb.append(_size);
        sb.append(" si={")
            .append(_info == null ? "<unknown>" : _info.getStorageClass())
            .append("}");
        return sb.toString();
    }
}