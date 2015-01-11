package org.dcache.pool.repository.v5;

import java.util.Collection;

import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

public class CacheEntryImpl implements CacheEntry
{
    private final long _size;
    private final EntryState _state;
    private final long _created_at;
    private final long _accessed_at;
    private final int _linkCount;
    private final boolean _isSticky;
    private final Collection<StickyRecord> _sticky;
    private final FileAttributes _fileAttributes;

    public CacheEntryImpl(MetaDataRecord entry)
    {
        synchronized (entry) {
            _size = entry.getSize();
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
    public EntryState getState()
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
        StringBuilder sb = new StringBuilder();

        sb.append(getPnfsId());

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
            .append(_fileAttributes.isDefined(FileAttribute.STORAGECLASS)
                            ? _fileAttributes.getStorageClass()
                            : "<unknown>")
            .append("}");
        return sb.toString();
    }
}
