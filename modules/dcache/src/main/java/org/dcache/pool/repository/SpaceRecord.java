package org.dcache.pool.repository;

/**
 * Immutable class containing information about the allocation in a
 * cache repository.
 */
public class SpaceRecord
{
    private final long _totalSize;
    private final long _freeSpace;
    private final long _preciousSpace;
    private final long _removableSpace;
    private final long _lru;

    public SpaceRecord(long total, long free, long precious, long removable,
                       long lru)
    {
        _totalSize = total;
        _freeSpace = free;
        _preciousSpace = precious;
        _removableSpace = removable;
        _lru = lru;
    }

    public long getTotalSpace()
    {
        return _totalSize;
    }

    public long getFreeSpace()
    {
        return _freeSpace;
    }

    public long getPreciousSpace()
    {
        return _preciousSpace;
    }

    public long getRemovableSpace()
    {
        return _removableSpace;
    }

    public long getLRU()
    {
        return _lru;
    }

    @Override
    public String toString()
    {
        return String.format("[total=%d;free=%d;precious=%d;removable=%d;lru=%d]", _totalSize, _freeSpace, _preciousSpace, _removableSpace, _lru);
    }
}