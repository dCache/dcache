package org.dcache.pool.repository;

import java.util.Set;
import java.util.Map;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.CacheException;

/**
 * Cache of MetaDataRecords.
 *
 * The class decorates another MetaDataStore. The cache assumes that
 * it has exclusive access to the inner MetaDataStore. The inner
 * MetaDataStore must be thread-safe, however the cache will never
 * concurrently invoke several methods of the inner MetaDataStore
 * using the same PNFS ID.
 *
 * The cache guarantees that it always returns the same MetaDataRecord
 * instance for a given entry.
 */
public class MetaDataCache
    implements MetaDataStore
{
    /** Map of cached MetaDataRecords.
     */
    private final Map<PnfsId,MetaDataRecord> _entries =
        new ConcurrentHashMap<PnfsId,MetaDataRecord>();

    /**
     * Ids of entries not yet cached. Invariantly all ids of entries
     * in the inner MetaDataStore are in the union of this set and the
     * key set of the _entries map.
     */
    private final Set<PnfsId> _unread;

    /**
     * Ids of enties currently being read.
     */
    private final Set<PnfsId> _reading = new HashSet<PnfsId>();

    private final MetaDataStore _inner;

    /**
     * Constructs a new cache.
     *
     * The operation may be slow as the list method of inner is called.
     */
    public MetaDataCache(MetaDataStore inner)
    {
        _inner = inner;
        _unread = new HashSet(inner.list());
    }

    public synchronized Collection<PnfsId> list()
    {
        if (_unread.size() == 0) {
            return Collections.unmodifiableCollection(_entries.keySet());
        } else {
            Collection<PnfsId> ids = new HashSet<PnfsId>(_unread);
            ids.addAll(_entries.keySet());
            return ids;
        }
    }

    public MetaDataRecord get(PnfsId id)
        throws CacheException, InterruptedException
    {
        synchronized (this) {
            while (_reading.contains(id)) {
                wait();
            }
            if (!_unread.contains(id)) {
                return _entries.get(id);
            }
            _reading.add(id);
        }

        try {
            MetaDataRecord entry = _inner.get(id);
            synchronized (this) {
                if (entry != null) {
                    _entries.put(id, entry);
                }
                _unread.remove(id);
            }
            return entry;
        } finally {
            synchronized (this) {
                _reading.remove(id);
                notifyAll();
            }
        }
    }

    public synchronized MetaDataRecord create(PnfsId id)
        throws DuplicateEntryException, CacheException
    {
        if (_entries.containsKey(id) || _unread.contains(id)) {
            throw new DuplicateEntryException(id);
        }
        MetaDataRecord entry = _inner.create(id);
        _entries.put(id, entry);
        return entry;
    }

    public synchronized MetaDataRecord create(MetaDataRecord entry)
        throws DuplicateEntryException, CacheException
    {
        PnfsId id = entry.getPnfsId();
        if (_entries.containsKey(id) || _unread.contains(id)) {
            throw new DuplicateEntryException(id);
        }
        entry = _inner.create(entry);
        _entries.put(id, entry);
        return entry;
    }

    public synchronized void remove(PnfsId id)
    {
        boolean interrupted = false;
        while (_reading.contains(id)) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        _unread.remove(id);
        _inner.remove(id);
        _entries.remove(id);
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean isOk()
    {
        return _inner.isOk();
    }

    public synchronized void close()
    {
        boolean interrupted = false;
        while (!_reading.isEmpty()) {
            try {
                wait();
            } catch (InterruptedException e) {
                interrupted = true;
            }
        }
        _unread.clear();
        _inner.close();
        _entries.clear();
        if (interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public long getFreeSpace()
    {
        return _inner.getFreeSpace();
    }

    public long getTotalSpace()
    {
        return _inner.getTotalSpace();
    }
}