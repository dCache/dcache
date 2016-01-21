package org.dcache.pool.repository;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.repository.v5.CacheEntryImpl;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkState;
import static org.dcache.pool.repository.EntryState.DESTROYED;
import static org.dcache.pool.repository.EntryState.NEW;

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
 *
 * The cache submits state change events to a StateChangeListener. The
 * listener is called from the thread making the modification and with
 * the MetaDataRecord locked. Care must be taken in the listener to
 * not cause deadlocks or slow down the store.
 */
public class MetaDataCache
    implements MetaDataStore
{
    private static final float LOAD_FACTOR = 0.75f;

    /** Map of cached MetaDataRecords.
     */
    private final ConcurrentMap<PnfsId,Monitor> _entries;

    private final MetaDataStore _inner;
    private final StateChangeListener _stateChangeListener;

    private volatile boolean _isClosed;

    /**
     * Constructs a new cache.
     *
     * The operation may be slow as the list method of {@code inner} is called.
     */
    public MetaDataCache(MetaDataStore inner, StateChangeListener stateChangeListener) throws CacheException
    {
        _inner = inner;
        _stateChangeListener = stateChangeListener;

        Collection<PnfsId> list = inner.index();
        _entries = new ConcurrentHashMap<>(
                (int)(list.size() / LOAD_FACTOR + 1), LOAD_FACTOR);
        for (PnfsId id: list) {
            _entries.put(id, new Monitor(id));
        }
    }

    /**
     * Encapsulates operations on meta data records, ensuring sequential
     * access to any particular record. The class delegates operations to
     * both the store and to the decorated MetaDataRecord.
     *
     * Correctness follows by observing that
     *
     * 1. after an initial check it is guaranteed that
     *
     *        _entries[this._id] == this
     *
     *    it follows that for any given id, the condition can only be true
     *    for one Monitor at a time
     *
     * 2. all the monitor methods are synchronized, thus ensuring that for any
     *    given id only one thread at a time will get past the above check
     *
     * 3. all modifications of _entries or the Monitor happen from within
     *    the Monitor itself, only after the above condition has been
     *    established, and only the entry of this._id is modified, thus
     *    ensuring that the condition stays true until the end of the method
     *    or until the Monitor removes itself from _entries.
     *
     * The point from which the condition in item 1 is true is marked by
     * assertions in the code.
     */
    private class Monitor implements MetaDataRecord
    {
        private final PnfsId _id;
        private MetaDataRecord _record;

        private Monitor(PnfsId id)
        {
            _id = id;
        }

        private synchronized MetaDataRecord get()
                throws InterruptedException, CacheException
        {
            if (_entries.get(_id) != this) {
                return null;
            }
            assert _entries.get(_id) == this;
            if (_record == null) {
                _record = _inner.get(_id);
                if (_record == null) {
                    _entries.remove(_id, this);
                    return null;
                }
                CacheEntry entry = new CacheEntryImpl(_record);
                _stateChangeListener.stateChanged(
                        new StateChangeEvent(entry, entry, NEW, _record.getState()));
            }
            return this;
        }

        private synchronized MetaDataRecord create()
                throws CacheException
        {
            if (_entries.putIfAbsent(_id, this) != null) {
                throw new DuplicateEntryException(_id);
            }
            assert _entries.get(_id) == this;
            try {
                checkState(!_isClosed);
                _record = _inner.create(_id);
            } catch (DuplicateEntryException e) {
                _inner.remove(_id);
                _record = _inner.create(_id);
            } finally {
                if (_record == null) {
                    _entries.remove(_id);
                }
            }
            return (_record == null) ? null : this;
        }

        private synchronized MetaDataRecord create(MetaDataRecord entry)
                throws CacheException
        {
            if (_entries.putIfAbsent(_id, this) != null) {
                throw new DuplicateEntryException(_id);
            }
            assert _entries.get(_id) == this;
            try {
                checkState(!_isClosed);
                _record = _inner.create(entry);
            } catch (DuplicateEntryException e) {
                _inner.remove(_id);
                _record = _inner.create(_id);
            } finally {
                if (_record == null) {
                    _entries.remove(_id);
                }
            }
            return (_record == null) ? null : this;
        }

        private synchronized void remove() throws CacheException
        {
            if (_entries.get(_id) == this) {
                assert _entries.get(_id) == this;
                CacheEntry entry = new CacheEntryImpl(_record);
                _record.setState(DESTROYED);
                _inner.remove(_id);
                _entries.remove(_id);
                _stateChangeListener.stateChanged(
                        new StateChangeEvent(entry, entry, entry.getState(), DESTROYED));
            }
        }

        private synchronized void close()
        {
            _entries.remove(_id, this);
        }

        @Override
        public PnfsId getPnfsId()
        {
            return _id;
        }

        @Override
        public void setSize(long size)
        {
            _record.setSize(size);
        }

        @Override
        public long getSize()
        {
            return _record.getSize();
        }

        @Override
        public void setFileAttributes(FileAttributes attributes) throws CacheException
        {
            _record.setFileAttributes(attributes);
        }

        @Override
        public FileAttributes getFileAttributes() throws CacheException
        {
            return _record.getFileAttributes();
        }

        @Override
        public synchronized void setState(EntryState state) throws CacheException
        {
            if (_record.getState() != state) {
                CacheEntry oldEntry = new CacheEntryImpl(_record);
                _record.setState(state);
                CacheEntry newEntry = new CacheEntryImpl(_record);
                _stateChangeListener.stateChanged(
                        new StateChangeEvent(oldEntry, newEntry, oldEntry.getState(), newEntry.getState()));
            }
        }

        @Override
        public EntryState getState()
        {
            return _record.getState();
        }

        @Override
        public File getDataFile()
        {
            return _record.getDataFile();
        }

        @Override
        public long getCreationTime()
        {
            return _record.getCreationTime();
        }

        @Override
        public long getLastAccessTime()
        {
            return _record.getLastAccessTime();
        }

        @Override
        public void setLastAccessTime(long time) throws CacheException
        {
            _record.setLastAccessTime(time);
        }

        @Override
        public synchronized void touch() throws CacheException
        {
            CacheEntry oldEntry = new CacheEntryImpl(_record);
            _record.touch();
            CacheEntryImpl newEntry = new CacheEntryImpl(_record);
            _stateChangeListener.accessTimeChanged(new EntryChangeEvent(oldEntry, newEntry));
        }

        @Override
        public void decrementLinkCount()
        {
            _record.decrementLinkCount();
        }

        @Override
        public void incrementLinkCount()
        {
            _record.incrementLinkCount();
        }

        @Override
        public int getLinkCount()
        {
            return _record.getLinkCount();
        }

        @Override
        public boolean isSticky()
        {
            return _record.isSticky();
        }

        @Override
        public synchronized Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
        {
            CacheEntry oldEntry = new CacheEntryImpl(_record);
            Collection<StickyRecord> removed = _record.removeExpiredStickyFlags();
            if (!removed.isEmpty()) {
                CacheEntryImpl newEntry = new CacheEntryImpl(_record);
                _stateChangeListener.stickyChanged(new StickyChangeEvent(oldEntry, newEntry));
            }
            return removed;
        }

        @Override
        public synchronized boolean setSticky(String owner, long validTill, boolean overwrite) throws CacheException
        {
            CacheEntry oldEntry = new CacheEntryImpl(_record);
            boolean changed = _record.setSticky(owner, validTill, overwrite);
            if (changed) {
                CacheEntryImpl newEntry = new CacheEntryImpl(_record);
                _stateChangeListener.stickyChanged(new StickyChangeEvent(oldEntry, newEntry));
            }
            return changed;
        }

        @Override
        public Collection<StickyRecord> stickyRecords()
        {
            return _record.stickyRecords();
        }
    }

    public MetaDataRecord get(PnfsId id)
            throws CacheException, InterruptedException
    {
        Monitor monitor = _entries.get(id);
        return (monitor != null) ? monitor.get() : null;
    }

    @Override
    public MetaDataRecord create(PnfsId id) throws CacheException
    {
        return new Monitor(id).create();
    }

    @Override
    public MetaDataRecord create(MetaDataRecord entry)
            throws CacheException
    {
        return new Monitor(entry.getPnfsId()).create(entry);
    }

    @Override
    public void remove(PnfsId id) throws CacheException
    {
        Monitor monitor = _entries.get(id);
        if (monitor != null) {
            monitor.remove();
        }
    }

    @Override
    public Set<PnfsId> index()
    {
        return Collections.unmodifiableSet(_entries.keySet());
    }

    @Override
    public boolean isOk()
    {
        return _inner.isOk();
    }

    @Override
    public void close()
    {
        _isClosed = true;
        for (Monitor monitor : _entries.values()) {
            monitor.close();
        }
        _inner.close();
    }

    @Override
    public long getFreeSpace()
    {
        return _inner.getFreeSpace();
    }

    @Override
    public long getTotalSpace()
    {
        return _inner.getTotalSpace();
    }
}
