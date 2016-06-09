package org.dcache.pool.repository;

import javax.annotation.concurrent.GuardedBy;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
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
    /** Map of cached MetaDataRecords.
     */
    private final ConcurrentMap<PnfsId,Monitor> _entries;

    private final MetaDataStore _inner;
    private final StateChangeListener _stateChangeListener;
    private final FaultListener _faultListener;

    private volatile boolean _isClosed;

    /**
     * Constructs a new cache.
     */
    public MetaDataCache(MetaDataStore inner, StateChangeListener stateChangeListener, FaultListener faultListener)
    {
        _inner = inner;
        _stateChangeListener = stateChangeListener;
        _faultListener = faultListener;
        _entries = new ConcurrentHashMap<>();
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
            if (_entries.get(_id) != this || _record != null) {
                throw new DuplicateEntryException(_id);
            }
            assert _entries.get(_id) == this;
            try {
                checkState(!_isClosed);
                _record = _inner.create(_id);
            } catch (DuplicateEntryException e) {
                throw e;
            } catch (RuntimeException | CacheException e) {
                _entries.remove(_id);
                throw e;
            }
            return this;
        }

        private synchronized MetaDataRecord create(MetaDataRecord entry)
                throws CacheException
        {
            if (_entries.get(_id) != this || _record != null) {
                throw new DuplicateEntryException(_id);
            }
            assert _entries.get(_id) == this;
            try {
                checkState(!_isClosed);
                _record = _inner.copy(entry);
            } catch (DuplicateEntryException e) {
                throw e;
            } catch (RuntimeException | CacheException e) {
                _entries.remove(_id);
                throw e;
            }
            return this;
        }

        @GuardedBy("this")
        private void destroy()
        {
            assert _entries.get(_id) == this;
            try {
                CacheEntry entry = new CacheEntryImpl(_record);
                _record.update(r -> r.setState(DESTROYED));
                _inner.remove(_id);
                _entries.remove(_id);
                _stateChangeListener.stateChanged(
                        new StateChangeEvent(entry, entry, entry.getState(), DESTROYED));
            } catch (DiskErrorCacheException | RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
            } catch (CacheException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.READONLY, "Internal repository error", e));
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
            try {
                _record.setSize(size);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public long getSize()
        {
            try {
                return _record.getSize();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public FileAttributes getFileAttributes() throws CacheException
        {
            try {
                return _record.getFileAttributes();
            } catch (RuntimeException | DiskErrorCacheException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public EntryState getState()
        {
            try {
                return _record.getState();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public File getDataFile()
        {
            try {
                return _record.getDataFile();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public long getCreationTime()
        {
            try {
                return _record.getCreationTime();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public long getLastAccessTime()
        {
            try {
                return _record.getLastAccessTime();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public void setLastAccessTime(long time) throws CacheException
        {
            try {
                _record.setLastAccessTime(time);
            } catch (IllegalArgumentException e) {
                throw e;
            } catch (RuntimeException | DiskErrorCacheException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public synchronized void touch() throws CacheException
        {
            try {
                CacheEntry oldEntry = new CacheEntryImpl(_record);
                _record.touch();
                CacheEntryImpl newEntry = new CacheEntryImpl(_record);
                _stateChangeListener.accessTimeChanged(new EntryChangeEvent(oldEntry, newEntry));
            } catch (RuntimeException | DiskErrorCacheException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public synchronized int decrementLinkCount()
        {
            int cnt = _record.decrementLinkCount();
            if (cnt == 0 && _record.getState() == EntryState.REMOVED) {
                destroy();
            }
            return cnt;
        }

        @Override
        public int incrementLinkCount()
        {
            try {
                return _record.incrementLinkCount();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public int getLinkCount()
        {
            try {
                return _record.getLinkCount();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public boolean isSticky()
        {
            try {
                return _record.isSticky();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public synchronized Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
        {
            try {
                CacheEntry oldEntry = new CacheEntryImpl(_record);
                Collection<StickyRecord> removed = _record.removeExpiredStickyFlags();
                if (!removed.isEmpty()) {
                    CacheEntryImpl newEntry = new CacheEntryImpl(_record);
                    _stateChangeListener.stickyChanged(new StickyChangeEvent(oldEntry, newEntry));
                }
                return removed;
            } catch (RuntimeException | DiskErrorCacheException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public Collection<StickyRecord> stickyRecords()
        {
            try {
                return _record.stickyRecords();
            } catch (RuntimeException e) {
                _faultListener.faultOccurred(
                        new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
                throw e;
            }
        }

        @Override
        public synchronized <T> T update(Update<T> update) throws CacheException
        {
            try {
                return _record.update(
                        r -> update.apply(
                                new UpdatableRecord()
                                {
                                    @Override
                                    public boolean setSticky(String owner, long validTill,
                                                             boolean overwrite) throws CacheException
                                    {
                                        CacheEntry oldEntry = new CacheEntryImpl(_record);
                                        boolean changed = r.setSticky(owner, validTill, overwrite);
                                        if (changed) {
                                            CacheEntryImpl newEntry = new CacheEntryImpl(_record);
                                            _stateChangeListener.stickyChanged(new StickyChangeEvent(oldEntry, newEntry));
                                        }
                                        return changed;
                                    }

                                    @Override
                                    public Void setState(EntryState state) throws CacheException
                                    {
                                        if (r.getState() != state) {
                                            CacheEntry oldEntry = new CacheEntryImpl(_record);
                                            r.setState(state);
                                            CacheEntry newEntry = new CacheEntryImpl(_record);
                                            _stateChangeListener.stateChanged(
                                                    new StateChangeEvent(oldEntry, newEntry, oldEntry.getState(),
                                                                         newEntry.getState()));
                                            if (r.getLinkCount() == 0 && state == EntryState.REMOVED) {
                                                destroy();
                                            }
                                        }
                                        return null;
                                    }

                                    @Override
                                    public Void setFileAttributes(FileAttributes attributes) throws CacheException
                                    {
                                        return r.setFileAttributes(attributes);
                                    }

                                    @Override
                                    public FileAttributes getFileAttributes() throws CacheException
                                    {
                                        return r.getFileAttributes();
                                    }

                                    @Override
                                    public EntryState getState()
                                    {
                                        return r.getState();
                                    }

                                    @Override
                                    public int getLinkCount()
                                    {
                                        return r.getLinkCount();
                                    }
                                }));
            } catch (IllegalArgumentException | IllegalStateException e) {
                throw e;
            } catch (RuntimeException | CacheException e) {
                FaultEvent event = new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e);
                _faultListener.faultOccurred(event);
                throw e;
            }
        }
    }

    public MetaDataRecord get(PnfsId id)
            throws CacheException, InterruptedException
    {
        try {
            return _entries.computeIfAbsent(id, Monitor::new).get();
        } catch (RuntimeException | DiskErrorCacheException e) {
            _faultListener.faultOccurred(
                    new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
            throw e;
        }
    }

    @Override
    public MetaDataRecord create(PnfsId id) throws CacheException
    {
        try {
            return _entries.computeIfAbsent(id, Monitor::new).create();
        } catch (RuntimeException | DiskErrorCacheException e) {
            _faultListener.faultOccurred(
                    new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
            throw e;
        }
    }

    @Override
    public MetaDataRecord copy(MetaDataRecord entry)
            throws CacheException
    {
        try {
            return _entries.computeIfAbsent(entry.getPnfsId(), Monitor::new).create(entry);
        } catch (RuntimeException | DiskErrorCacheException e) {
            _faultListener.faultOccurred(
                    new FaultEvent("repository", FaultAction.DEAD, "Internal repository error", e));
            throw e;
        }
    }

    @Override
    public void remove(PnfsId id) throws CacheException
    {
        throw new UnsupportedOperationException("Call setState(REMOVED) instead.");
    }

    /**
     * The operation may be slow as the {@code index} method of {@code inner} is called.
     */
    @Override
    public void init() throws CacheException
    {
        for (PnfsId id: _inner.index(IndexOption.ALLOW_REPAIR)) {
            _entries.putIfAbsent(id, new Monitor(id));
        }
    }

    @Override
    public Set<PnfsId> index(IndexOption... options)
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
