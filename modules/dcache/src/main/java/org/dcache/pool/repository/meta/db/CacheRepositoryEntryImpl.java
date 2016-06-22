package org.dcache.pool.repository.meta.db;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.je.Transaction;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.filter;
import static org.dcache.pool.repository.EntryState.*;

/**
 * Berkeley DB aware implementation of CacheRepositoryEntry interface.
 */
public class CacheRepositoryEntryImpl implements MetaDataRecord
{
    private static final Logger _log =
        LoggerFactory.getLogger(CacheRepositoryEntryImpl.class);

    // Reusable list for the common case
    private static final ImmutableList<StickyRecord> SYSTEM_STICKY =
            ImmutableList.of(new StickyRecord("system", -1));

    private final PnfsId _pnfsId;
    private final AbstractBerkeleyDBMetaDataRepository _repository;

    /**
     * Sticky records held by the file.
     */
    private ImmutableList<StickyRecord> _sticky;

    private EntryState _state;

    private long _creationTime = System.currentTimeMillis();

    private long _lastAccess;

    private int  _linkCount;

    private long _size;

    public CacheRepositoryEntryImpl(AbstractBerkeleyDBMetaDataRepository repository, PnfsId pnfsId)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = NEW;
        _sticky = ImmutableList.of();
        _lastAccess = _creationTime;
    }

    public CacheRepositoryEntryImpl(AbstractBerkeleyDBMetaDataRepository repository, PnfsId pnfsId, EntryState state,
                                    Collection<StickyRecord> sticky, BasicFileAttributes attributes)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = state;
        setStickyRecords(sticky);
        _lastAccess = attributes.lastModifiedTime().toMillis();
        _size = attributes.size();
    }

    private void setStickyRecords(Iterable<StickyRecord> records)
    {
        _sticky = elementsEqual(records, SYSTEM_STICKY) ? SYSTEM_STICKY : ImmutableList.copyOf(records);
    }

    @Override
    public synchronized int decrementLinkCount()
    {
        if (_linkCount <= 0) {
            throw new IllegalStateException("Link count is already zero");
        }
        _linkCount--;
        return _linkCount;
    }

    @Override
    public synchronized int incrementLinkCount()
    {
        EntryState state = getState();
        if (state == REMOVED || state == DESTROYED) {
            throw new IllegalStateException("Entry is marked as removed");
        }
        _linkCount++;
        return _linkCount;
    }

    @Override
    public synchronized int getLinkCount()
    {
        return _linkCount;
    }

    public synchronized void setCreationTime(long time)
    {
        _creationTime = time;
    }

    @Override
    public synchronized long getCreationTime()
    {
        return _creationTime;
    }

    @Override
    public synchronized long getLastAccessTime()
    {
        return _lastAccess;
    }

    @Override
    public synchronized void setLastAccessTime(long time) throws CacheException
    {
        try {
            _repository.setLastModifiedTime(_pnfsId, time);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set modification time for " + _pnfsId + ": " + e.toString(), e);
        }
        _lastAccess = time;
    }

    @Override
    public synchronized long getReplicaSize()
    {
        try {
            return _state.isMutable() ? _repository.getFileSize(_pnfsId) : _size;
        } catch (IOException e) {
            _log.error("Failed to read file size: " + e);
            return 0;
        }
    }

    private synchronized StorageInfo getStorageInfo()
    {
        return _repository.getStorageInfoMap().get(_pnfsId.toString());
    }

    @Override
    public synchronized FileAttributes getFileAttributes() throws CacheException
    {
        try {
            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(_pnfsId);
            StorageInfo storageInfo = getStorageInfo();
            if (storageInfo != null) {
                StorageInfos.injectInto(storageInfo, attributes);
            }
            return attributes;
        } catch (EnvironmentFailureException e) {
            if (!_repository.isValid()) {
                throw new DiskErrorCacheException("Meta data lookup failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data lookup failed: " + e.getMessage(), e);
        }
    }

    private Void setFileAttributes(FileAttributes attributes) throws CacheException
    {
        try {
            String id = _pnfsId.toString();
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                _repository.getStorageInfoMap().put(id, StorageInfos.extractFrom(attributes));
            } else {
                _repository.getStorageInfoMap().remove(id);
            }
        } catch (EnvironmentFailureException e) {
            if (!_repository.isValid()) {
                throw new DiskErrorCacheException("Meta data update failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        }
        return null;
    }

    @Override
    public synchronized PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    @Override
    public synchronized EntryState getState()
    {
        return _state;
    }

    @Override
    public synchronized boolean isSticky()
    {
        return !_sticky.isEmpty();
    }

    @Override
    public synchronized URI getReplicaUri()
    {
        return _repository.getUri(_pnfsId);
    }

    @Override
    public RepositoryChannel openChannel(IoMode mode) throws IOException
    {
        return _repository.openChannel(_pnfsId, mode);
    }

    @Override
    public Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        return update(r -> {
            long now = System.currentTimeMillis();
            List<StickyRecord> removed = Lists.newArrayList(filter(_sticky, s -> !s.isValidAt(now)));
            if (!removed.isEmpty()) {
                setStickyRecords(ImmutableList.copyOf(filter(_sticky, s -> s.isValidAt(now))));
            }
            return removed;
        });
    }

    public synchronized Collection<StickyRecord> stickyRecords()
    {
        return _sticky;
    }

    @Override
    public synchronized <T> T update(Update<T> update) throws CacheException
    {
        T result;
        EntryState state = _state;
        ImmutableList<StickyRecord> sticky = _sticky;
        Transaction transaction = _repository.beginTransaction();
        try {
            UpdatableRecordImpl record = new UpdatableRecordImpl();
            result = update.apply(record);
            record.save();
            transaction.commit();
        } catch (Throwable t) {
            _state = state;
            _sticky = sticky;
            try {
                if (transaction.getState() != Transaction.State.COMMITTED &&
                    transaction.getState() != Transaction.State.POSSIBLY_COMMITTED) {
                    transaction.abort();
                }
            } catch (Throwable e) {
                t.addSuppressed(e);
            }
            if (t instanceof EnvironmentFailureException && !_repository.isValid()) {
                throw new DiskErrorCacheException(
                        "Meta data update failed and a pool restart is required: " + t.getMessage(), t);
            }
            if (transaction.getState() == Transaction.State.POSSIBLY_COMMITTED) {
                throw new DiskErrorCacheException(
                        "Meta data commit and a pool restart is required: " + t.getMessage(), t);
            }
            Throwables.propagateIfPossible(t, CacheException.class);
            throw new CacheException("Meta data update failed: " + t.getMessage(), t);
        }
        return result;
    }

    private synchronized void storeState() throws CacheException
    {
        try {
            _repository.getStateMap().put(_pnfsId.toString(), new CacheRepositoryEntryState(_state, _sticky));
        } catch (EnvironmentFailureException e) {
            if (!_repository.isValid()) {
                throw new DiskErrorCacheException("Meta data update failed and a pool restart is required: " + e.getMessage(), e);
            }
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        } catch (OperationFailureException e) {
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        }
    }

    static CacheRepositoryEntryImpl load(BerkeleyDBMetaDataRepository repository, PnfsId pnfsId,
                                         BasicFileAttributes attributes) throws IOException
    {
        try {
            String id = pnfsId.toString();
            CacheRepositoryEntryState state = repository.getStateMap().get(id);
            if (state != null) {
                return new CacheRepositoryEntryImpl(repository, pnfsId, state.getState(), state.stickyRecords(), attributes);
            }
        } catch (ClassCastException e) {
            _log.warn(e.toString());
        } catch (RuntimeExceptionWrapper e) {
            /* BerkeleyDB wraps internal exceptions. We ignore class
             * cast and class not found exceptions, since they are a
             * result of us changing the layout of serialized classes.
             */
            if (!(e.getCause() instanceof ClassNotFoundException) &&
                !(e.getCause() instanceof ClassCastException)) {
                throw e;
            }
            _log.warn(e.toString());
        }
        return new CacheRepositoryEntryImpl(repository, pnfsId, BROKEN, ImmutableList.of(), attributes);
    }

    private class UpdatableRecordImpl implements UpdatableRecord
    {
        private boolean _stateModified;

        @Override
        public boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException
        {
            if (_state == REMOVED) {
                throw new CacheException("Entry in removed state");
            }
            Predicate<StickyRecord> subsumes =
                    r -> r.owner().equals(owner) && (r.expire() == expire || !overwrite && r.isValidAt(expire));
            if (_sticky.stream().anyMatch(subsumes)) {
                return false;
            }
            ImmutableList.Builder<StickyRecord> builder = ImmutableList.builder();
            _sticky.stream().filter(r -> !r.owner().equals(owner)).forEach(builder::add);
            builder.add(new StickyRecord(owner, expire));
            setStickyRecords(builder.build());
            _stateModified = true;
            return true;
        }

        @Override
        public Void setState(EntryState state) throws CacheException
        {
            if (_state != state) {
                if (_state.isMutable() && !state.isMutable()) {
                    try {
                        _size = _repository.getFileSize(_pnfsId);
                    } catch (IOException e) {
                        throw new DiskErrorCacheException("Failed to query file size: " + e, e);
                    }
                }
                _state = state;
                _stateModified = true;
            }
            return null;
        }

        @Override
        public Void setFileAttributes(FileAttributes attributes) throws CacheException
        {
            return CacheRepositoryEntryImpl.this.setFileAttributes(attributes);
        }

        @Override
        public FileAttributes getFileAttributes() throws CacheException
        {
            return CacheRepositoryEntryImpl.this.getFileAttributes();
        }

        @Override
        public EntryState getState()
        {
            return CacheRepositoryEntryImpl.this.getState();
        }

        @Override
        public int getLinkCount()
        {
            return CacheRepositoryEntryImpl.this.getLinkCount();
        }

        public void save() throws CacheException
        {
            if (_stateModified) {
                storeState();
            }
        }
    }
}
