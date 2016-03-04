package org.dcache.pool.repository.meta.db;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.*;

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
    private final BerkeleyDBMetaDataRepository _repository;

    /**
     * Sticky records held by the file.
     */
    private ImmutableList<StickyRecord> _sticky;

    private EntryState _state;

    private long _creationTime = System.currentTimeMillis();

    private long _lastAccess;

    private int  _linkCount;

    private long _size;

    public CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                    PnfsId pnfsId)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _sticky = ImmutableList.of();
        _state = EntryState.NEW;
        File file = getDataFile();
        _lastAccess = file.lastModified();
        _size = file.length();
        if (_lastAccess == 0) {
            _lastAccess = _creationTime;
        }
    }

    public CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                    MetaDataRecord entry) throws CacheException
    {
        _repository   = repository;
        _pnfsId       = entry.getPnfsId();
        _lastAccess   = entry.getLastAccessTime();
        _linkCount    = entry.getLinkCount();
        _creationTime = entry.getCreationTime();
        _size         = entry.getSize();
        _state        = entry.getState();
        setStickyRecords(entry.stickyRecords());

        storeState();
        setFileAttributes(entry.getFileAttributes());
        if (_lastAccess == 0) {
            _lastAccess = _creationTime;
        }
    }

    private CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                     PnfsId pnfsId,
                                     CacheRepositoryEntryState state)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = state.getState();
        setStickyRecords(state.stickyRecords());
        File file = getDataFile();
        _lastAccess = file.lastModified();
        _size = file.length();
        if (_lastAccess == 0) {
            _lastAccess = _creationTime;
        }
    }

    private void setStickyRecords(Iterable<StickyRecord> records)
    {
        _sticky = elementsEqual(records, SYSTEM_STICKY) ? SYSTEM_STICKY : ImmutableList.copyOf(records);
    }

    @Override
    public synchronized void decrementLinkCount()
    {
        if (_linkCount <= 0) {
            throw new IllegalStateException("Link count is already zero");
        }
        _linkCount--;
    }

    @Override
    public synchronized void incrementLinkCount()
    {
        EntryState state = getState();
        if (state == EntryState.REMOVED || state == EntryState.DESTROYED) {
            throw new IllegalStateException("Entry is marked as removed");
        }
        _linkCount++;
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
    public void setLastAccessTime(long time) throws CacheException
    {
        File file = getDataFile();
        if (!file.setLastModified(time)) {
            throw new DiskErrorCacheException("Failed to set modification time: " + file);
        }
        _lastAccess = time;
    }

    @Override
    public synchronized void setSize(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Negative entry size is not allowed");
        }
        _size = size;
    }

    @Override
    public synchronized long getSize()
    {
        return _size;
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

    @Override
    public void setFileAttributes(FileAttributes attributes) throws CacheException
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
    public synchronized void setState(EntryState state) throws CacheException
    {
        if (_state != state) {
            _state = state;
            storeState();
        }
    }

    @Override
    public synchronized boolean isSticky()
    {
        return !_sticky.isEmpty();
    }

    @Override
    public synchronized File getDataFile()
    {
        return _repository.getDataFile(_pnfsId);
    }

    @Override
    public synchronized Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        long now = System.currentTimeMillis();
        List<StickyRecord> removed = Lists.newArrayList(filter(_sticky, r -> !r.isValidAt(now)));
        if (!removed.isEmpty()) {
            setStickyRecords(ImmutableList.copyOf(filter(_sticky, r -> r.isValidAt(now))));
            storeState();
        }
        return removed;
    }

    @Override
    public synchronized boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException
    {
        if (_state == EntryState.REMOVED) {
            throw new CacheException("Entry in removed state");
        }
        if (any(_sticky, r -> r.owner().equals(owner) && (r.expire() == expire || !overwrite && r.isValidAt(expire)))) {
            return false;
        }
        ImmutableList.Builder<StickyRecord> builder = ImmutableList.builder();
        builder.addAll(filter(_sticky, r -> !r.owner().equals(owner)));
        builder.add(new StickyRecord(owner, expire));
        setStickyRecords(builder.build());
        storeState();
        return true;
    }

    @Override
    public synchronized void touch() throws CacheException
    {
        File file = getDataFile();
        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            throw new DiskErrorCacheException("IO error creating: " + file);
        }

        setLastAccessTime(System.currentTimeMillis());
    }

    @Override
    public synchronized Collection<StickyRecord> stickyRecords()
    {
        return _sticky;
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

    static CacheRepositoryEntryImpl load(BerkeleyDBMetaDataRepository repository, PnfsId pnfsId)
    {
        try {
            String id = pnfsId.toString();
            CacheRepositoryEntryState state = repository.getStateMap().get(id);
            if (state != null) {
                return new CacheRepositoryEntryImpl(repository, pnfsId, state);
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

        return new CacheRepositoryEntryImpl(repository, pnfsId);
    }
}
