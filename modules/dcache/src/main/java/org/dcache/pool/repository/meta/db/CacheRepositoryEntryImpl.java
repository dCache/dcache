package org.dcache.pool.repository.meta.db;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
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

/**
 * Berkeley DB aware implementation of CacheRepositoryEntry interface.
 */
public class CacheRepositoryEntryImpl implements MetaDataRecord
{
    private static Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private final CacheRepositoryEntryState _state;
    private final PnfsId _pnfsId;
    private final BerkeleyDBMetaDataRepository _repository;

    private long _creationTime = System.currentTimeMillis();

    private long _lastAccess;

    private int  _linkCount;

    private long _size;

    public CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                    PnfsId pnfsId)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = new CacheRepositoryEntryState();
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
        _state        = new CacheRepositoryEntryState(entry);
        storeStateIfDirty();
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
        _state = state;
        File file = getDataFile();
        _lastAccess = file.lastModified();
        _size = file.length();
        if (_lastAccess == 0) {
            _lastAccess = _creationTime;
        }
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
    public synchronized FileAttributes getFileAttributes() throws DiskErrorCacheException
    {
        try {
            FileAttributes attributes = new FileAttributes();
            attributes.setPnfsId(_pnfsId);
            StorageInfo storageInfo = getStorageInfo();
            if (storageInfo != null) {
                StorageInfos.injectInto(storageInfo, attributes);
            }
            return attributes;
        } catch (DatabaseException e) {
            throw new DiskErrorCacheException("Meta data lookup failed: " + e.getMessage(), e);
        }
    }

    @Override
    public void setFileAttributes(FileAttributes attributes) throws DiskErrorCacheException
    {
        try {
            String id = _pnfsId.toString();
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {
                _repository.getStorageInfoMap().put(id, StorageInfos.extractFrom(attributes));
            } else {
                _repository.getStorageInfoMap().remove(id);
            }
        } catch (DatabaseException e) {
            throw new DiskErrorCacheException("Meta data update failed: " + e.getMessage(), e);
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
        return _state.getState();
    }

    @Override
    public synchronized void setState(EntryState state) throws DiskErrorCacheException
    {
        _state.setState(state);
        storeStateIfDirty();
    }

    @Override
    public synchronized boolean isSticky()
    {
        return _state.isSticky();
    }

    @Override
    public synchronized File getDataFile()
    {
        return _repository.getDataFile(_pnfsId);
    }

    @Override
    public synchronized List<StickyRecord> removeExpiredStickyFlags() throws DiskErrorCacheException
    {
        List<StickyRecord> removed = _state.removeExpiredStickyFlags();
        if (!removed.isEmpty()) {
            storeStateIfDirty();
        }
        return removed;
    }

    @Override
    public synchronized boolean setSticky(String owner, long expire, boolean overwrite) throws CacheException
    {
        try {
            if (_state.setSticky(owner, expire, overwrite)) {
                storeStateIfDirty();
                return true;
            }
            return false;
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
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

        long now = System.currentTimeMillis();
        if (!file.setLastModified(now)) {
            throw new DiskErrorCacheException("Failed to set modification time: " + file);
        }
        _lastAccess = now;
    }

    @Override
    public synchronized List<StickyRecord> stickyRecords()
    {
        return _state.stickyRecords();
    }

    @Override
    public synchronized String toString()
    {
        StorageInfo info = getStorageInfo();
        return _pnfsId.toString()+
            " <"+_state.toString()+"-"+
            "(0)"+
            "["+getLinkCount()+"]> "+
            getSize()+
            " si={"+(info==null?"<unknown>":info.getStorageClass())+"}" ;
    }

    private synchronized void storeStateIfDirty() throws DiskErrorCacheException
    {
        if (_state.dirty()) {
            try {
                _repository.getStateMap().put(_pnfsId.toString(), _state);
            } catch (DatabaseException e) {
                throw new DiskErrorCacheException("Meta data updated failed: " + e.getMessage(), e);
            }
        }
    }

    static CacheRepositoryEntryImpl load(BerkeleyDBMetaDataRepository repository,
                                         PnfsId pnfsId)
    {
        try {
            String id = pnfsId.toString();
            CacheRepositoryEntryState state =
                repository.getStateMap().get(id);

            if (state != null) {
                return new CacheRepositoryEntryImpl(repository, pnfsId, state);
            }

            _log.debug("No entry found for " + id);
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

        return null;
    }
}
