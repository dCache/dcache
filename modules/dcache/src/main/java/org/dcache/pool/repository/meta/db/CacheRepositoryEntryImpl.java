package org.dcache.pool.repository.meta.db;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.File;
import java.util.List;
import java.lang.ref.SoftReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sleepycat.util.RuntimeExceptionWrapper;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.EntryState;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

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

    private int  _linkCount = 0;

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
                                    MetaDataRecord entry)
        throws CacheException
    {
        _repository   = repository;
        _pnfsId       = entry.getPnfsId();
        _lastAccess   = entry.getLastAccessTime();
        _linkCount    = entry.getLinkCount();
        _creationTime = entry.getCreationTime();
        _size         = entry.getSize();
        _state        = new CacheRepositoryEntryState(entry);
        storeStateIfDirty();
        setStorageInfo(entry.getStorageInfo());
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

    public synchronized void decrementLinkCount()
    {
        if (_linkCount <= 0)
            throw new IllegalStateException("Link count is already zero");
        _linkCount--;
    }

    public synchronized void incrementLinkCount()
    {
        EntryState state = getState();
        if (state == EntryState.REMOVED || state == EntryState.DESTROYED)
            throw new IllegalStateException("Entry is marked as removed");
        _linkCount++;
    }

    public synchronized int getLinkCount()
    {
        return _linkCount;
    }

    public synchronized void setCreationTime(long time)
    {
        _creationTime = time;
    }

    public synchronized long getCreationTime()
    {
        return _creationTime;
    }

    public synchronized long getLastAccessTime()
    {
        return _lastAccess;
    }

    public synchronized void setSize(long size)
    {
        if (size < 0) {
            throw new IllegalArgumentException("Negative entry size is not allowed");
        }
        _size = size;
    }

    public synchronized long getSize()
    {
        return _size;
    }

    public synchronized StorageInfo getStorageInfo()
    {
        String id = _pnfsId.toString();
        return (StorageInfo)_repository.getStorageInfoMap().get(id);
    }

    public synchronized PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public synchronized EntryState getState()
    {
        return _state.getState();
    }

    public synchronized void setState(EntryState state)
    {
        _state.setState(state);
        storeStateIfDirty();
    }

    public synchronized boolean isSticky()
    {
        return _state.isSticky();
    }

    public synchronized File getDataFile()
    {
        return _repository.getDataFile(_pnfsId);
    }

    public synchronized List<StickyRecord> removeExpiredStickyFlags()
    {
        List<StickyRecord> removed = _state.removeExpiredStickyFlags();
        if (!removed.isEmpty()) {
            storeStateIfDirty();
        }
        return removed;
    }

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

    public synchronized void setStorageInfo(StorageInfo info)
    {
        String id = _pnfsId.toString();
        if (info != null) {
            _repository.getStorageInfoMap().put(id, info);
        } else {
            _repository.getStorageInfoMap().remove(id);
        }
    }

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

    private synchronized void storeStateIfDirty()
    {
        if (_state.dirty()) {
            _repository.getStateMap().put(_pnfsId.toString(), _state);
        }
    }

    static CacheRepositoryEntryImpl load(BerkeleyDBMetaDataRepository repository,
                                         PnfsId pnfsId)
    {
        try {
            String id = pnfsId.toString();
            CacheRepositoryEntryState state =
                (CacheRepositoryEntryState)repository.getStateMap().get(id);

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
