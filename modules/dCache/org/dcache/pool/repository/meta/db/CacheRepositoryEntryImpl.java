package org.dcache.pool.repository.meta.db;

import java.io.IOException;
import java.io.InvalidClassException;
import java.io.File;
import java.util.List;
import java.lang.ref.SoftReference;

import org.apache.log4j.Logger;

import com.sleepycat.util.RuntimeExceptionWrapper;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.EventType;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.CacheRepositoryStatistics;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.util.event.CacheRepositoryEvent;
import diskCacheV111.vehicles.StorageInfo;

/**
 * Berkeley DB aware implementation of CacheRepositoryEntry interface.
 */
public class CacheRepositoryEntryImpl implements CacheRepositoryEntry
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    private final CacheRepositoryEntryState _state;
    private final PnfsId _pnfsId;
    private final BerkeleyDBMetaDataRepository _repository;

    private long _creationTime = System.currentTimeMillis();

    private long _lastAccess = _creationTime;

    private int  _linkCount = 0;

    private boolean _isLocked = false;

    private long _lockUntil = 0;

    public CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                    PnfsId pnfsId)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = new CacheRepositoryEntryState();
        _lastAccess = getDataFile().lastModified();
    }

    public CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                    CacheRepositoryEntry entry)
        throws CacheException
    {
        _repository   = repository;
        _pnfsId       = entry.getPnfsId();
        _lastAccess   = entry.getLastAccessTime();
        _linkCount    = entry.getLinkCount();
        _creationTime = entry.getCreationTime();
        _state        = new CacheRepositoryEntryState(entry);
        storeStateIfDirty();
        setStorageInfo(entry.getStorageInfo());
    }

    private CacheRepositoryEntryImpl(BerkeleyDBMetaDataRepository repository,
                                     PnfsId pnfsId,
                                     CacheRepositoryEntryState state)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = state;
        _lastAccess = getDataFile().lastModified();
    }

    private void destroy()
    {
        generateEvent(EventType.DESTROY);
        _repository.remove(_pnfsId);
    }

    public CacheRepositoryStatistics getCacheRepositoryStatistics()
        throws CacheException
    {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Atomically decrements link count by one. Returns true if and
     * only if the link count has reached zero and the entry is marked
     * REMOVED.
     *
     * @see internalRemove
     */
    private synchronized boolean internalDecrementLinkCount()
    {
        if (_linkCount <= 0)
            throw new IllegalStateException("Link count is already zero");
        _linkCount--;
        return (_linkCount == 0 && isRemoved());
    }

    /**
     * Atomically marks the entry as REMOVED. Returns true if and only
     * if the link count has reached zero. The method does not
     * generate a removal event.
     *
     * @see internalDecrementLinkCount
     */
    private synchronized boolean internalRemove()
    {
        _state.setRemoved();
        storeStateIfDirty();
        return (getLinkCount() == 0);
    }

    public void decrementLinkCount()
    {
        if (internalDecrementLinkCount()) {
            destroy();
        }
    }

    public synchronized void incrementLinkCount()
    {
        if (isRemoved())
            throw new IllegalStateException("Entry is marked as removed");
        _linkCount++;
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

    public synchronized int getLinkCount()
    {
        return _linkCount;
    }

    public synchronized long getSize()
    {
        if (_state.isReady()) {
            StorageInfo info = getStorageInfo();
            if (info != null) {
                return info.getFileSize();
            }
        }

        return getDataFile().length();
    }

    public synchronized StorageInfo getStorageInfo()
    {
        String id = _pnfsId.toString();
        return (StorageInfo)_repository.getStorageInfoMap().get(id);
    }

    private synchronized long getLastAccess()
    {
        return _lastAccess;
    }

    private synchronized void setLastAccess(long time)
    {
        _lastAccess = time;
    }

    public synchronized void lock(boolean locked)
    {
        _isLocked = locked;
    }

    public synchronized void lock(long millisSeconds)
    {
        long now = System.currentTimeMillis();

        if (now + millisSeconds > _lockUntil) {
            _lockUntil = now + millisSeconds;
        }
    }

    public synchronized boolean isLocked()
    {
        return _isLocked || _lockUntil > System.currentTimeMillis();
    }

    public PnfsId getPnfsId()
    {
        return _pnfsId;
    }

    public String getState()
    {
        return _state.toString();
    }

    public boolean isBad()
    {
        return _state.isError();
    }

    public boolean isCached()
    {
        return _state.isCached();
    }

    public boolean isRemoved()
    {
        return _state.isRemoved();
    }

    public boolean isDestroyed()
    {
        return _linkCount == 0 && isRemoved();
    }

    public boolean isPrecious()
    {
        return _state.isPrecious();
    }

    public boolean isReceivingFromClient()
    {
        return _state.isReceivingFromClient();
    }

    public boolean isReceivingFromStore()
    {
        return _state.isReceivingFromStore();
    }

    public boolean isSendingToStore()
    {
        return _state.isSendingToStore();
    }

    public boolean isSticky()
    {
        return _state.isSticky();
    }

    public File getDataFile()
    {
        return _repository.getDataFile(_pnfsId);
    }

    /**
     * Generates an event and sends it to the repository for
     * processing.
     *
     * FIXME: We should actually generate a copy of the entry provided
     * in the event.
     */
    private void generateEvent(EventType type)
    {
        CacheRepositoryEvent event =
            new CacheRepositoryEvent(_repository, this);
        _repository.processEvent(type, event);
    }

    public void setBad(boolean bad)
    {
        try {
            if (bad) {
                _state.setError();
            } else {
                _state.cleanBad();
            }
            storeStateIfDirty();
        } catch (IllegalStateException e) {
            // TODO: throw Cache exception
        }
    }

    public void setCached() throws CacheException
    {
        try {
            if (_state.isReceivingFromClient() || _state.isReceivingFromStore()) {
                generateEvent(EventType.AVAILABLE);
            }

            _state.setCached();

            generateEvent(EventType.CACHED);

            storeStateIfDirty();
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setPrecious(boolean force) throws CacheException
    {
        try {
            if (_state.isReceivingFromClient()) {
                generateEvent(EventType.AVAILABLE);
            }

            _state.setPrecious(force);
            storeStateIfDirty();

            generateEvent(EventType.PRECIOUS);
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setPrecious() throws CacheException
    {
        setPrecious(false);
    }

    public void setReceivingFromClient() throws CacheException
    {
        try {
            _state.setFromClient();
            storeStateIfDirty();
            generateEvent(EventType.CREATE);
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setReceivingFromStore() throws CacheException
    {
        try {
            _state.setFromStore();
            storeStateIfDirty();
            generateEvent(EventType.CREATE);
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setSendingToStore(boolean sending) throws CacheException
    {
        try {
            if (sending) {
                _state.setToStore();
            }else{
                _state.cleanToStore();
            }
            storeStateIfDirty();
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void removeExpiredStickyFlags()
    {
        if (_state.removeExpiredStickyFlags()) {
            storeStateIfDirty();
            if (!isSticky()) {
                generateEvent(EventType.STICKY);
            }
        }
    }

    public void setSticky(boolean sticky) throws CacheException
    {
        try {
            _state.setSticky("system", sticky ? -1 : 0, true);
            storeStateIfDirty();

            generateEvent(EventType.STICKY);
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void setSticky(String owner, long expire, boolean overwrite) throws CacheException
    {
        try {
            _state.setSticky(owner, expire, overwrite);
            storeStateIfDirty();

            generateEvent(EventType.STICKY);

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

    public void setRemoved() throws CacheException
    {
        try {
            boolean isDead = internalRemove();
            generateEvent(EventType.REMOVE);
            if (isDead) {
                destroy();
            }
        } catch (IllegalStateException e) {
            throw new CacheException(e.getMessage());
        }
    }

    public void touch() throws CacheException
    {
        File file = getDataFile();

        try {
            if (!file.exists()) {
                file.createNewFile();
            }
        } catch(IOException e) {
            throw new CacheException("IO error creating: " + file);
        }

        long now = System.currentTimeMillis();
        file.setLastModified(now);
        setLastAccess(now);
    }

    public List<StickyRecord> stickyRecords()
    {
        return _state.stickyRecords();
    }

    public synchronized String toString()
    {
        StorageInfo info = getStorageInfo();
        return _pnfsId.toString()+
            " <"+_state.toString()+(_isLocked ? "L":"-")+
            "(" + _lockUntil +")"+
            "["+getLinkCount()+"]> "+
            getSize()+
            " si={"+(info==null?"<unknown>":info.getStorageClass())+"}" ;
    }

    private void storeStateIfDirty()
    {
        synchronized (_state) {
            if (_state.dirty()) {
                _repository.getStateMap().put(_pnfsId.toString(), _state);
            }
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
            _log.warn(e);
        } catch (RuntimeExceptionWrapper e) {
            /* BerkeleyDB wraps internal exceptions. We ignore class
             * cast and class not found exceptions, since they are a
             * result of us changing the layout of serialized classes.
             */
            if (!(e.getCause() instanceof ClassNotFoundException) &&
                !(e.getCause() instanceof ClassCastException)) {
                throw e;
            }

            _log.warn(e);
        }

        return null;
    }
}
