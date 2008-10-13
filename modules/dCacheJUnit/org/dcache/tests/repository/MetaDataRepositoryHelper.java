package org.dcache.tests.repository;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dcache.pool.repository.DataFileRepository;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.MetaDataRepository;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.meta.db.CacheRepositoryEntryState;
import org.dcache.pool.repository.v3.RepositoryException;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.CacheRepositoryStatistics;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

public class MetaDataRepositoryHelper implements MetaDataRepository {



    public static class CacheRepositoryEntryImpl implements CacheRepositoryEntry {
        private final CacheRepositoryEntryState _state;
        private final PnfsId _pnfsId;

        private long _creationTime = System.currentTimeMillis();

        private long _lastAccess = _creationTime;

        private int  _linkCount = 0;

        private boolean _isLocked = false;

        private long _lockUntil = 0;
        private StorageInfo _storageInfo;
        private final DataFileRepository _repository;

        public CacheRepositoryEntryImpl(DataFileRepository repository, PnfsId pnfsId)
        {
            _repository = repository;
            _pnfsId = pnfsId;
            _state = new CacheRepositoryEntryState();
            _lastAccess = getDataFile().lastModified();
        }

        public CacheRepositoryEntryImpl(DataFileRepository repository,
                                        CacheRepositoryEntry entry)
            throws CacheException
        {
            _repository   = repository;
            _pnfsId       = entry.getPnfsId();
            _lastAccess   = entry.getLastAccessTime();
            _linkCount    = entry.getLinkCount();
            _creationTime = entry.getCreationTime();
            _state        = new CacheRepositoryEntryState(entry);
            setStorageInfo(entry.getStorageInfo());
        }

        public CacheRepositoryStatistics getCacheRepositoryStatistics()
            throws CacheException
        {
            // TODO Auto-generated method stub
            return null;
        }

        public synchronized void decrementLinkCount() throws CacheException
        {
            assert _linkCount > 0;
            _linkCount--;
            if (_linkCount == 0 && isRemoved()) {
                //_repository.remove(_pnfsId);
            }
        }


        public synchronized void incrementLinkCount()
        {
            assert !isRemoved();
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
            return _storageInfo;
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
            return _repository.get(_pnfsId);
        }

        public void setBad(boolean bad)
        {
            try {
                if (bad) {
                    _state.setError();
                } else {
                    _state.cleanBad();
                }

            } catch (IllegalStateException e) {
                // TODO: throw Cache exception
            }
        }

        public void setCached() throws CacheException
        {
            try {
                _state.setCached();

            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        public void setPrecious(boolean force) throws CacheException
        {
            try {
                _state.setPrecious(force);

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
            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        public void setReceivingFromStore() throws CacheException
        {
            try {
                _state.setFromStore();
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
            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        public void setSticky(boolean sticky) throws CacheException
        {
            try {
                if (sticky) {
                    _state.setSticky("system", -1, true);
                } else {
                    _state.setSticky("system", 0, true);
                }

            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        public void setSticky(String owner, long lifetime, boolean overwrite)
            throws CacheException
        {
            try {
                _state.setSticky(owner, lifetime, overwrite);
            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        public synchronized void setStorageInfo(StorageInfo info)
        {
            _storageInfo = info;
        }

        public synchronized void setRemoved() throws CacheException
        {
            try {
                _state.setRemoved();

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

        @Override
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

    }


    private final Map<PnfsId, CacheRepositoryEntry> _entryList = new HashMap<PnfsId, CacheRepositoryEntry>();
    private final DataFileRepository _repository;
    public MetaDataRepositoryHelper(DataFileRepository repository) {
        _repository = repository;
    }

    public CacheRepositoryEntry create(PnfsId id) throws DuplicateEntryException, RepositoryException {
        CacheRepositoryEntry   entry = new CacheRepositoryEntryImpl(_repository, id);

        _entryList.put(id, entry);
        return entry;
    }

    public CacheRepositoryEntry create(CacheRepositoryEntry entry) throws DuplicateEntryException, CacheException {

        _entryList.put(entry.getPnfsId(), entry);

        return entry;
    }

    public CacheRepositoryEntry get(PnfsId id) {
        return _entryList.get(id);
    }

    public boolean isOk() {
        return true;
    }

    public void remove(PnfsId id) {
        _entryList.remove(id);
    }

    public void close() {
    }

}
