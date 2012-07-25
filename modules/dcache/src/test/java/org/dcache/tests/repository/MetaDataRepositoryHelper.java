package org.dcache.tests.repository;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;

import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.meta.db.CacheRepositoryEntryState;
import org.dcache.pool.repository.v3.RepositoryException;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;

public class MetaDataRepositoryHelper implements MetaDataStore {



    public static class CacheRepositoryEntryImpl implements MetaDataRecord {
        private final CacheRepositoryEntryState _state;
        private final PnfsId _pnfsId;

        private long _creationTime = System.currentTimeMillis();

        private long _lastAccess = _creationTime;

        private int  _linkCount;

        private long _size;

        private StorageInfo _storageInfo;
        private final FileStore _repository;

        public CacheRepositoryEntryImpl(FileStore repository, PnfsId pnfsId)
        {
            _repository = repository;
            _pnfsId = pnfsId;
            _state = new CacheRepositoryEntryState();
            _lastAccess = getDataFile().lastModified();
            _size = getDataFile().length();
        }

        public CacheRepositoryEntryImpl(FileStore repository,
                                        MetaDataRecord entry)
        {
            _repository   = repository;
            _pnfsId       = entry.getPnfsId();
            _lastAccess   = entry.getLastAccessTime();
            _linkCount    = entry.getLinkCount();
            _creationTime = entry.getCreationTime();
            _state        = new CacheRepositoryEntryState(entry);
            setStorageInfo(entry.getStorageInfo());
        }

        @Override
        public synchronized void decrementLinkCount()
        {
            assert _linkCount > 0;
            _linkCount--;
        }


        @Override
        public synchronized void incrementLinkCount()
        {
            _linkCount++;
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
        public synchronized int getLinkCount()
        {
            return _linkCount;
        }

        @Override
        public synchronized void setSize(long size)
        {
            size = _size;
        }

        @Override
        public synchronized long getSize()
        {
            return _size;
        }

        @Override
        public synchronized StorageInfo getStorageInfo()
        {
            return _storageInfo;
        }

        private synchronized void setLastAccess(long time)
        {
            _lastAccess = time;
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
        public synchronized void setState(EntryState state)
        {
            _state.setState(state);
        }

        @Override
        public synchronized boolean isSticky()
        {
            return _state.isSticky();
        }

        @Override
        public synchronized File getDataFile()
        {
            return _repository.get(_pnfsId);
        }

        @Override
        public synchronized boolean setSticky(String owner, long lifetime, boolean overwrite)
            throws CacheException
        {
            try {
                return _state.setSticky(owner, lifetime, overwrite);
            } catch (IllegalStateException e) {
                throw new CacheException(e.getMessage());
            }
        }

        @Override
        public synchronized void setStorageInfo(StorageInfo info)
        {
            _storageInfo = info;
        }

        @Override
        public synchronized void touch() throws CacheException
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

        @Override
        public synchronized List<StickyRecord> stickyRecords()
        {
            return _state.stickyRecords();
        }

        @Override
        public synchronized List<StickyRecord> removeExpiredStickyFlags()
        {
            return new ArrayList();
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

    }


    private final Map<PnfsId, MetaDataRecord> _entryList = new HashMap();
    private final FileStore _repository;
    public MetaDataRepositoryHelper(FileStore repository) {
        _repository = repository;
    }

    @Override
    public MetaDataRecord create(PnfsId id) throws DuplicateEntryException, RepositoryException {
        MetaDataRecord entry = new CacheRepositoryEntryImpl(_repository, id);

        _entryList.put(id, entry);
        return entry;
    }

    @Override
    public MetaDataRecord create(MetaDataRecord entry) throws DuplicateEntryException, CacheException {

        _entryList.put(entry.getPnfsId(), entry);

        return entry;
    }

    @Override
    public MetaDataRecord get(PnfsId id) {
        return _entryList.get(id);
    }

    @Override
    public Collection<PnfsId> list() {
        return Collections.unmodifiableCollection(_entryList.keySet());
    }

    @Override
    public boolean isOk() {
        return true;
    }

    @Override
    public void remove(PnfsId id) {
        _entryList.remove(id);
    }

    @Override
    public void close() {
    }

    @Override
    public long getTotalSpace() {
        return 0;
    }

    @Override
    public long getFreeSpace() {
        return 0;
    }
}
