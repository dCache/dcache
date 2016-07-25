package org.dcache.tests.repository;

import java.io.IOException;
import java.net.URI;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.namespace.FileAttribute;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.DuplicateEntryException;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaStore;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.toList;

public class ReplicaStoreHelper implements ReplicaStore
{



    public static class CacheRepositoryEntryImpl implements ReplicaRecord, ReplicaRecord.UpdatableRecord {
        private final PnfsId _pnfsId;

        private long _creationTime = System.currentTimeMillis();

        private long _lastAccess = _creationTime;

        private int  _linkCount;

        private long _size;

        private ReplicaState _state;

        private List<StickyRecord> _sticky = new ArrayList<>();

        private final FileStore _repository;
        private FileAttributes _fileAttributes;

        public CacheRepositoryEntryImpl(FileStore repository, PnfsId pnfsId) throws IOException
        {
            _repository = repository;
            _pnfsId = pnfsId;
            BasicFileAttributes attributes = repository.getFileAttributeView(pnfsId).readAttributes();
            _lastAccess = attributes.lastModifiedTime().toMillis();
            _size = attributes.size();
            _state = ReplicaState.NEW;
            _fileAttributes = new FileAttributes();
        }

        @Override
        public synchronized int decrementLinkCount()
        {
            assert _linkCount > 0;
            _linkCount--;
            return _linkCount;
        }


        @Override
        public synchronized int incrementLinkCount()
        {
            _linkCount++;
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
        public synchronized void setLastAccessTime(long time)
        {
            _lastAccess = time;
        }

        @Override
        public synchronized int getLinkCount()
        {
            return _linkCount;
        }

        @Override
        public synchronized long getReplicaSize()
        {
            return _size;
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
        public synchronized ReplicaState getState()
        {
            return _state;
        }

        @Override
        public Void setState(ReplicaState state)
        {
            _state = state;
            return null;
        }

        @Override
        public synchronized boolean isSticky()
        {
            return !_sticky.isEmpty();
        }

        @Override
        public synchronized URI getReplicaUri()
        {
            return _repository.get(_pnfsId);
        }

        @Override
        public RepositoryChannel openChannel(IoMode mode) throws IOException
        {
            return _repository.openDataChannel(_pnfsId, mode);
        }

        @Override
        public boolean setSticky(String owner, long expire, boolean overwrite)
            throws CacheException
        {
            if (!overwrite && _sticky.stream().anyMatch(r -> r.owner().equals(owner) && r.isValidAt(expire))) {
                return false;
            }
            _sticky = Stream.concat(_sticky.stream().filter(r -> !r.owner().equals(owner)),
                                    Stream.of(new StickyRecord(owner, expire)))
                    .collect(toList());
            return true;
        }

        @Override
        public synchronized Collection<StickyRecord> stickyRecords()
        {
            return _sticky;
        }

        @Override
        public synchronized <T> T update(Update<T> update) throws CacheException
        {
            return update.apply(this);
        }

        @Override
        public synchronized Collection<StickyRecord> removeExpiredStickyFlags()
        {
            return Collections.emptyList();
        }

        @Override
        public Void setFileAttributes(FileAttributes attributes) throws CacheException
        {
            _fileAttributes = attributes;
            return null;
        }

        @Override
        public FileAttributes getFileAttributes()
        {
            return _fileAttributes;
        }

        @Override
        public synchronized String toString()
        {
            return _pnfsId.toString() +
                   " <" + _state.toString() + "-" +
                   "(0)" +
                   "[" + getLinkCount() + "]> " +
                   getReplicaSize() +
                   " si={" + (_fileAttributes.isDefined(FileAttribute.STORAGECLASS) ? "<unknown>" : _fileAttributes.getStorageClass()) + "}" ;
        }

    }


    private final Map<PnfsId, ReplicaRecord> _entryList = new HashMap<>();
    private final FileStore _repository;
    public ReplicaStoreHelper(FileStore repository) {
        _repository = repository;
    }

    @Override
    public ReplicaRecord create(PnfsId id, Set<Repository.OpenFlags> flags) throws DuplicateEntryException, RepositoryException {
        try {
            ReplicaRecord entry = new CacheRepositoryEntryImpl(_repository, id);
            _entryList.put(id, entry);
            return entry;
        } catch (IOException e) {
            throw new RepositoryException("Failed to create entry: " + e, e);
        }
    }

    @Override
    public ReplicaRecord get(PnfsId id) throws RepositoryException
    {
        try {
            if (!_repository.contains(id)) {
                return null;
            }
            return _entryList.containsKey(id) ? _entryList.get(id) : new CacheRepositoryEntryImpl(_repository, id);
        } catch (IOException e) {
            throw new RepositoryException("Failed to open entry: " + e, e);
        }
    }

    @Override
    public void init() throws CacheException
    {
    }

    @Override
    public Set<PnfsId> index(IndexOption... options) {
        return Collections.unmodifiableSet(_entryList.keySet());
    }

    @Override
    public boolean isOk() {
        return true;
    }

    @Override
    public void remove(PnfsId id) throws DiskErrorCacheException
    {
        try {
            _repository.remove(id);
            _entryList.remove(id);
        } catch (IOException e) {
            throw new DiskErrorCacheException(
                    "Failed to remove meta data for " + id + ": " + e.getMessage(), e);
        }
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
