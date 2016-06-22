package org.dcache.tests.repository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributeView;
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
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.MetaDataStore;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.vehicles.FileAttributes;

import static java.util.stream.Collectors.toList;

public class MetaDataRepositoryHelper implements MetaDataStore {



    public static class CacheRepositoryEntryImpl implements MetaDataRecord, MetaDataRecord.UpdatableRecord {
        private final PnfsId _pnfsId;

        private long _creationTime = System.currentTimeMillis();

        private long _lastAccess = _creationTime;

        private int  _linkCount;

        private long _size;

        private EntryState _state;

        private List<StickyRecord> _sticky = new ArrayList<>();

        private final FileStore _repository;
        private FileAttributes _fileAttributes;

        public CacheRepositoryEntryImpl(FileStore repository, PnfsId pnfsId) throws IOException
        {
            _repository = repository;
            _pnfsId = pnfsId;
            Path path = _repository.get(_pnfsId);
            BasicFileAttributes attributes =
                    Files.getFileAttributeView(path, BasicFileAttributeView.class).readAttributes();
            _lastAccess = attributes.lastModifiedTime().toMillis();
            _size = attributes.size();
            _state = EntryState.NEW;
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
        public synchronized EntryState getState()
        {
            return _state;
        }

        @Override
        public Void setState(EntryState state)
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
        public synchronized File getDataFile()
        {
            return _repository.get(_pnfsId).toFile();
        }

        @Override
        public RepositoryChannel openChannel(IoMode mode) throws IOException
        {
            return new FileRepositoryChannel(_repository.get(_pnfsId), mode.toOpenString());
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


    private final Map<PnfsId, MetaDataRecord> _entryList = new HashMap<>();
    private final FileStore _repository;
    public MetaDataRepositoryHelper(FileStore repository) {
        _repository = repository;
    }

    @Override
    public MetaDataRecord create(PnfsId id) throws DuplicateEntryException, RepositoryException {
        try {
            MetaDataRecord entry = new CacheRepositoryEntryImpl(_repository, id);
            _entryList.put(id, entry);
            return entry;
        } catch (IOException e) {
            throw new RepositoryException("Failed to create entry: " + e, e);
        }
    }

    @Override
    public MetaDataRecord get(PnfsId id) throws RepositoryException
    {
        try {
            if (!Files.exists(_repository.get(id))) {
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
            Files.deleteIfExists(_repository.get(id));
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
