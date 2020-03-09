package org.dcache.pool.repository.meta.db;

import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.sleepycat.je.EnvironmentFailureException;
import com.sleepycat.je.OperationFailureException;
import com.sleepycat.util.RuntimeExceptionWrapper;
import org.dcache.pool.repository.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.OpenOption;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.PnfsId;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.vehicles.StorageInfos;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.collect.Iterables.elementsEqual;
import static com.google.common.collect.Iterables.filter;
import static org.dcache.pool.repository.ReplicaState.*;

/**
 * Berkeley DB aware implementation of CacheRepositoryEntry interface.
 */
public class CacheRepositoryEntryImpl implements ReplicaRecord
{
    private static final Logger _log =
        LoggerFactory.getLogger(CacheRepositoryEntryImpl.class);

    // Reusable list for the common case
    private static final ImmutableList<StickyRecord> SYSTEM_STICKY =
            ImmutableList.of(new StickyRecord("system", -1));

    private final PnfsId _pnfsId;
    private final AbstractBerkeleyDBReplicaStore _repository;
    private  long _creationTime;

    /**
     * Sticky records held by the file.
     */
    private ImmutableList<StickyRecord> _sticky;

    private ReplicaState _state;

    private long _lastAccess;

    private int  _linkCount;

    private long _size;
    private  FileStore _fileStore;
    // cached storage info
    private StorageInfo _storageInfo;

    


    public CacheRepositoryEntryImpl(AbstractBerkeleyDBReplicaStore repository, PnfsId pnfsId, FileStore fileStore)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = NEW;
        _sticky = ImmutableList.of();
        _creationTime =   System.currentTimeMillis();
        _lastAccess = _creationTime;
        _fileStore = fileStore;
    }

    public CacheRepositoryEntryImpl(AbstractBerkeleyDBReplicaStore repository, PnfsId pnfsId, ReplicaState state,
                                    Collection<StickyRecord> sticky,  FileStore filestore)
    {
        _repository = repository;
        _pnfsId = pnfsId;
        _state = state;
        setStickyRecords(sticky);
        _fileStore = filestore;

        Long lastAccess = repository.getLastAccessInfo().get(pnfsId.toString());



        Long creationTime = repository.getCreatTimeInfo().get(pnfsId.toString());


        StorageInfo si = getStorageInfo();



            if (si ==null  ){

                try {
                    _size = _fileStore.getFileAttributeView(pnfsId).readAttributes().size();
                } catch (IOException e) {
                    e.printStackTrace();
                }


                _state = BROKEN;

            }

      else {

                Long size = (si) == null ? null : Long.valueOf(si.getLegacySize());


                if (lastAccess == null) {
                    try {
                        _lastAccess = (_fileStore.getFileAttributeView(pnfsId).readAttributes().creationTime().toMillis());


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {
                        _repository.setLastModifiedTime(pnfsId, _lastAccess);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    _lastAccess = lastAccess.longValue();
                }


                if (creationTime == null) {

                    try {
                        _creationTime = _fileStore.getFileAttributeView(pnfsId).readAttributes().creationTime().toMillis();
                        _repository.setCreationTime(pnfsId, _creationTime);


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    _creationTime = lastAccess.longValue();
                }
                // _lastAccess = lastAccess == null?  System.currentTimeMillis(): lastAccess.longValue();
                //_creationTime =  creationTime == null?  System.currentTimeMillis() : creationTime.longValue();
                // _size = size == null?  attributes.size() : size.longValue();


                if (size == null) {
                    try {
                        _size = _fileStore.getFileAttributeView(pnfsId).readAttributes().size();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    try {


                       // _repository.getStorageInfoMap().put(pnfsId.toString(), getFileAttributes().getStorageInfo());
                        _repository.setSize(pnfsId, _size);


                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } else {
                    _size = size.longValue();
                }

            }

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
        ReplicaState state = getState();
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

            Stopwatch stopwatch = Stopwatch.createStarted();

            _repository.setLastModifiedTime(_pnfsId, time);

        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set modification time for " + _pnfsId + ": " + e.toString(), e);
        }
        _lastAccess = time;
    }

    @Override
    public synchronized void setCreationTime(long time) throws CacheException
    {
        try {

            Stopwatch stopwatch = Stopwatch.createStarted();

            _repository.setCreationTime(_pnfsId, time);

        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set creation time for " + _pnfsId + ": " + e.toString(), e);
        }
        _lastAccess = time;
    }

    @Override
    public synchronized void setSize(long size) throws CacheException {
        try {

            _repository.setSize(_pnfsId, size);
        } catch (IOException e) {
            throw new DiskErrorCacheException("Failed to set modification time for " + _pnfsId + ": " + e.toString(), e);
        }
    }


    @Override
    public synchronized long getReplicaSize()
    {
        try {

            return _state.isMutable() ? _fileStore
                    .getFileAttributeView(_pnfsId)
                    .readAttributes()
                    .size() : _size;
        } catch (IOException e) {
            _log.error("Failed to read file size: {}", e.toString());
            return 0;
        }
    }

    private synchronized StorageInfo getStorageInfo()
    {

        if (_storageInfo == null){
            _storageInfo = _repository.getStorageInfoMap().get(_pnfsId.toString());

        }

        return _storageInfo;
    }

    @Override
    public synchronized FileAttributes getFileAttributes() throws CacheException
    {
        try {

            FileAttributes attributes = FileAttributes.ofPnfsId(_pnfsId);
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
            // invalidate cached value
            _storageInfo =  null;
            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {

                //System.out.println("TEST ATTRIBUTES ");
                _repository.getStorageInfoMap().put(id, StorageInfos.extractFrom(attributes));



            } else {
                _repository.getStorageInfoMap().remove(id);
            }

            if (attributes.isDefined(FileAttribute.STORAGEINFO)) {

                _repository.getStorageInfoMap().put(id, StorageInfos.extractFrom(attributes));



            } else {
                _repository.getStorageInfoMap().remove(id);
            }

            if (attributes.isDefined(FileAttribute.ACCESS_TIME)) {

                //System.out.println("TEST ATTRIBUTES " );

                _repository.getLastAccessInfo().put(id, attributes.getAccessTime());


            } else {
                _repository.getLastAccessInfo().remove(id);
            }

            if (attributes.isDefined(FileAttribute.CREATION_TIME)) {

                _repository.getCreatTimeInfo().put(id, attributes.getCreationTime());


            } else {
                _repository.getCreatTimeInfo().remove(id);
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
    public synchronized ReplicaState getState()
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
    public RepositoryChannel openChannel(Set<? extends OpenOption> mode) throws IOException
    {
        return _repository.openChannel(_pnfsId, mode);
    }

    @Override
    public Collection<StickyRecord> removeExpiredStickyFlags() throws CacheException
    {
        return update("removing expired sticky", r -> {
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
    public synchronized <T> T update(String why, Update<T> update) throws CacheException
    {
        AtomicReference<T> result = new AtomicReference<>();
        ReplicaState state = _state;
        ImmutableList<StickyRecord> sticky = _sticky;
        try {
            _repository.run(() -> {
                UpdatableRecordImpl record = new UpdatableRecordImpl();
                result.set(update.apply(record));
                record.save();
            });
        } catch (Exception e) {
            _state = state;
            _sticky = sticky;
            if (e instanceof EnvironmentFailureException && !_repository.isValid()) {
                throw new DiskErrorCacheException(
                        "Meta data update failed and a pool restart is required: " + e.getMessage(), e);
            }
            Throwables.propagateIfPossible(e, CacheException.class);
            throw new CacheException("Meta data update failed: " + e.getMessage(), e);
        }
        return result.get();
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
                                          FileStore fileStore) throws IOException
    {
        try {
            String id = pnfsId.toString();

            CacheRepositoryEntryState state = repository.getStateMap().get(id);

            //if (state != null || (state == null && attributes!=null) ) {
            if (state != null ) {
                return new CacheRepositoryEntryImpl(repository, pnfsId, state.getState(), state.stickyRecords(),  fileStore);
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
        return new CacheRepositoryEntryImpl(repository, pnfsId, BROKEN, ImmutableList.of(),  fileStore);
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
        public Void setState(ReplicaState state) throws CacheException
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
        public ReplicaState getState()
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
