package org.dcache.pool.repository.v5;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;
import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.movers.IoMode;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.EntryState;
import org.dcache.pool.repository.FileRepositoryChannel;
import org.dcache.pool.repository.MetaDataRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.Repository;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.isEmpty;
import static com.google.common.collect.Iterables.unmodifiableIterable;
import static java.util.Collections.singleton;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.CHECKSUM;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIZE;

class WriteHandleImpl implements ReplicaDescriptor
{
    enum HandleState
    {
        OPEN, COMMITTED, CLOSED
    }

    private static final Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    /**
     * Time that a new CACHED file with no sticky flags will be marked
     * sticky.
     */
    private static final long HOLD_TIME = 5 * 60 * 1000; // 5 minutes

    /** Callback for resilience handling.  Pool name can be accessed here */
    private final CacheRepositoryV5 _repository;

    /** Space allocation is delegated to this allocator. */
    private final Allocator _allocator;

    /** The handler provides access to this entry. */
    private final MetaDataRecord _entry;

    /** File attributes of the file being written. */
    private final FileAttributes _fileAttributes;

    /** Stub for talking to the PNFS manager. */
    private final PnfsHandler _pnfs;

    /** Sticky flags to be applied after the transfer. */
    private final List<StickyRecord> _stickyRecords;

    /** The entry state used during transfer. */
    private final EntryState _initialState;

    /** The entry state used when the handle is committed. */
    private EntryState _targetState;

    /** The state of the write handle. */
    private HandleState _state;

    /** Amount of space allocated for this handle. */
    private long _allocated;

    /** Current thread which performs allocation. */
    private Thread _allocationThread;

    /** Last access time of new replica. */
    private Long _atime;

    WriteHandleImpl(CacheRepositoryV5 repository,
                    Allocator allocator,
                    PnfsHandler pnfs,
                    MetaDataRecord entry,
                    FileAttributes fileAttributes,
                    EntryState targetState,
                    List<StickyRecord> stickyRecords,
                    Set<Repository.OpenFlags> flags) throws IOException
    {
        _repository = checkNotNull(repository);
        _allocator = checkNotNull(allocator);
        _pnfs = checkNotNull(pnfs);
        _entry = checkNotNull(entry);
        _fileAttributes = checkNotNull(fileAttributes);
        _initialState = entry.getState();
        _targetState = checkNotNull(targetState);
        _stickyRecords = checkNotNull(stickyRecords);
        _state = HandleState.OPEN;
        _allocated = 0;

        checkState(_initialState != EntryState.FROM_CLIENT || _fileAttributes.isDefined(EnumSet.of(RETENTION_POLICY, ACCESS_LATENCY)));
        checkState(_initialState == EntryState.FROM_CLIENT || _fileAttributes.isDefined(SIZE));

        if (flags.contains(Repository.OpenFlags.CREATEFILE)) {
            File file = _entry.getDataFile();
            if (!file.createNewFile()) {
                throw new IOException("File exists when it should not: " + file);
            }
        }
    }

    private synchronized void setState(HandleState state)
    {
        _state = state;
        if (state != HandleState.OPEN && _allocationThread != null) {
            _allocationThread.interrupt();
        }
    }

    private synchronized boolean isOpen()
    {
        return _state == HandleState.OPEN;
    }

    @Override
    public RepositoryChannel createChannel() throws IOException {
        return new FileRepositoryChannel(getFile(), IoMode.WRITE.toOpenString());
    }

    /**
     * Sets the allocation thread to the calling thread. Blocks if
     * allocation thread is already set.
     *
     * @throws InterruptedException if thread is interrupted
     * @throws IllegalStateException if handle is closed
     */
    private synchronized void setAllocationThread()
        throws InterruptedException,
               IllegalStateException
    {
        while (_allocationThread != null) {
            wait();
        }

        if (!isOpen()) {
            throw new IllegalStateException("Handle is closed");
        }

        _allocationThread = Thread.currentThread();
    }

    /**
     * Clears the allocation thread field.
     */
    private synchronized void clearAllocationThread()
    {
        _allocationThread = null;
        notifyAll();
    }

    /**
     * Allocate space and block until space becomes available.
     *
     * @param size in bytes
     * @throws InterruptedException if thread is interrupted
     * @throws IllegalStateException if handle is closed
     * @throws IllegalArgumentException
     *             if <i>size</i> &lt; 0
     */
    @Override
    public void allocate(long size)
        throws IllegalStateException, IllegalArgumentException, InterruptedException
    {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative");
        }

        setAllocationThread();
        try {
            _allocator.allocate(size);
        } catch (InterruptedException e) {
            if (!isOpen()) {
                throw new IllegalStateException("Handle is closed");
            }
            throw e;
        } finally {
            clearAllocationThread();
        }

        synchronized (this) {
            _allocated += size;
            _entry.setSize(_allocated);
        }
    }

    /**
     * Allocate space if available. A non blocking version of {@link Allocator#allocate(long)}
     *
     * @param size in bytes
     * @throws InterruptedException if thread is interrupted
     * @throws IllegalStateException if handle is closed
     * @throws IllegalArgumentException if <i>size</i> &lt; 0
     * @return true if and only if the request space was allocated
     */
    @Override
    public boolean allocateNow(long size)
            throws IllegalStateException, IllegalArgumentException, InterruptedException {
        if (size < 0) {
            throw new IllegalArgumentException("Size is negative");
        }

        boolean isAllocated;
        setAllocationThread();
        try {
            isAllocated = _allocator.allocateNow(size);
        } catch (InterruptedException e) {
            if (!isOpen()) {
                throw new IllegalStateException("Handle is closed");
            }
            throw e;
        } finally {
            clearAllocationThread();
        }

        if (isAllocated) {
            synchronized (this) {
                _allocated += size;
                _entry.setSize(_allocated);
            }
        }
        return  isAllocated;
    }

    /**
     * Freeing space through a write handle is not supported. This
     * method always throws IllegalStateException.
     */
    @Override
    public void free(long size)
        throws IllegalStateException
    {
        throw new IllegalStateException("Space cannot be freed through a write handle");
    }

    /**
     * Adjust space reservation. Will log an error in case of under
     * allocation.
     */
    private synchronized void adjustReservation(long length)
        throws InterruptedException
    {
        try {
            if (_allocated < length) {
                _log.error("Under allocation detected. This is a bug. Please report it.");
                _allocator.allocate(length - _allocated);
            } else if (_allocated > length) {
                _allocator.free(_allocated - length);
            }
            _allocated = length;
            _entry.setSize(length);
        } catch (InterruptedException e) {
            /* Space allocation is broken now. The entry size
             * matches up with what was actually allocated,
             * however the file on disk is too large.
             *
             * Should only happen during shutdown, so no harm done.
             */
            _log.warn("Failed to adjust space reservation because the operation was interrupted. The pool is now over allocated.");
            throw e;
        }
    }

    private void registerFileAttributesInNameSpace()
            throws CacheException
    {
        FileAttributes attributesToUpdate = new FileAttributes();
        if (_fileAttributes.isDefined(CHECKSUM)) {
                /* PnfsManager detects conflicting checksums and will fail the update. */
            attributesToUpdate.setChecksums(_fileAttributes.getChecksums());
        }
        if (_initialState == EntryState.FROM_CLIENT) {
            attributesToUpdate.setAccessLatency(_fileAttributes.getAccessLatency());
            attributesToUpdate.setRetentionPolicy(_fileAttributes.getRetentionPolicy());
            if (_fileAttributes.isDefined(SIZE) && _fileAttributes.getSize() > 0) {
                attributesToUpdate.setSize(_fileAttributes.getSize());
            }
        }
        attributesToUpdate.setLocations(singleton(_repository.getPoolName()));

        _pnfs.setFileAttributes(_entry.getPnfsId(), attributesToUpdate);
    }

    private void verifyFileSize(long length) throws CacheException
    {
        assert _initialState == EntryState.FROM_CLIENT || _fileAttributes.isDefined(SIZE);
        if ((_initialState != EntryState.FROM_CLIENT ||
                (_fileAttributes.isDefined(SIZE) && _fileAttributes.getSize() > 0)) &&
                _fileAttributes.getSize() != length) {
            throw new FileCorruptedCacheException(_fileAttributes.getSize(), length);
        }
    }

    @Override
    public synchronized void commit()
        throws IllegalStateException, InterruptedException, CacheException
    {
        if (_state != HandleState.OPEN) {
            throw new IllegalStateException("Handle is closed");
        }

        try {
            if (_atime == null) {
                _entry.touch();
            } else {
                _entry.setLastAccessTime(_atime);
            }

            long length = getFile().length();
            adjustReservation(length);
            verifyFileSize(length);
            _fileAttributes.setSize(length);

            registerFileAttributesInNameSpace();

            _entry.update(r -> {
                r.setFileAttributes(_fileAttributes);
                /* In several situations, dCache requests a CACHED file
                 * without having any sticky flags on it. Such files are
                 * subject to immediate garbage collection if we are short on
                 * disk space. Thus to give other clients time to access the
                 * file, we mark it sticky for a short amount of time.
                 */
                if (_targetState == EntryState.CACHED && _stickyRecords.isEmpty()) {
                    long now = System.currentTimeMillis();
                    r.setSticky("self", now + HOLD_TIME, false);
                }

                /* Move entry to target state.
                 */
                for (StickyRecord record : _stickyRecords) {
                    r.setSticky(record.owner(), record.expire(), false);
                }
                return r.setState(_targetState);
            });

            setState(HandleState.COMMITTED);
        } catch (CacheException e) {
            /* If any of the PNFS operations return FILE_NOT_FOUND,
             * then we change the target state and the close method
             * will take care of removing the file.
             */
            if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                _targetState = EntryState.REMOVED;
            }
            throw e;
        }
    }

    /**
     * Fails the operation. Called by close without a successful
     * commit. The file is either removed or marked bad, depending on
     * its state.
     */
    private synchronized void fail()
    {
        long length = getFile().length();
        try {
            adjustReservation(length);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        /* Files from tape or from another pool are deleted in case of
         * errors.
         */
        if (_initialState == EntryState.FROM_POOL ||
            _initialState == EntryState.FROM_STORE) {
            _targetState = EntryState.REMOVED;
        }

        /* Unless replica is to be removed, register cache location and
         * other attributes.
         */
        if (_targetState != EntryState.REMOVED) {
            try {
                /* We register cache location separately in the failure flow, because
                 * updating other attributes (such as checksums) may itself trigger
                 * failures in PNFS, and at the very least our cache location should
                 * be registered.
                 */
                _pnfs.addCacheLocation(_entry.getPnfsId(), _repository.getPoolName());
                registerFileAttributesInNameSpace();
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    _targetState = EntryState.REMOVED;
                } else {
                    _log.warn("Failed to register {} after failed replica creation: {}",
                            _fileAttributes, e.getMessage());
                }
            }
        }

        if (_targetState == EntryState.REMOVED) {
            try {
                _entry.update(r -> r.setState(EntryState.REMOVED));
            } catch (CacheException e) {
                _log.warn("Failed to remove replica: {}", e.getMessage());
            }
        } else {
            PnfsId id = _entry.getPnfsId();
            _log.warn(AlarmMarkerFactory.getMarker(PredefinedAlarm.BROKEN_FILE,
                                                   id.toString(),
                                                   _repository.getPoolName()),
                      "Marking pool entry {} on {} as BROKEN",
                      _entry.getPnfsId(),
                      _repository.getPoolName());
            try {
                _entry.update(r -> r.setState(EntryState.BROKEN));
            } catch (CacheException e) {
                _log.warn("Failed to mark replica as broken: {}", e.getMessage());
            }
        }
    }

    @Override
    public synchronized void close()
        throws IllegalStateException
    {
        switch (_state) {
        case CLOSED:
            throw new IllegalStateException("Handle is closed");

        case OPEN:
            fail();
            setState(HandleState.CLOSED);
            break;

        case COMMITTED:
            setState(HandleState.CLOSED);
            break;
        }
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    @Override
    public synchronized File getFile() throws IllegalStateException
    {
        if (_state == HandleState.CLOSED) {
            throw new IllegalStateException("Handle is closed");
        }

        return _entry.getDataFile();
    }

    @Override
    public FileAttributes getFileAttributes()  throws IllegalStateException
    {
        return _fileAttributes;
    }

    @Override
    public synchronized Iterable<Checksum> getChecksums() throws CacheException
    {
        if (!_fileAttributes.isDefined(CHECKSUM)) {
            _fileAttributes.setChecksums(_pnfs
                    .getFileAttributes(_entry.getPnfsId(), EnumSet.of(CHECKSUM))
                    .getChecksums());
        }
        return unmodifiableIterable(_fileAttributes.getChecksums());
    }

    @Override
    public synchronized void addChecksums(Iterable<Checksum> checksums)
    {
        if (!isEmpty(checksums)) {
            Iterable<Checksum> newChecksums;
            if (_fileAttributes.isDefined(CHECKSUM)) {
                newChecksums = concat(_fileAttributes.getChecksums(), checksums);
            } else {
                newChecksums = checksums;
            }
            _fileAttributes.setChecksums(Sets.newHashSet(newChecksums));
        }
    }

    @Override
    public void setLastAccessTime(long time)
    {
        if (_state == HandleState.CLOSED) {
            throw new IllegalStateException("Handle is closed");
        }
        _atime = time;
    }
}
