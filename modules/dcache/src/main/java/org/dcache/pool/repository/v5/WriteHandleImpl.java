package org.dcache.pool.repository.v5;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.OpenOption;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileCorruptedCacheException;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.PnfsId;

import org.dcache.alarms.AlarmMarkerFactory;
import org.dcache.alarms.PredefinedAlarm;
import org.dcache.pool.repository.Allocator;
import org.dcache.pool.repository.AllocatorAwareRepositoryChannel;
import org.dcache.pool.repository.FileStore;
import org.dcache.pool.repository.ReplicaState;
import org.dcache.pool.repository.ReplicaRecord;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.pool.repository.RepositoryChannel;
import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.statistics.IoStatisticsReplicaRecord;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.*;
import static org.dcache.namespace.FileAttribute.*;

class WriteHandleImpl implements ReplicaDescriptor
{
    enum HandleState
    {
        OPEN, COMMITTED, CLOSED
    }

    private static final Logger _log =
        LoggerFactory.getLogger("logger.org.dcache.repository");

    private static final Set<OpenOption> OPEN_OPTIONS = ImmutableSet.<OpenOption>builder()
            .addAll(FileStore.O_RW)
            .add(IoStatisticsReplicaRecord.OpenFlags.ENABLE_IO_STATISTICS)
            .build();

    /**
     * Time that a new CACHED file with no sticky flags will be marked
     * sticky.
     */
    private static final long HOLD_TIME = 5 * 60 * 1000; // 5 minutes

    /** Callback for resilience handling.  Pool name can be accessed here */
    private final ReplicaRepository _repository;

    /** Space allocation is delegated to this allocator. */
    private final Allocator _allocator;

    /** The handler provides access to this entry. */
    private final ReplicaRecord _entry;

    /** File attributes of the file being written. */
    private final FileAttributes _fileAttributes;

    /** Stub for talking to the PNFS manager. */
    private final PnfsHandler _pnfs;

    /** Sticky flags to be applied after the transfer. */
    private final List<StickyRecord> _stickyRecords;

    /** The entry state used during transfer. */
    private final ReplicaState _initialState;

    /** The entry state used when the handle is committed. */
    private ReplicaState _targetState;

    /** The state of the write handle. */
    private HandleState _state;

    /** Last access time of new replica. */
    private Long _atime;

    /**
     * Tells, should allocator block for available space or not.
     */
    private final boolean _useHardAllocator;

    WriteHandleImpl(ReplicaRepository repository,
                    Allocator allocator,
                    PnfsHandler pnfs,
                    ReplicaRecord entry,
                    FileAttributes fileAttributes,
                    ReplicaState targetState,
                    List<StickyRecord> stickyRecords,
                    boolean useHardAllocator)
    {
        _repository = checkNotNull(repository);
        _allocator = checkNotNull(allocator);
        _pnfs = checkNotNull(pnfs);
        _entry = checkNotNull(entry);
        _fileAttributes = checkNotNull(fileAttributes);
        _initialState = entry.getState();
        _targetState = checkNotNull(targetState);
        _stickyRecords = checkNotNull(stickyRecords);
        _useHardAllocator = useHardAllocator;
        _state = HandleState.OPEN;

        checkState(_initialState != ReplicaState.FROM_CLIENT || _fileAttributes.isDefined(EnumSet.of(RETENTION_POLICY, ACCESS_LATENCY)));
        checkState(_initialState == ReplicaState.FROM_CLIENT || _fileAttributes.isDefined(SIZE));
    }

    private synchronized void setState(HandleState state)
    {
        _state = state;
    }

    @Override
    public synchronized RepositoryChannel createChannel() throws IOException {

        if (_state == HandleState.CLOSED) {
            throw new IllegalStateException("Handle is closed");
        }

        return new AllocatorAwareRepositoryChannel(_entry.openChannel(OPEN_OPTIONS),
                _allocator,  _useHardAllocator);
    }

    private void registerFileAttributesInNameSpace()
            throws CacheException
    {
        FileAttributes attributesToUpdate = FileAttributes.ofLocation(_repository.getPoolName());
        if (_fileAttributes.isDefined(CHECKSUM)) {
                /* PnfsManager detects conflicting checksums and will fail the update. */
            attributesToUpdate.setChecksums(_fileAttributes.getChecksums());
        }
        if (_initialState == ReplicaState.FROM_CLIENT) {
            attributesToUpdate.setAccessLatency(_fileAttributes.getAccessLatency());
            attributesToUpdate.setRetentionPolicy(_fileAttributes.getRetentionPolicy());
            if (_fileAttributes.isDefined(SIZE) && _fileAttributes.getSize() > 0) {
                attributesToUpdate.setSize(_fileAttributes.getSize());
            }
        }

        _pnfs.setFileAttributes(_entry.getPnfsId(), attributesToUpdate);
    }

    private void verifyFileSize(long length) throws CacheException
    {
        assert _initialState == ReplicaState.FROM_CLIENT || _fileAttributes.isDefined(SIZE);
        if ((_initialState != ReplicaState.FROM_CLIENT ||
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
            _entry.setLastAccessTime((_atime == null) ? System.currentTimeMillis() : _atime);

            long length = _entry.getReplicaSize();
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
                if (_targetState == ReplicaState.CACHED && _stickyRecords.isEmpty()) {
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
                _targetState = ReplicaState.REMOVED;
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

        /* Files from tape or from another pool are deleted in case of
         * errors.
         */
        if (_initialState == ReplicaState.FROM_POOL ||
            _initialState == ReplicaState.FROM_STORE) {
            _targetState = ReplicaState.REMOVED;
        }

        /* If nothing was uploaded, we delete the replica and leave the name space
         * entry it is virgin state.
         */
        long length = _entry.getReplicaSize();
        if (length == 0) {
            _targetState = ReplicaState.REMOVED;
        }

        /* Unless replica is to be removed, register cache location and
         * other attributes.
         */
        if (_targetState != ReplicaState.REMOVED) {
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
                    _targetState = ReplicaState.REMOVED;
                } else {
                    _log.warn("Failed to register {} after failed replica creation: {}",
                            _fileAttributes, e.getMessage());
                }
            }
        }

        if (_targetState == ReplicaState.REMOVED) {
            try {
                _entry.update(r -> r.setState(ReplicaState.REMOVED));
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
                _entry.update(r -> r.setState(ReplicaState.BROKEN));
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
    public synchronized URI getReplicaFile() throws IllegalStateException
    {
        if (_state == HandleState.CLOSED) {
            throw new IllegalStateException("Handle is closed");
        }

        return _entry.getReplicaUri();
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

    @Override
    public long getReplicaSize()
    {
        return _entry.getReplicaSize();
    }
}
