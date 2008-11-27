package org.dcache.pool.repository.v5;

import org.apache.log4j.Logger;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.Checksum;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.FileSizeMismatchException;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.EntryState;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.io.File;
import java.io.IOException;

class WriteHandleImpl implements WriteHandle
{
    enum HandleState
    {
        OPEN, COMMITTED, CLOSED
    }

    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    /**
     * Time that a new CACHED file with no sticky flags will be marked
     * sticky.
     */
    private static long HOLD_TIME = 5 * 60 * 1000; // 5 minutes

    private final CacheRepositoryV5 _repository;

    /** Space allocation is delegated to this space monitor. */
    private final SpaceMonitor _monitor;

    /** The handler provides access to this entry. */
    private final CacheRepositoryEntry _entry;

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

    WriteHandleImpl(CacheRepositoryV5 repository,
                    SpaceMonitor monitor,
                    PnfsHandler pnfs,
                    CacheRepositoryEntry entry,
                    StorageInfo info,
                    EntryState initialState,
                    EntryState targetState,
                    List<StickyRecord> stickyRecords)
        throws CacheException, IOException
    {
        switch (initialState) {
        case FROM_CLIENT:
        case FROM_STORE:
        case FROM_POOL:
            break;
        default:
            throw new IllegalArgumentException("Invalid initial state");
        }

        switch (targetState) {
        case PRECIOUS:
        case CACHED:
            break;
        default:
            throw new IllegalArgumentException("Invalid target state");
        }

        _repository = repository;
        _monitor = monitor;
        _pnfs = pnfs;
        _entry = entry;
        _initialState = initialState;
        _targetState = targetState;
        _stickyRecords = stickyRecords;
        _state = HandleState.OPEN;
        _allocated = 0;

        if (getFile().exists())
            throw new CacheException(CacheException.PANIC,
                                     "File exists, although we didn't expect it to");

        _entry.lock(true);
        _entry.setStorageInfo(info);

        _repository.setState(_entry, initialState);
    }


    /**
     * Allocate space and block until space becomes available.
     *
     * @param size in bytes
     * @throws InterruptedException
     * @throws IllegalStateException if EntryIODescriptor is closed or
     * READ-ONLY
     * @throws IllegalArgumentException
     *             if <i>size</i> < 0
     */
    public void allocate(long size)
        throws IllegalStateException, IllegalArgumentException, InterruptedException
    {
        if (_state != HandleState.OPEN)
            throw new IllegalStateException("Handle is closed");

        _monitor.allocateSpace(size);
        _allocated += size;
        _entry.setSize(_allocated);
    }

    /**
     * Allocate space and block specified time until space becomes available.
     *
     * @param size in bytes
     * @param time to block in milliseconds
     * @throws InterruptedException
     * @throws IllegalStateException if EntryIODescriptor is closed or READ-ONLY
     * @throws TimeoutException if request timed out
     * @throws IllegalArgumentException if either<code>size</code> or
     *             <code>time</code> is negative.
     */
    public void allocate(long size, long time)
        throws IllegalStateException, IllegalArgumentException,
               InterruptedException, TimeoutException
    {
        if (_state != HandleState.OPEN)
            throw new IllegalStateException("Handle is closed");

        _monitor.allocateSpace(size, time);
        _allocated += size;
        _entry.setSize(_allocated);
    }

    /**
     * Adjust space reservation. Will log an error in case of under
     * allocation.
     */
    private void adjustReservation(long length)
        throws InterruptedException
    {
        try {
            if (_allocated < length) {
                _log.error("Under allocation detected. This is a bug. Please report it.");
                allocate(length - _allocated);
            } else if (_allocated > length) {
                _monitor.freeSpace(_allocated - length);
            }
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

    /**
     * Compare and update checksum. For now we only store the checksum
     * locally. TODO: Compare and update in PNFS.
     */
    private void setChecksum(StorageInfo info, Checksum checksum)
        throws CacheException
    {
        String flags = info.getKey("flag-c");
        if (flags == null) {
            info.setKey("flag-c", checksum.toString());
            _entry.setStorageInfo(info);
        } else if (!checksum.equals(new Checksum(flags))) {
            throw new CacheException(String.format("Checksum error: file=%s, expected=%s",
                                                   checksum, flags));
        }
    }

    /**
     * Set file size in PNFS.
     *
     * If this is a new file, i.e. we did not get it from tape or
     * another pool, then update the size in the storage info and in
     * PNFS.  Otherwise fail the operation if the file size is wrong.
     */
    private void setFileSize(StorageInfo info, long length)
        throws CacheException
    {
        if (_initialState == EntryState.FROM_CLIENT &&
            info.getFileSize() == 0) {
            info.setFileSize(length);
            _entry.setStorageInfo(info);
            _pnfs.setFileSize(_entry.getPnfsId(), length);
        } else if (info.getFileSize() != length) {
            throw new CacheException("File does not have expected length");
        }
    }

    private void registerCacheLocation()
        throws CacheException
    {
        _pnfs.addCacheLocation(_entry.getPnfsId());
    }


    private void setToTargetState()
        throws CacheException
    {
        /* In several situations, dCache requests a CACHED file
         * without having any sticky flags on it. Such files are
         * subject to immediate garbage collection if we are short on
         * disk space. Thus to give other clients time to access the
         * file, we mark it sticky for a short amount of time.
         */
        if (_targetState == EntryState.CACHED && _stickyRecords.isEmpty()) {
            long now = System.currentTimeMillis();
            _entry.setSticky("self", now + HOLD_TIME, false);
        }
        
        /* Move entry to target state.
         */
        for (StickyRecord record: _stickyRecords) {
            _entry.setSticky(record.owner(), record.expire(), false);
        }
        _repository.setState(_entry, _targetState);
    }

    public void commit(Checksum checksum)
        throws IllegalStateException, InterruptedException, CacheException
    {
        if (_state != HandleState.OPEN)
            throw new IllegalStateException("Handle is closed");

        try {
            long length = getFile().length();
            adjustReservation(length);

            StorageInfo info = _entry.getStorageInfo();

            if (checksum != null) {
                setChecksum(info, checksum);
            }

            setFileSize(info, length);

            /* Register cache location. Should this fail due to
             * FILE_NOT_FOUND, then the catch below will cause the
             * replica to be removed. Should it fail for any other
             * reason, then the file will be marked broken and the
             * pool will repeat the registration step at the next
             * start.
             */
            registerCacheLocation();

            setToTargetState();

            _state = HandleState.COMMITTED;
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
     * Fails the operation. Called by close without a successfulc
     * commit. The file is either removed or marked bad, depending on
     * its state.
     */
    private void fail()
    {
        long length = getFile().length();
        try {
            adjustReservation(length);
        } catch (InterruptedException e) {
            // Carry on
        }

        /* Files from tape or from another pool are deleted in case of
         * errors.
         */
        if (_initialState == EntryState.FROM_POOL ||
            _initialState == EntryState.FROM_STORE) {
            _targetState = EntryState.REMOVED;
        }

        /* Register cache location unless replica is to be
         * removed.
         */
        if (_targetState != EntryState.REMOVED) {
            try {
                registerCacheLocation();
            } catch (CacheException e) {
                if (e.getRc() == CacheException.FILE_NOT_FOUND) {
                    _targetState = EntryState.REMOVED;
                }
            }
        }

        if (_targetState == EntryState.REMOVED) {
            /* A locked entry cannot be removed, thus we need to
             * unlock it before setting the state.
             */
            _entry.lock(false);
            _repository.setState(_entry, EntryState.REMOVED);
        } else {
            _log.warn("Marking pool entry as BROKEN");
            _repository.setState(_entry, EntryState.BROKEN);
            _entry.lock(false);
        }
    }

    public void close()
        throws IllegalStateException
    {
        switch (_state) {
        case CLOSED:
            throw new IllegalStateException("Handle is closed");

        case OPEN:
            fail();
            _state = HandleState.CLOSED;
            break;

        case COMMITTED:
            _state = HandleState.CLOSED;
            _entry.lock(false);
            break;
        }
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public File getFile() throws IllegalStateException
    {
        if (_state == HandleState.CLOSED)
            throw new IllegalStateException("Handle is closed");

        try {
            return _entry.getDataFile();
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }

    /**
     *
     * @return cache entry
     * @throws IllegalStateException
     */
    public CacheEntry getEntry()  throws IllegalStateException
    {
        if (_state == HandleState.CLOSED)
            throw new IllegalStateException("Handle is closed");

        try {
            return new CacheEntryImpl(_entry, _repository.getState(_entry.getPnfsId()));
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }
}