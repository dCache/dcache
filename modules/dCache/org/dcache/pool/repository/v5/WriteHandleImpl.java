package org.dcache.pool.repository.v5;

import org.apache.log4j.Logger;

import diskCacheV111.repository.CacheRepositoryEntry;
import diskCacheV111.repository.SpaceMonitor;
import diskCacheV111.util.PnfsHandler;
import diskCacheV111.vehicles.StorageInfo;
import diskCacheV111.util.CacheException;

import org.dcache.pool.repository.StickyRecord;
import org.dcache.pool.repository.FileSizeMismatchException;
import org.dcache.pool.repository.CacheEntry;
import org.dcache.pool.repository.WriteHandle;
import org.dcache.pool.repository.EntryState;

import java.util.concurrent.TimeoutException;
import java.io.File;
import java.io.IOException;

class WriteHandleImpl implements WriteHandle
{
    private static Logger _log =
        Logger.getLogger("logger.org.dcache.repository");

    private final CacheRepositoryV5 _repository;

    /** Space allocation is delegated to this space monitor. */
    private final SpaceMonitor _monitor;

    /** The handler provides access to this entry. */
    private final CacheRepositoryEntry _entry;

    /** Stub for talking to the PNFS manager. */
    private final PnfsHandler _pnfs;

    /** If non-null the file is marked sticky after transfer. */
    private final StickyRecord _sticky;

    /** The entry state used during transfer. */
    private final EntryState _initialState;

    /** The entry state used when the handle is closed. */
    private EntryState _targetState;

    /** True while the handle is open. */
    private boolean _open;

    /** Amount of space allocated for this handle. */
    private long _allocated;


    WriteHandleImpl(CacheRepositoryV5 repository,
                    SpaceMonitor monitor,
                    PnfsHandler pnfs,
                    CacheRepositoryEntry entry,
                    StorageInfo info,
                    EntryState initialState,
                    EntryState targetState,
                    StickyRecord sticky)
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
        _sticky = sticky;
        _open = true;
        _allocated = 0;

        if (!getFile().createNewFile())
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
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        _monitor.allocateSpace(size);
        _allocated += size;
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
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        _monitor.allocateSpace(size, time);
        _allocated += size;
    }


    /**
     * Cancels the process of creating a replica.
     *
     * If the replica is kept, its entry is flagged as bad. Otherwise
     * the replica is deleted, allocated space is released and the
     * entry will be removed.
     *
     * The handle will not be closed and the close method must still
     * be called.
     */
    public void cancel(boolean keep) throws IllegalStateException
    {
        if (keep) {
            _targetState = EntryState.BROKEN;
        } else {
            _targetState = EntryState.REMOVED;
        }
    }

    /**
     * Closes the write handle. The file must not be modified after
     * the handle has been closed and the handle itself must be
     * discarded.
     *
     * Closing the handle adjust space reservation to match the actual
     * file size. It may cause the file size in the storage info and
     * in PNFS to be updated.
     *
     * Closing the handle sets the repository entry to its target
     * state.
     *
     * In case of problems, the entry is marked bad and an exception
     * is thrown.
     *
     * Closing a handle multiple times causes an
     * IllegalStateException.
     *
     * @throws IllegalStateException if EntryIODescriptor is closed.
     * @throws FileSizeMismatchException if file size does not match
     * the expected size.
     * @throws CacheException if the repository or PNFS state could
     * not be updated.
     */
    public void close()
        throws IllegalStateException, InterruptedException, CacheException
    {
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        try {
            /* Adjust reservation.
             */
            long length = getFile().length();
            try {
                if (_allocated < length) {
                    _log.error("Underallocation");
                    allocate(length - _allocated);
                } else if (_allocated > length) {
                    _monitor.freeSpace(_allocated - length);
                }
            } catch (InterruptedException e) {
                // FIXME: Space allocation is broken now.
                throw e;
            }

            StorageInfo info = _entry.getStorageInfo();

            /* If this is a new file, i.e. we did not get it from tape
             * or another pool, then update the size in the storage
             * info and in PNFS.
             */
            if (_initialState == EntryState.FROM_CLIENT
                && info.getFileSize() == 0
                && _targetState != EntryState.REMOVED) {
                info.setFileSize(length);
                _entry.setStorageInfo(info);

                _pnfs.setFileSize(_entry.getPnfsId(), length);
            }

            /* Fail the transfer if file size does not match.
             */
            if (info.getFileSize() != length) {
                if (_targetState != EntryState.REMOVED)
                    _targetState = EntryState.BROKEN;
                _log.error("File does not have expected length. Marking it bad.");
            }

            /* Apply sticky bit before making the file available.
             */
            if (_sticky != null)
                _entry.setSticky(true, _sticky.owner(), _sticky.expire());
        } catch (CacheException e) {
            _targetState = EntryState.BROKEN;
            throw e;
        } finally {
            if (_targetState != EntryState.REMOVED) {
                _pnfs.addCacheLocation(_entry.getPnfsId());
            }

            _entry.lock(false);
            _repository.setState(_entry, _targetState);
            _open = false;
        }
    }

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    public File getFile() throws IllegalStateException
    {
        if (!_open)
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
        if (!_open)
            throw new IllegalStateException("Handle is closed");

        try {
            return new CacheEntryImpl(_entry, _repository.getState(_entry.getPnfsId()));
        } catch (CacheException e) {
            throw new RuntimeException("Internal repository error: "
                                       + e.getMessage());
        }
    }
}