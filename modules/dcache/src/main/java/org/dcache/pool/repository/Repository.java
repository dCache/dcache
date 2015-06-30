package org.dcache.pool.repository;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.LockedCacheException;
import diskCacheV111.util.PnfsId;

import org.dcache.pool.FaultListener;
import org.dcache.vehicles.FileAttributes;

public interface Repository
    extends Iterable<PnfsId>
{
    enum OpenFlags
    {
        /* Do not update the file last access time when the file is
         * read.
         */
        NOATIME,

        /* Create the data file when creating an entry.
         */
        CREATEFILE
    }

    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once before any other operation can be
     * performed. Depending on the implementation, some methods may
     * not be accesible until after the load method has been called.
     *
     * @throws IllegalStateException if called multiple times
     */
    void init()
            throws IllegalStateException, CacheException;

    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once after init was called.
     *
     * @throws IllegalStateException if called multiple times
     * @throws IOException if an io error occurs
     * @throws CacheException in case of other errors
     * @throws InterruptedException if thread was interrupted
     */
    void load()
        throws InterruptedException,
               CacheException, IllegalStateException;

    /**
     * Creates entry in the repository. Returns a write handle for the
     * entry. The write handle must be explicitly closed. As long as
     * the write handle is not closed, reads are not allowed on the
     * entry.
     *
     * While the handle is open, the entry is in the transfer
     * state. Once the handle is closed, the entry is automatically
     * moved to the target state, unless the handle is cancelled
     * first.
     *
     * @param id the PNFS ID of the new entry
     * @param info the storage info of the new entry
     * @param transferState the transfer state
     * @param targetState the target state
     * @param sticky sticky record to apply to entry; can be null
     * @param flags options that influence how the entry is created
     * @return A write handle for the entry.
     * @throws FileInCacheException if an entry with the same ID
     * already exists.
     */
    ReplicaDescriptor createEntry(FileAttributes fileAttributes,
                                  EntryState transferState,
                                  EntryState targetState,
                                  List<StickyRecord> sticky,
                                  Set<OpenFlags> flags)
        throws FileInCacheException;

    /**
     * Opens an entry for reading.
     *
     * A read handle is returned which is used to access the file. The
     * read handle must be explicitly closed after use. While the read
     * handle is open, it acts as a shared lock, which prevents the
     * entry from being deleted. Notice that an open read handle does
     * not prevent state changes.
     *
     * @param id the PNFS ID of the entry to open
     * @param flags options that influence how the entry is opened
     * @return IO descriptor
     * @throws InterruptedException if thread was interrupted
     * @throws FileNotInCacheException if file not found
     * @throws LockedCacheException if in a state in which it cannot be opened
     * @throws CacheException in case of other errors
     */
    ReplicaDescriptor openEntry(PnfsId id, Set<OpenFlags> flags)
        throws CacheException,
               InterruptedException;

    /**
     * Returns information about an entry. Equivalent to calling
     * <code>getEntry</code> on a read handle, but avoid the cost of
     * creating a read handle.
     *
     * @param id the PNFS ID of the entry to open
     * @throws InterruptedException if thread was interrupted
     * @throws FileNotInCacheException if file not found or in a state
     * in which it cannot be opened
     * @throws CacheException in case of other errors
     */
    CacheEntry getEntry(PnfsId id)
        throws CacheException,
               InterruptedException;

    /**
     * Sets the lifetime of a named sticky flag. If expiration time is
     * -1, then the sticky flag never expires. If is is 0, the flag
     * expires immediately.
     *
     * @param id the PNFS ID of the entry for which to change the flag
     * @param owner the owner of the sticky flag
     * @param expire expiration time in milliseconds since the epoch
     * @param overwrite replace existing flag when true, extend
     *                  lifetime if false
     * @throws InterruptedException if thread was interrupted
     * @throws FileNotInCacheException when an entry with the given id
     * is not found in the repository
     * @throws CacheException in case of other errors
     * @throws IllegalArgumentException when <code>id</code> or
     * <code>owner</code> are null or when <code>lifetime</code> is
     * smaller than -1.
     */
    void setSticky(PnfsId id, String owner, long expire, boolean overwrite)
        throws IllegalArgumentException,
               InterruptedException,
               CacheException;

    /**
     * Returns information about the size and space usage of the
     * repository.
     *
     * @return snapshot of current space usage record
     */
    SpaceRecord getSpaceRecord();

    /**
     * Returns the state of an entry.
     *
     * @param id the PNFS ID of an entry
     * @throws InterruptedException if thread was interrupted
     * @throws CacheException in case of other errors
     */
    EntryState getState(PnfsId id)
        throws InterruptedException,
               CacheException;

    /**
     * Sets the state of an entry. Only the following transitions are
     * allowed:
     *
     * <ul>
     * <li>{NEW, REMOVED, DESTROYED} to REMOVED.
     * <li>{PRECIOUS, CACHED, BROKEN} to {PRECIOUS, CACHED, BROKEN, REMOVED}.
     * </ul>
     *
     * @param id a PNFS ID
     * @param state an entry state
     * @throws IllegalTransitionException if the transition is illegal.
     * @throws IllegalArgumentException if <code>id</code> is null.
     * @throws InterruptedException if thread was interrupted
     * @throws CacheException in case of other errors
     */
    void setState(PnfsId id, EntryState state)
        throws IllegalTransitionException, IllegalArgumentException,
               InterruptedException, CacheException;

    /**
     * Adds a state change listener.
     */
    void addListener(StateChangeListener listener);

    /**
     * Removes a state change listener.
     */
    void removeListener(StateChangeListener listener);

    /**
     * Adds a state change listener.
     */
    void addFaultListener(FaultListener listener);

    /**
     * Removes a fault change listener.
     */
    void removeFaultListener(FaultListener listener);
}
