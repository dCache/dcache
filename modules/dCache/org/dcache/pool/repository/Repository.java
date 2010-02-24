package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;
import diskCacheV111.util.FileInCacheException;
import diskCacheV111.util.FileNotInCacheException;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.StorageInfo;

import org.dcache.pool.repository.v3.RepositoryException;
import org.dcache.pool.FaultListener;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Iterator;
import java.util.List;

public interface Repository
    extends Iterable<PnfsId>
{
    /**
     * Loads the repository from the on disk state. Must be done
     * exactly once before any other operation can be performed.
     *
     * @throws IllegalStateException if called multiple times
     * @throws IOException if an io error occurs
     * @throws RepositoryException in case of other internal errors
     */
    void init()
        throws IOException, RepositoryException, IllegalStateException;

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
     * @return A write handle for the entry.
     * @throws FileInCacheException if an entry with the same ID
     * already exists.
     */
    WriteHandle createEntry(PnfsId id,
                            StorageInfo info,
                            EntryState transferState,
                            EntryState targetState,
                            List<StickyRecord> sticky)
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
     * TODO: Refine the exceptions. Throwing FileNotInCacheException
     * implies that one could create the entry, however this is not
     * the case for broken or incomplet files.
     *
     * @param id the PNFS ID of the entry to open
     * @return IO descriptor
     * @throws FileNotInCacheException if file not found or in a state
     * in which it cannot be opened
     */
    ReadHandle openEntry(PnfsId id)
        throws FileNotInCacheException;

    /**
     * Returns information about an entry. Equivalent to calling
     * <code>getEntry</code> on a read handle, but avoid the cost of
     * creating a read handle.
     *
     * @param id the PNFS ID of the entry to open
     * @throws FileNotInCacheException if file not found or in a state
     * in which it cannot be opened
     */
    CacheEntry getEntry(PnfsId id)
        throws FileNotInCacheException;

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
     * @throws FileNotInCacheException when an entry with the given id
     * is not found in the repository
     * @throws IllegalArgumentException when <code>id</code> or
     * <code>owner</code> are null or when <code>lifetime</code> is
     * smaller than -1.
     */
    void setSticky(PnfsId id, String owner, long expire, boolean overwrite)
        throws IllegalArgumentException,
               FileNotInCacheException;

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
     */
    EntryState getState(PnfsId id);

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
     */
    void setState(PnfsId id, EntryState state)
        throws IllegalTransitionException, IllegalArgumentException;

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