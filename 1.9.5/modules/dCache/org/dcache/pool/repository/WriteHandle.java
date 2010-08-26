package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.Checksum;
import java.util.concurrent.TimeoutException;
import java.io.File;



/**
 * Repository handle providing write access to an entry.
 *
 * The only way to create a new entry is through a write handle.  A
 * handle must be explicitly committed and closed after the write has
 * completed. Failure to create the file is signaled by closing the
 * handle without committing it first.
 *
 * The write handle provides methods for allocating space for the
 * entry. Space must be allocated before it is consumed on the
 * disk. It is the responsibility of the write handle to release any
 * over allocation after the transfer has completed.
 */
public interface WriteHandle extends Allocator
{
    /**
     * Signal successful creation of the replica.
     *
     * The file must not be modified after the handle has been
     * committed.
     *
     * Committing adjusts space reservation to match the actual file
     * size. It may cause the file size in the storage info and in
     * PNFS to be updated. Committing sets the repository entry to its
     * target state.
     *
     * The checksum provided is compared to a known checksum, if
     * possible. If a mismatch is detected CacheException is
     * thrown. If no checksum was known, the checksum is stored in the
     * storage info and in PNFS.
     *
     * In case of problems, the handle is not closed and an exception
     * is thrown.
     *
     * Committing a handle multiple times causes an
     * IllegalStateException.
     *
     * @param checksum Checksum of the replica. May be null.
     * @throws IllegalStateException if the handle is already
     * committed or closed.
     * @throws FileSizeMismatchException if file size does not match
     * the expected size.
     * @throws CacheException if the repository or PNFS state could
     * not be updated.
     */
    void commit(Checksum checksum)
        throws IllegalStateException, InterruptedException, CacheException;

    /**
     * Closes the write handle. The file must not be modified after
     * the handle has been closed and the handle itself must be
     * discarded.
     *
     * If the handle was not committed, closing the handle will mark
     * the replica broken or delete it. The action taken depends on
     * the handle state and possibly configuration settings.
     *
     * Closing a handle multiple times causes an
     * IllegalStateException.
     *
     * @throws IllegalStateException if the handle is closed.
     */
    void close() throws IllegalStateException;

    /**
     * @return disk file
     * @throws IllegalStateException if EntryIODescriptor is closed.
     */
    File getFile() throws IllegalStateException;

    /**
     *
     * @return cache entry
     * @throws IllegalStateException
     */
    CacheEntry getEntry()  throws IllegalStateException;
}