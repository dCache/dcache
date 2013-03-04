package org.dcache.pool.repository;

import java.io.File;

import diskCacheV111.util.CacheException;

import org.dcache.util.Checksum;



/**
 * Repository replica IO descriptor providing read or write access to an entry.
 *
 * A descriptor must be explicitly closed when access id no longer desired.
 * Opened for the write descriptor have to be committed prior closing it.
 *
 * Two or more read descriptors for the same entry can be open
 * simultaneously. An open read descriptor does not prevent entry state
 * changes.
 *
 * The descriptor provides methods for allocating space for the
 * entry. Space must be allocated before it is consumed on the
 * disk. It is the responsibility of the handle to release any
 * over allocation after the transfer has completed.
 */
public interface ReplicaDescriptor extends Allocator
{
    /*
     * TODO:
     * for now commit is not called only in case of checksum errors.
     * As checksum semanting will be changed, there will ne no need
     * for an extra commit step prior close().
     */

    /**
     * Commit changes on file.
     *
     * The file must not be modified after the descriptor has been
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
     * In case of problems, the descriptor is not closed and an exception
     * is thrown.
     *
     * Committing a descriptor multiple times causes an
     * IllegalStateException.
     *
     * @param checksum Checksum of the replica. May be null.
     * @throws IllegalStateException if the descriptor is already
     * committed or closed.
     * @throws FileSizeMismatchException if file size does not match
     * the expected size.
     * @throws CacheException if the repository or PNFS state could
     * not be updated.
     */
    void commit(Checksum checksum)
        throws IllegalStateException, InterruptedException, FileSizeMismatchException, CacheException;

    /**
     * Closes the descriptor. Once descriptor is closed it can't be used any more.
     *
     * If the descriptor was not committed, closing the descriptor will mark
     * the replica broken or delete it. The action taken depends on
     * the descriptor state and possibly configuration settings.
     *
     * Closing a descriptor multiple times causes an
     * IllegalStateException.
     *
     * @throws IllegalStateException if the descriptor is closed.
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
