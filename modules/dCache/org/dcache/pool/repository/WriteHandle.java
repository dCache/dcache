package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import java.util.concurrent.TimeoutException;
import java.io.File;

/**
 * Repository handle providing write access to an entry.
 *
 * The only way to create a new entry is through a write handle.  A
 * handle must be explicitly closed after the write has
 * completed. Failure to create the file can be signaled through the
 * <code>cancel</code> method.
 *
 * The write handle provides methods for allocating space for the
 * entry. Space must be allocated before it is consumed on the
 * disk. It is the reponsibility of the write handle to release any
 * over allocation after the transfer has completed.
 */
public interface WriteHandle
{
    /**
     * Allocates space and blocks until space becomes available.
     *
     * @param size in bytes
     * @throws InterruptedException
     * @throws IllegalStateException if handler has been closed
     * @throws IllegalArgumentException
     *             if <i>size</i> < 0
     */
    void allocate(long size)
        throws IllegalStateException, IllegalArgumentException, InterruptedException;

    /**
     * Allocates space and blocks specified time until space becomes
     * available.
     *
     * @param size in bytes
     * @param time to block in milliseconds
     * @throws InterruptedException
     * @throws IllegalStateException if handler has been closed
     * @throws TimeoutException if request timed out
     * @throws IllegalArgumentException if either<code>size</code> or
     *             <code>time</code> is negative.
     */
    void allocate(long size, long time)
        throws IllegalStateException, IllegalArgumentException,
               InterruptedException, TimeoutException;

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
    void cancel(boolean keep) throws IllegalStateException;

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
    void close()
        throws IllegalStateException, InterruptedException, CacheException;


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