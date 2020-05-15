package org.dcache.pool.repository;

import diskCacheV111.util.PnfsId;

/**
 * An allocator supports methods for allocating and freeing space.
 */
public interface Allocator
{
    /**
     * Allocates space and blocks until space becomes available.
     *
     * @param size in bytes
     * @throws InterruptedException if thread was interrupted
     * @throws IllegalStateException if operation is not allowed at this point
     * @throws IllegalArgumentException if <code>size</code> is less
     * than 0.
     * @throws OutOfDiskException if no further capacity is available.
     */
    void allocate(PnfsId id, long size)
        throws IllegalStateException,
               IllegalArgumentException,
               InterruptedException,
               OutOfDiskException;

    /**
     * Frees space previously allocated with one of the allocate
     * methods.
     *
     * @throws IllegalArgumentException if <code>size</code> is
     * negative or larger than the amount of allocated space.
     * @throws IllegalStateException if operation is not allowed at
     * this point
     */
    void free(PnfsId id, long size)
        throws IllegalStateException, IllegalArgumentException;
}
