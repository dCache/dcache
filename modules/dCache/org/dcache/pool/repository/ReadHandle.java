package org.dcache.pool.repository;

import java.util.concurrent.TimeoutException;
import java.io.File;

public interface ReadHandle
{
    /**
     * Closes the handle. All further attempts to use the handle will
     * throw IllegalStateException.
     *
     * @throws IllegalStateException if handle has been closed.
     */
    void close() throws IllegalStateException;

    /**
     * @return disk file
     * @throws IllegalStateException if handle has been closed.
     */
    File getFile() throws IllegalStateException;

    /**
     *
     * @return cache entry
     * @throws IllegalStateException if handle has been closed.
     */
    CacheEntry getEntry()  throws IllegalStateException;
}