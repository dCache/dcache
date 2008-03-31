package org.dcache.pool.repository;

import java.util.concurrent.TimeoutException;
import java.io.File;

/**
 * Repository handle providing read access to an entry.
 *
 * The handle must be explicitly closed when read access is no longer
 * desired. While the handle is open, an entry is not destroyed.
 *
 * Two or more read handles for the same entry can be open
 * simultaneously. An open read handle does not prevent entry state
 * changes.
 */
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