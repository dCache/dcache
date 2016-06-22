package org.dcache.pool.repository;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;

import diskCacheV111.util.PnfsId;

/**
 * The FileStore interface provides an abstraction of the file layout
 * of the pool data directory.
 */
public interface FileStore
{
    /**
     * Returns the path to the data file for the given PNFS id.
     */
    Path get(PnfsId id);

    /**
     * Returns the PNFS-IDs of available data files.
     */
    Set<PnfsId> index() throws IOException;

    /**
     * Provides the amount of free space on the file system containing
     * the data files.
     */
    long getFreeSpace() throws IOException;

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    long getTotalSpace() throws IOException;

    /**
     * Returns whether the store appears healthy. How this is
     * determined is up to the implementation.
     */
    boolean isOk();
}
