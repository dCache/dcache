package org.dcache.pool.repository;

import java.io.File;
import java.util.List;

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
    File get(PnfsId id);

    /**
     * Returns a list of PNFS ids of available data files.
     */
    List<PnfsId> list();

    /**
     * Provides the amount of free space on the file system containing
     * the data files.
     */
    long getFreeSpace();

    /**
     * Provides the total amount of space on the file system
     * containing the data files.
     */
    long getTotalSpace();

    /**
     * Returns whether the store appears healthy. How this is
     * determined is up to the implementation.
     */
    boolean isOk();
}
