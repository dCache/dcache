package org.dcache.pool.repository;

import java.io.File;
import java.util.List;

import diskCacheV111.util.PnfsId;

/**
 * The DataFileRepository interface provides an abstraction of the
 * file layout of the pool data directory.
 */
public interface DataFileRepository
{
    /**
     * Returns the path to the data file for the given PNFS id.
     */
    public File get(PnfsId id);

    /**
     * Returns a list of PNFS ids of available data files.
     */
    public List<PnfsId> list();

    /**
     * Provides the amount of free space on the file system containing
     * the data files. This will only be implemented for Java 6.
     */
    public long getFreeSpace();

    /**
     * Provides the total amount of space on the file system
     * containing the data files. This will only be implemented for
     * Java 6.
     */
    public long getTotalSpace();

    /**
     * Returns whether the repository appears healthy. How this is
     * determined is up to the implementation.
     */
    public boolean isOk();
}
