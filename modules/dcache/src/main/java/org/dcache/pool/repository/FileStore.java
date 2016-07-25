package org.dcache.pool.repository;

import java.io.IOException;
import java.nio.file.attribute.BasicFileAttributeView;
import java.net.URI;
import java.util.Set;

import diskCacheV111.util.PnfsId;
import org.dcache.pool.movers.IoMode;

/**
 * The FileStore interface provides an abstraction of the file layout
 * of the pool data directory.
 */
public interface FileStore
{
    /**
     * Returns the URI to the data file for the given PNFS id.
     * @return uri to data file.
     */
    URI get(PnfsId id);

    /**
     * Returns true, if back-end store contains data file for a given PNFS id.
     */
    boolean contains(PnfsId id);

    /**
     *
     * @param id
     * @return
     */
    public BasicFileAttributeView getFileAttributeView(PnfsId id) throws IOException;

    /**
     * Create a data file for the given PNFS id.
     * @return file store specific URI to the data file.
     */
    URI create(PnfsId id) throws IOException;

    /**
     * Remove the data file for a given PNFS id.
     */
    void remove(PnfsId id) throws IOException;

    /**
     * Get {@link RepositoryChannel} to a data file for a given PNFS id.
     * The caller is responsible to close the channel when not used.
     */
    RepositoryChannel openDataChannel(PnfsId id, IoMode ioMode) throws IOException;

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
