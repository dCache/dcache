package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PnfsId;
import java.io.Closeable;
import java.nio.file.OpenOption;
import java.util.Set;

/**
 * The ReplicaStore interface provides an abstraction of how ReplicaRecord objects are created,
 * retrieved and removed.
 * <p>
 * The name is misleading and should be renamed. The reason is that the interface is used as an
 * abstraction over both meta data storage and file storage.
 */
public interface ReplicaStore extends Closeable {

    enum IndexOption {
        META_ONLY, ALLOW_REPAIR
    }

    void init() throws CacheException;

    /**
     * Returns a collection of PNFS ids of available entries.
     * <p>
     * If called with ALLOW_REPAIR, no concurrent access should be made on any methods of this
     * interface.
     */
    Set<PnfsId> index(IndexOption... options) throws CacheException;

    /**
     * Retrieves an existing entry previously created with
     * <i>create</i>.
     *
     * @param id PNFS id for which to retrieve the entry.
     * @return The entry or null if the entry does not exist.
     * @throws CacheException if looking up the entry failed.
     */
    ReplicaRecord get(PnfsId id)
          throws CacheException;

    /**
     * Creates a new entry. The entry must not exist prior to this call.
     *
     * @param id    PNFS id for which to create the entry
     * @param flags options that influence how the entry is created
     * @return The new entry
     * @throws DuplicateEntryException if entry already exists
     * @throws CacheException          if entry creation fails
     */
    ReplicaRecord create(PnfsId id, Set<? extends OpenOption> flags)
          throws DuplicateEntryException, CacheException;

    /**
     * Removes a meta data entry. If the entry does not exist, nothing happens.
     *
     * @param id PNFS id of the entry to return.
     */
    void remove(PnfsId id)
          throws CacheException;

    /**
     * Returns whether the store appears healthy. How this is determined is up to the
     * implementation.
     */
    boolean isOk();

    /**
     * Closes the store and frees any associated resources.
     */
    void close();

    /**
     * Provides the amount of free space on the file system containing the data files.
     */
    long getFreeSpace();

    /**
     * Provides the total amount of space on the file system containing the data files.
     */
    long getTotalSpace();

    /**
     * Implementations are expected to provide a meaningful name.  Decorator classes may return just
     * the decorated class' toString output, otherwise the suggested format is {@literal XXX[YYY]}
     * where {@literal XXX} is an identifier for the decorator and {@literal YYY} is the decorated
     * class' toString response.
     */
    @Override
    String toString();
}
