package org.dcache.pool.repository;

import diskCacheV111.util.CacheException;
import java.io.IOException;
import java.net.URI;
import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;


/**
 * Repository replica IO descriptor providing read or write access to an entry.
 * <p>
 * A descriptor must be explicitly closed when access id no longer desired. Opened for the write
 * descriptor have to be committed prior closing it.
 * <p>
 * Two or more read descriptors for the same entry can be open simultaneously. An open read
 * descriptor does not prevent entry state changes.
 */
public interface ReplicaDescriptor extends AutoCloseable {
    /*
     * TODO:
     * for now commit is not called only in case of checksum errors.
     * As checksum semanting will be changed, there will ne no need
     * for an extra commit step prior close().
     */

    /**
     * Closes the descriptor. Once descriptor is closed it can't be used any more.
     * <p>
     * If the descriptor was not committed, closing the descriptor will mark the replica broken or
     * delete it. The action taken depends on the descriptor state and possibly configuration
     * settings.
     * <p>
     * Closing a descriptor multiple times causes an IllegalStateException.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    void close() throws IllegalStateException;

    /**
     * Returns the disk file of this replica.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    URI getReplicaFile() throws IllegalStateException;

    /**
     * Get {@link RepositoryChannel} for this {@code ReplicaDescriptor}.
     *
     * @return repository channel.
     * @throws IOException if repository channel can't be created.
     */
    RepositoryChannel createChannel() throws IOException;

    /**
     * Returns the file attributes of the file represented by this replica.
     */
    FileAttributes getFileAttributes() throws IllegalStateException;

    /**
     * Returns known checksums of the file.
     * <p>
     * These are the expected checksums, not the actual checksums of the replica. If the replica is
     * corrupted, the actual checksums may differ from the expected checksums.
     * <p>
     * This method differs from calling getEntry().getFileAttributes().getChecksums() by doing a
     * name space lookup if checksums are not defined in the file attributes. The result of such a
     * lookup is cached.
     */
    Iterable<Checksum> getChecksums() throws CacheException;

    /**
     * Returns the current size of the replica.
     */
    long getReplicaSize();

    /**
     * Returns replica creation time in milliseconds.
     *
     * @return replica creation time.
     */
    long getReplicaCreationTime();
}
