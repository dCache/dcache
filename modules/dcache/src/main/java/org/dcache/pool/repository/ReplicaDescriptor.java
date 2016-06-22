package org.dcache.pool.repository;

import java.io.File;
import java.io.IOException;

import diskCacheV111.util.CacheException;

import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;


/**
 * Repository replica IO descriptor providing read or write access to an entry.
 *
 * A descriptor must be explicitly closed when access id no longer desired.
 * Opened for the write descriptor have to be committed prior closing it.
 *
 * Two or more read descriptors for the same entry can be open
 * simultaneously. An open read descriptor does not prevent entry state
 * changes.
 *
 * The descriptor provides methods for allocating space for the
 * entry. Space must be allocated before it is consumed on the
 * disk. It is the responsibility of the handle to release any
 * over allocation after the transfer has completed.
 */
public interface ReplicaDescriptor extends Allocator
{
    /*
     * TODO:
     * for now commit is not called only in case of checksum errors.
     * As checksum semanting will be changed, there will ne no need
     * for an extra commit step prior close().
     */

    /**
     * Commit changes on file.
     *
     * The file must not be modified after the descriptor has been
     * committed.
     *
     * Committing adjusts space reservation to match the actual file
     * size. It may cause the file size in the storage info and in
     * PNFS to be updated. Committing sets the repository entry to its
     * target state.
     *
     * In case of problems, the descriptor is not closed and an exception
     * is thrown.
     *
     * Committing a descriptor multiple times causes an
     * IllegalStateException.
     *
     * @throws IllegalStateException if the descriptor is already
     * committed or closed.
     * @throws FileSizeMismatchException if file size does not match
     * the expected size.
     * @throws CacheException if the repository or PNFS state could
     * not be updated.
     */
    void commit()
        throws IllegalStateException, InterruptedException, FileSizeMismatchException, CacheException;

    /**
     * Closes the descriptor. Once descriptor is closed it can't be used any more.
     *
     * If the descriptor was not committed, closing the descriptor will mark
     * the replica broken or delete it. The action taken depends on
     * the descriptor state and possibly configuration settings.
     *
     * Closing a descriptor multiple times causes an
     * IllegalStateException.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    void close() throws IllegalStateException;

    /**
     * Returns the disk file of this replica.
     *
     * @throws IllegalStateException if the descriptor is closed.
     */
    File getFile() throws IllegalStateException;

    /**
     * Get {@link RepositoryChannel} for this {@code ReplicaDescriptor}.
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
     *
     * These are the expected checksums, not the actual checksums of the replica. If the
     * replica is corrupted, the actual checksums may differ from the expected checksums.
     *
     * This method differs from calling getEntry().getFileAttributes().getChecksums() by
     * doing a name space lookup if checksums are not defined in the file attributes. The
     * result of such a lookup is cached.
     */
    Iterable<Checksum> getChecksums() throws CacheException;

    /**
     * Add checksums of the file.
     *
     * The checksums are not in any way verified. Only valid checksums should be added.
     * The checksums will be stored in the name space on commit or close.
     *
     * @param checksum Checksum of the file
     */
    void addChecksums(Iterable<Checksum> checksum);

    /**
     * Sets the last access time of the replica.
     *
     * Only applicable to writes.
     *
     * @param time
     */
    void setLastAccessTime(long time);

    /**
     * Returns the current size of the replica.
     */
    long getReplicaSize();
}
