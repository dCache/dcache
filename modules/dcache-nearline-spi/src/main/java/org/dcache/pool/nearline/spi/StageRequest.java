/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.pool.nearline.spi;

import com.google.common.util.concurrent.ListenableFuture;

import java.io.File;
import java.net.URI;
import java.util.Set;

import org.dcache.util.Checksum;
import org.dcache.vehicles.FileAttributes;

/**
 * A request to retrieve a file from nearline storage.
 *
 * The result of a stage request are zero or more checksums of the
 * file. Some implementations may be able to extract such a checksum
 * from an external storage system.
 */
public interface StageRequest extends NearlineRequest<Set<Checksum>>
{
    /**
     * A local file system path to which to stage the replica.
     *
     * <p>Consider using {@link StageRequest#getReplicaUri} instead.
     *
     * @return A file system path
     * @throws UnsupportedOperationException if this pool is not backed by th default
     *         file system provider.
     */
    @Deprecated
    File getFile();

    /**
     * A URI to the replica to stage. This identifies the replica in the pool,
     * not the file stored on tape. This is typically a file:// URI unless
     * a file store other than the OS file system is used.
     *
     * @return A URI to the replica.
     * @since 2.17
     */
    URI getReplicaUri();

    /**
     * Attributes of the file to stage. Specifically tape locations of the file
     * to stage can be access as {@code FileAttributes#getStorageInfo().locations()}.
     *
     * @return Attributes of the file
     */
    FileAttributes getFileAttributes();

    /**
     * Triggers space allocation for the file being requested.
     *
     * <p>Before completing a stage request, space must be allocated for the file.
     * A NearlineStorage must take care to not use any of the pool's disk space
     * before space has been allocated to it.
     *
     * <p>Some NearlineStorage implementations may have a dedicated buffer area
     * separate from the pool allocation. Such implementations are able to
     * stage the file first and ask the pool for space afterwards. This allows
     * the implementation to reorder stage requests to optimize tape access
     * patterns while allowing more stage requests to be submitted than the
     * pool has free space (presumably the files would be streamed off the pool
     * as they become available, thus eventually allowing all files to be
     * staged).
     *
     * <p>Space allocation may not be instantaneous as dCache may have to delete
     * other files to free up space. For this reason the result is provided
     * asynchronously. A NearlineStorage should not proceed with processing
     * the request until allocation has completed.
     *
     * @return An asynchronous reply indicating when to proceed with processing
     *         the request. The allocation may fail and a NearlineStorage must
     *         fail the entire request by calling failed with the exception
     *         returned by the future.
     */
    ListenableFuture<Void> allocate();
}
