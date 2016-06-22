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

import org.dcache.vehicles.FileAttributes;

/**
 * A request to flush a file to nearline storage.
 *
 * The result of a flush request is one or more URIs that identify
 * the flushed file.
 */
public interface FlushRequest extends NearlineRequest<Set<URI>>
{
    /**
     * A local file system path to the replica to flush.
     *
     * <p>Consider using {@link FlushRequest#getReplicaUri} instead.
     *
     * @return A file system path
     * @throws UnsupportedOperationException if this pool is not backed by th default
     *         file system provider.
     */
    @Deprecated
    File getFile();

    /**
     * A URI to the replica to flush. This identifies the replica in the pool,
     * not the file stored on tape. This is typically a file:// URI unless
     * a file store other than the OS file system is used.
     *
     * @return A URI
     */
    URI getReplicaUri();

    /**
     * Attributes of the file to which this request applies.
     *
     * @return Attributes of the file
     */
    FileAttributes getFileAttributes();

    /**
     * Signals that the request is being activated and returns the path of the file.
     *
     * <p>Similar to <code>activate</code>, but in addition to marking the request as
     * active, this method resolves the path of the file. Resolving the path of a
     * file is relatively expensive, which is why <code>activate</code> doesn't do
     * it. If a file has several hard-links, only one of the paths is returned.
     *
     * @return An asynchronous reply indicating when to proceed with processing
     *         the request. The activation may fail and a NearlineStorage must
     *         fail the entire request by calling {@code failed} with the exception
     *         returned by the future. The result carries the path of the file.
     */
    ListenableFuture<String> activateWithPath();
}
