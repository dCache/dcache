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

import java.util.Map;
import java.util.UUID;

/**
 * Service provider interface for nearline storage.
 *
 * Files can be flushed to, staged from, and removed from nearline storage.
 *
 * The interface is designed for bulk operations. Whether a nearline storage
 * makes use of that or processes each request individually is an implementation
 * detail.
 *
 * Each request has a unique identifier that can be used to cancel the request.
 *
 * A file flushed to nearline storage is identified by an implementation
 * specific URI. This URI is used to stage or remove the file from nearline
 * storage.
 */
public interface NearlineStorage
{
    /**
     * Flush all files in {@code requests} to nearline storage.
     */
    void flush(Iterable<FlushRequest> requests);

    /**
     * Stage all files in {@code requests} from nearline storage.
     */
    void stage(Iterable<StageRequest> requests);

    /**
     * Delete all files in {@code requests} from nearline storage.
     */
    void remove(Iterable<RemoveRequest> requests);

    /**
     * Cancel any flush, stage or remove request with the given id.
     *
     * The failed method of any cancelled request should be called with a
     * CancellationException. If the request completes before it can be
     * cancelled, then the cancellation should be ignored and the completed
     * or failed method should be called as appropriate.
     *
     * A call to cancel must be non-blocking.
     *
     * @param uuid  id of the request to cancel
     */
    void cancel(UUID uuid);

    /**
     * Applies a new configuration.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    void configure(Map<String, String> properties)
            throws IllegalArgumentException;

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage
     * interface.
     *
     * This method does not wait for actively executing requests to
     * terminate.
     */
    void shutdown();
}

