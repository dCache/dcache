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
 * <p>
 * Files can be flushed to, staged from, and removed from nearline storage.
 * <p>
 * The interface is designed for bulk operations. Whether a nearline storage makes use of that or
 * processes each request individually is an implementation detail.
 * <p>
 * Each request has a unique identifier that can be used to cancel the request.
 * <p>
 * A file flushed to nearline storage is identified by an implementation specific URI. This URI is
 * used to stage or remove the file from nearline storage.
 * <p>
 * Object life-cycle: under normal operation, the {@link #configure} method is called first,
 * followed by the {@link #start} method.  After this any of the methods other than
 * {@literal start} may be called.  It is guaranteed that {@link shutdown} is called.  After
 * {@literal shutdown} returns, no further methods are called and the object will then be garbage
 * collected at some point.
 * <p>
 * A configuration-testing life-cycle is used to verify the NearlineStorage configuration is
 * correct without affecting the "live" system.  Under this mode, an object is created and the
 * {@link #configure} method is called once and will then be garbage collected.  No other methods
 * are called.
 */
public interface NearlineStorage {

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
     * <p>
     * The failed method of any cancelled request should be called with a CancellationException. If
     * the request completes before it can be cancelled, then the cancellation should be ignored and
     * the completed or failed method should be called as appropriate.
     * <p>
     * A call to cancel must be non-blocking.
     *
     * @param uuid id of the request to cancel
     */
    void cancel(UUID uuid);

    /**
     * Applies a new configuration.  This method is called once before {@link #start}, but may be
     * called subsequently.
     *
     * @throws IllegalArgumentException if the configuration is invalid
     */
    void configure(Map<String, String> properties)
          throws IllegalArgumentException;

    /**
     * Inform the NearlineStorageProvider to start any background activity or open external
     * resources.  This method is only called once.  If called, it is guaranteed that
     * {@link #shutdown} is called.
     */
    default void start(){}

    /**
     * Cancels all requests and initiates a shutdown of the nearline storage interface.
     * <p>
     * This method does not wait for actively executing requests to terminate.
     * <p>
     * This method should also halt any background activity and close any external resources,
     * typically established via the {@link #start} method.
     */
    void shutdown();
}

