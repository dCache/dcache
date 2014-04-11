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

import java.util.UUID;

/**
 * A request to a nearline storage.
 *
 * The request is parametrised with type of the result of the request.
 *
 * A request has a lifetime containing three stages: queued, activated and
 * completed/failed. Activation is signalled by calling activated, while
 * completion/failure is signaled by calling completed/failed.
 */
public interface NearlineRequest<T>
{
    /**
     * Returns an identifier uniquely identifying this request.
     *
     * @return A unique identifier
     */
    UUID getId();

    /**
     * A deadline for the request.
     *
     * If the request does not complete before the deadline, the pool will
     * likely cancel the request. The deadline is not a promise that the
     * request will be cancelled, nor is it a promise that the request will
     * not be cancelled ahead of time.
     *
     * @return Deadline in milliseconds since the epoch
     */
    long getDeadline();

    /**
     * Signals that the request is being activated.
     *
     * An activated request is actively being processed rather than just being
     * queued. Note however than an external HSM may itself queue requests. Such
     * requests are still considered active by dCache.
     *
     * The activation may not be instantaneous as dCache may perform additional
     * name space lookups during activation. For this reason the result is
     * provided asynchronously. A NearlineStorage should not proceed with
     * processing the request until activation has completed.
     *
     * @return An asynchronous reply indicating when to proceed with processing
     *         the request. The activation may fail and a NearlineStorage must
     *         fail the entire request by calling {@code failed} with the exception
     *         returned by the future.
     */
    ListenableFuture<Void> activate();

    /**
     * Signals that the request has failed.
     *
     * Any exception is allowed and will be translated to a CacheException before
     * propagation to dCache. Exceptions are translated as follows:
     *
     * <ul>
     * <li>Any {@link java.util.concurrent.ExecutionException} has its <i>cause</i> unwrapped
     *     and the cause is translated according to these rules.
     * <li>Any {@link InterruptedException} and {@link java.util.concurrent.CancellationException} are
     *     considered as indication that the request has been cancelled.
     * <li>Any {@link diskCacheV111.util.CacheException} is propagated untouched.
     * <li>Any other exception is propagated, but is most likely turned wrapped in a CacheException
     *     by the code that invoked the request.
     * </ul>
     *
     * @param cause Exception indicating the cause of the failure
     */
    void failed(Exception cause);

    /**
     * Signals that the request has completed successfully.
     *
     * @param result The result of the request
     */
    void completed(T result);
}
