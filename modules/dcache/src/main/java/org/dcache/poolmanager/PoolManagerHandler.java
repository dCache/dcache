/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
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

package org.dcache.poolmanager;

import com.google.common.util.concurrent.ListenableFuture;

import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;

/**
 * Client stub for communicating with the pool manager.
 *
 * <p>Implementations allow requests to be send to the pool manager and to start
 * movers on pools.
 *
 * <p>The simplest implementation would simply forward these requests to the pool
 * manager, however more advanced implementations may process some of the requests
 * locally or load balance requests over multiple pool managers.
 *
 * <p>Implementations are typically obtained from the pool manager through a
 * {@link PoolManagerHandlerSubscriber}.
 */
public interface PoolManagerHandler
{
    /**
     * Submit a request to a pool.
     *
     * Some pool requests are intercepted by pool manager and thus should be
     * submitted through a PoolManagerHandler.
     *
     * @param endpoint endpoint through which to send messages
     * @param pool The address of the pool
     * @param msg  The mover creation request
     * @param timeout timeout in milliseconds
     * @return An asynchronous reply
     */
    <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(CellEndpoint endpoint, CellAddressCore pool, T msg, long timeout);

    /**
     * Submit a request to a pool.
     *
     * Some pool requests are intercepted by pool manager and thus should be
     * submitted through a PoolManagerHandler.
     *
     * In contrast to {@link PoolManagerHandler#startAsync(CellEndpoint, CellAddressCore, PoolIoFileMessage, long)},
     * this method sends the reply by returning the given envelope (i.e. the reply is sent
     * to the source path of the envelope). This avoids the need for registering a callback.
     *
     * @param endpoint endpoint through which to send messages
     * @param envelope The envelope to return with the reply
     * @param msg The mover creation request
     * @return An asynchronous reply
     */
    void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg);

    /**
     * Submit a request to pool manager.
     *
     * Implementations are free to process such requests locally or proxy the requests
     * through other services if desired.
     *
     * @param endpoint endpoint through which to send messages
     * @param msg The pool manager request
     * @param timeout timeout in milliseconds
     * @return An asynchronous reply
     */
    <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout);

    /**
     * Submit a request to pool manager.
     *
     * Implementations are free to process such requests locally or proxy the requests
     * through other services if desired.
     *
     * In contrast to {@link PoolManagerHandler#sendAsync(CellEndpoint, PoolManagerMessage, long)},
     * this method sends the reply by returning the given envelope (i.e. the reply is sent
     * to the source path of the envelope). This avoids the need for registering a callback.
     *
     * @param endpoint endpoint through which to send messages
     * @param envelope The envelope to return with the reply
     * @param msg The pool manager request
     */
    void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg);
}

