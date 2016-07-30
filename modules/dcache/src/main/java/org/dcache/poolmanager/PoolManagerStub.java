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

import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageSender;

import org.dcache.util.TimeUtils;

/**
 * A facade to a PoolManagerHandler.
 *
 * <p>Defines default parameters and timeout limits and provides a simpler interface than
 * what PoolManagerHandler exposes.
 */
public class PoolManagerStub implements CellMessageSender
{
    private CellEndpoint endpoint;

    private PoolManagerHandler handler;

    private long maxPoolTimeout = Long.MAX_VALUE;

    private TimeUnit maxPoolTimeoutUnit = TimeUnit.MILLISECONDS;

    private long maxPoolManagerTimeout = Long.MAX_VALUE;

    private TimeUnit maxPoolManagerTimeoutUnit = TimeUnit.MILLISECONDS;

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }

    public void setHandler(PoolManagerHandler handler)
    {
        this.handler = handler;
    }

    /**
     * Sets a maximum timeout for pool manager requests.
     *
     * <p>If {@link PoolManagerStub#sendAsync(PoolManagerMessage, long)} is called with a
     * larger value, the timeout is limited to the maximum.
     *
     * @param timeout
     */
    public void setMaximumPoolManagerTimeout(long timeout)
    {
        maxPoolManagerTimeout = timeout;
    }

    public void setMaximumPoolManagerTimeoutUnit(TimeUnit unit)
    {
        maxPoolManagerTimeoutUnit = unit;
    }

    /**
     * Sets a maximum timeout for pool requests.
     *
     * <p>If {@link PoolManagerStub#startAsync(CellAddressCore, PoolIoFileMessage, long)} is
     * called with a larger value, the timeout is limited to the maximum.
     *
     * @param timeout
     */
    public void setMaximumPoolTimeout(long timeout)
    {
        maxPoolTimeout = timeout;
    }

    public void setMaximumPoolTimeoutUnit(TimeUnit unit)
    {
        maxPoolTimeoutUnit = unit;
    }

    /**
     * Submit a request to a pool.
     *
     * @param pool The address of the pool
     * @param msg  The mover creation request
     * @param timeout timeout in milliseconds
     * @return An asynchronous reply
     */
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(CellAddressCore pool, T msg, long timeout)
    {
        long boundedTimeout = Math.min(timeout, maxPoolTimeoutUnit.toMillis(maxPoolTimeout));
        return handler.startAsync(endpoint, pool, msg, boundedTimeout);
    }

    /**
     * Submit a request to a pool.
     *
     * @param pool The address of the pool
     * @param msg  The mover creation request
     * @return An asynchronous reply
     */
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(CellAddressCore pool, T msg)
    {
        return handler.startAsync(endpoint, pool, msg, maxPoolTimeoutUnit.toMillis(maxPoolTimeout));
    }

    /**
     * Submit a request to pool manager.
     *
     * @param msg The pool manager request
     * @param timeout timeout in milliseconds
     * @return An asynchronous reply
     */
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(T msg, long timeout)
    {
        long boundedTimeout = Math.min(timeout, maxPoolManagerTimeoutUnit.toMillis(maxPoolManagerTimeout));
        return handler.sendAsync(endpoint, msg, boundedTimeout);
    }

    /**
     * Submit a request to pool manager.
     *
     * @param msg The pool manager request
     * @return An asynchronous reply
     */
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(T msg)
    {
        return handler.sendAsync(endpoint, msg, maxPoolManagerTimeoutUnit.toMillis(maxPoolManagerTimeout));
    }

    /**
     * Submit a request to start a mover to the named pool.
     *
     * @param pool The address of the pool
     * @param msg  The mover creation request
     * @return An asynchronous reply
     */
    public void start(CellAddressCore pool, PoolIoFileMessage msg)
    {
        handler.start(endpoint, new CellMessage(pool), msg);
    }

    /**
     * Submit a request to pool manager.
     *
     * @param msg The pool manager request
     * @return An asynchronous reply
     */
    public void send(PoolManagerMessage msg)
    {
        handler.send(endpoint, new CellMessage(), msg);
    }

    @Override
    public String toString()
    {
        CharSequence poolManagrTimeout =
                TimeUtils.duration(maxPoolManagerTimeout, maxPoolManagerTimeoutUnit, TimeUtils.TimeUnitFormat.SHORT);
        CharSequence poolTimeout =
                TimeUtils.duration(maxPoolTimeout, maxPoolTimeoutUnit, TimeUtils.TimeUnitFormat.SHORT);
        return "handler=" + handler + ", " + "pool manager timeout=" + poolManagrTimeout + ", " + "pool timeout=" + poolTimeout;
    }
}
