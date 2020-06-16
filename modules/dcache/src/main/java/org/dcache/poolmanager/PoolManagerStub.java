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
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageSender;

import org.dcache.util.TimeUtils;

/**
 * A facade to a PoolManagerHandler.
 *
 * <p>Defines default parameters and timeout limits and provides a simpler interface than
 * what PoolManagerHandler exposes.
 */
public class PoolManagerStub implements CellMessageSender, CellIdentityAware
{
    private CellEndpoint endpoint;

    private PoolManagerHandler handler;

    private long maxPoolTimeout = Long.MAX_VALUE;

    private TimeUnit maxPoolTimeoutUnit = TimeUnit.MILLISECONDS;

    private long maxPoolManagerTimeout = Long.MAX_VALUE;

    private TimeUnit maxPoolManagerTimeoutUnit = TimeUnit.MILLISECONDS;

    private CellAddressCore address;

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        this.endpoint = endpoint;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
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
     * Returns the communication timeout with Pool Manager in milliseconds.
     */
    public long getPoolManagerTimeoutInMillis() {
        return maxPoolManagerTimeoutUnit.toMillis(maxPoolManagerTimeout);
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
     * Returns the communication timeout with Pool in milliseconds.
     */
    public long getPoolTimeoutInMillis() {
        return maxPoolTimeoutUnit.toMillis(maxPoolTimeout);
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
        msg.setReplyRequired(true);
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
        msg.setReplyRequired(true);
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
        msg.setReplyRequired(true);
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
        msg.setReplyRequired(true);
        return handler.sendAsync(endpoint, msg, maxPoolManagerTimeoutUnit.toMillis(maxPoolManagerTimeout));
    }

    /**
     * Submit a request to start a mover to the named pool.  Any response
     * message is handled explicitly within the cell code.
     *
     * This method exists primarily to support legacy code; new code should
     * consider using the startAsync method instead.
     *
     * @param pool The address of the pool
     * @param msg  The mover creation request
     */
    public void start(CellAddressCore pool, PoolIoFileMessage msg)
    {
        // This method is only used by DCapDoorInterpreterV3, which is
        // expecting a reply.
        msg.setReplyRequired(true);
        CellMessage envelope = new CellMessage(pool);
        envelope.addSourceAddress(address);
        handler.start(endpoint, envelope, msg);
    }

    /**
     * Submit a request to pool manager.  Any response message is handled
     * explicitly within the cell code.
     *
     * This method exists primarily to support legacy code; new code should
     * consider using the sendAsync method instead.
     *
     * @param msg The pool manager request
     */
    public void send(PoolManagerMessage msg)
    {
        // This method is only used by DCapDoorInterpreterV3, which is
        // expecting a reply.
        msg.setReplyRequired(true);
        CellMessage envelope = new CellMessage();
        envelope.addSourceAddress(address);
        handler.send(endpoint, envelope, msg);
    }

    /**
     * Submit a request to pool manager, indicating that the response will be
     * ignored after some timeout.  Any response message is handled explicitly
     * within the cell code.
     *
     * This method exists primarily to support legacy code; new code should
     * consider using the sendAsync method instead.
     *
     * @param msg The pool manager request
     * @param timeout How long before PoolManager should discard this request
     */
    public void send(PoolManagerMessage msg, long timeout)
    {
        // This method is only used by DCapDoorInterpreterV3, which is
        // expecting a reply.
        msg.setReplyRequired(true);
        CellMessage envelope = new CellMessage();
        envelope.addSourceAddress(address);
        envelope.setTtl(timeout);
        handler.send(endpoint, envelope, msg);
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
