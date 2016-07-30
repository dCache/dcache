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

import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.Message;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.PoolManagerMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;

import org.dcache.cells.CellStub;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.*;
import static com.google.common.util.concurrent.Uninterruptibles.getUninterruptibly;
import static dmg.cells.nucleus.CellEndpoint.SendFlag.RETRY_ON_NO_ROUTE_TO_CELL;

/**
 * Client stub for obtaining implementations of PoolManagerHandler.
 *
 * <p>This class allows an implementation of PoolManagerHandler to be obtained from a
 * pool manager asynchronously. Updates to this implementation are fetched
 * transparently and asynchronously until the component is stopped.
 *
 * <p>The class implements PoolManagerHandler and passes along calls to the obtained
 * PoolManagerHandler.
 */
public class PoolManagerHandlerSubscriber
        implements CellLifeCycleAware, PoolManagerHandler
{
    /**
     * How frequently to poll for updates. Usually updates are propagated immediately by
     * downstream as a result of a PoolManagerGetUpdatedHandler request, but in case
     * such a request is lost, it will at most be until the poll period expires before
     * we resubmit the request.
     */
    private static final int POLLING_PERIOD = 60000;

    private CellStub poolManager;

    private boolean isStopped;

    private ListenableFuture<SerializablePoolManagerHandler> current;

    private SettableFuture<Void> startGate = SettableFuture.create();

    /**
     * Sets the cell stub used to query the PoolManagerHandler.
     * @param poolManager
     */
    @Required
    public void setPoolManager(CellStub poolManager)
    {
        this.poolManager = poolManager;
    }

    @PostConstruct
    public synchronized void start()
    {
        current = transformAsync(startGate, ignored -> transform(query(new PoolMgrGetHandler()), PoolMgrGetHandler::getHandler));

        Futures.addCallback(current, new FutureCallback<SerializablePoolManagerHandler>()
                            {
                                @Override
                                public void onSuccess(SerializablePoolManagerHandler handler)
                                {
                                    synchronized (PoolManagerHandlerSubscriber.this) {
                                        current = Futures.immediateFuture(handler);
                                        if (!isStopped) {
                                            ListenableFuture<SerializablePoolManagerHandler> next =
                                                    transform(query(new PoolMgrGetUpdatedHandler(handler.getVersion())), PoolMgrGetHandler::getHandler);
                                            Futures.addCallback(next, this);
                                        }
                                    }
                                }

                                @Override
                                public void onFailure(Throwable t)
                                {
                                    synchronized (PoolManagerHandlerSubscriber.this) {
                                        current = Futures.immediateFailedFuture(t);
                                    }
                                }
                            });
    }

    @Override
    public void afterStart()
    {
        startGate.set(null);
    }

    @PreDestroy
    public synchronized void stop()
    {
        isStopped = true;
        if (current != null) {
            current.cancel(false);
        }
    }

    @GuardedBy("this")
    private ListenableFuture<PoolMgrGetHandler> query(PoolMgrGetHandler request)
    {
        return catchingAsync(poolManager.send(request, POLLING_PERIOD, RETRY_ON_NO_ROUTE_TO_CELL),
                             TimeoutCacheException.class, t -> retryQuery(request));

    }

    @GuardedBy("this")
    private ListenableFuture<PoolMgrGetHandler> retryQuery(PoolMgrGetHandler request)
    {
        if (isStopped) {
            throw new CancellationException("Subscriber was stopped.");
        }
        return query(request);
    }

    /**
     * Returns the currently cached implementation of PoolManagerHandler.
     *
     * <p>This is returned as a {@link ListenableFuture} since during startup no handler
     * may be available. In such case the future is not done and calling {@code get}
     * on it will block.
     *
     * @return A future cached PoolManagerHandler implementation.
     */
    public synchronized ListenableFuture<SerializablePoolManagerHandler> current()
    {
        checkState(current != null);
        return nonCancellationPropagating(current);
    }

    @Override
    public <T extends PoolIoFileMessage> ListenableFuture<T> startAsync(
            CellEndpoint endpoint, CellAddressCore address, T msg, long timeout)
    {
        return withCurrent(handler -> handler.startAsync(endpoint, address, msg, timeout));
    }

    @Override
    public void start(CellEndpoint endpoint, CellMessage envelope, PoolIoFileMessage msg)
    {
        withCurrent(handler -> handler.start(endpoint, envelope, msg), endpoint, envelope, msg);
    }

    @Override
    public <T extends PoolManagerMessage> ListenableFuture<T> sendAsync(CellEndpoint endpoint, T msg, long timeout)
    {
        return withCurrent(handler -> handler.sendAsync(endpoint, msg, timeout));
    }

    @Override
    public void send(CellEndpoint endpoint, CellMessage envelope, PoolManagerMessage msg)
    {
        withCurrent(handler -> handler.send(endpoint, envelope, msg), endpoint, envelope, msg);
    }

    @Override
    public String toString()
    {
        ListenableFuture<SerializablePoolManagerHandler> current = this.current;
        if (current.isDone()) {
            try {
                return getUninterruptibly(current).toString();
            } catch (ExecutionException e) {
                return e.getCause().toString();
            }
        }
        return "unavailable";
    }

    private <T extends Message> ListenableFuture<T> withCurrent(AsyncFunction<SerializablePoolManagerHandler, T> f)
    {
        return transformAsync(current(), f);
    }

    private void withCurrent(Consumer<SerializablePoolManagerHandler> f, CellEndpoint endpoint, CellMessage envelope, Message msg)
    {
        Futures.addCallback(current,
                            new FutureCallback<SerializablePoolManagerHandler>()
                            {
                                @Override
                                public void onSuccess(@Nullable SerializablePoolManagerHandler handler)
                                {
                                    f.accept(handler);
                                }

                                @Override
                                public void onFailure(Throwable t)
                                {
                                    msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION, t);
                                    envelope.setMessageObject(msg);
                                    envelope.revertDirection();
                                    endpoint.sendMessage(envelope);
                                }
                            });
    }
}