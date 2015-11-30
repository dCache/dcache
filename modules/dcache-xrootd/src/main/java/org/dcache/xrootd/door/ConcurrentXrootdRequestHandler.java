/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2015 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd.door;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.channel.ChannelPromiseNotifier;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import dmg.cells.nucleus.CDC;

import org.dcache.util.CDCListeningExecutorServiceDecorator;
import org.dcache.xrootd.AbstractXrootdRequestHandler;
import org.dcache.xrootd.protocol.messages.XrootdRequest;

/**
 * Subclass of AbstractXrootdRequestHandler that dispatches calls to doOnXXX methods
 * from a supplied executor.
 *
 * Ensures that the CDC survives the thread boundaries.
 */
public class ConcurrentXrootdRequestHandler extends AbstractXrootdRequestHandler
{
    protected final ListeningExecutorService _executor;

    /**
     * The set of requests which are currently processed for this channel. They
     * will be interrupted in case the channel is disconnected.
     */
    private final Set<Future<?>> _requests = Collections.synchronizedSet(new HashSet<>());

    public ConcurrentXrootdRequestHandler(ExecutorService executor)
    {
        _executor = new CDCListeningExecutorServiceDecorator(executor);
    }

    @Override
    protected ChannelFuture respond(ChannelHandlerContext ctx, Object response)
    {
        CDC cdc = new CDC();
        ChannelPromise promise = ctx.newPromise();
        ctx.executor().execute(() -> {
            try (CDC ignored = cdc.restore()) {
                ctx.writeAndFlush(response)
                        .addListener(future -> {
                                if (!future.isSuccess()) {
                                    exceptionCaught(ctx, future.cause());
                                }
                            }
                        )
                        .addListener(new ChannelPromiseNotifier(promise));
            }
        });
        return promise;
    }

    @Override
    protected void requestReceived(ChannelHandlerContext ctx, XrootdRequest req)
    {
        ListenableFuture<?> future = _executor.submit(() -> super.requestReceived(ctx, req));
        _requests.add(future);
        future.addListener(() -> _requests.remove(future), _executor);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx)
            throws Exception
    {
        synchronized (_requests) {
            for (Future<?> request : _requests) {
                request.cancel(true);
            }
        }
    }
}
