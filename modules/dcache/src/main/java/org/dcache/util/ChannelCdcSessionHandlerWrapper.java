/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2018 Deutsches Elektronen-Synchrotron
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
package org.dcache.util;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler.Sharable;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOutboundHandler;
import io.netty.channel.ChannelPromise;
import io.netty.util.AttributeKey;

import java.net.SocketAddress;

import dmg.cells.nucleus.CDC;


/**
 * This class wraps some ChannelDuplexHandler and ensures that, if the Netty
 * Channel has a specific key then the value of this key is used as the CDC
 * session.  This class is really a hack to work-around that CDC is a
 * thread-local value that is not propagated when Netty schedules tasks outside
 * of the event loop.  Such scheduled tasks current happen in the pool, as the
 * response to a CloseRequest is delayed until after the mover has closed.
 * There seems to be no easy way to propagate the CDC within Netty scheduled
 * tasks.
 */
@Sharable
public class ChannelCdcSessionHandlerWrapper implements ChannelOutboundHandler, ChannelInboundHandler
{
    public static final AttributeKey<String> SESSION = AttributeKey.newInstance("org.dcache.cells.cdc.session");

    private final ChannelDuplexHandler _inner;

    public ChannelCdcSessionHandlerWrapper(ChannelDuplexHandler inner)
    {
        _inner = inner;
    }

    /**
     * Bind a particular session value to this channel.
     */
    public static void bindSessionToChannel(Channel channel, String session)
    {
        channel.attr(SESSION).set(session);
    }

    /*
     * Decorate ChannelOutboundHandler methods.
     */

    @Override
    public void bind(ChannelHandlerContext ctx, SocketAddress localAddress, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.bind(ctx, localAddress, promise);
        }
    }

    @Override
    public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
            SocketAddress localAddress, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.connect(ctx, remoteAddress, localAddress, promise);
        }
    }

    @Override
    public void disconnect(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.disconnect(ctx, promise);
        }
    }

    @Override
    public void close(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.close(ctx, promise);
        }
    }

    @Override
    public void deregister(ChannelHandlerContext ctx, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.deregister(ctx, promise);
        }
    }

    @Override
    public void read(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.read(ctx);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.write(ctx, msg, promise);
        }
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.flush(ctx);
        }
    }

    /*
     * Decorate ChannelInboundHandler methods.
     */

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelRegistered(ctx);
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelUnregistered(ctx);
        }
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelActive(ctx);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelInactive(ctx);
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelRead(ctx, msg);
        }
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelReadComplete(ctx);
        }
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.userEventTriggered(ctx, evt);
        }
    }

    @Override
    public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.channelWritabilityChanged(ctx);
        }
    }

    @Override
    @SuppressWarnings("deprecation")
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.exceptionCaught(ctx, cause);
        }
    }


    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.handlerAdded(ctx);
        }
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception
    {
        try (AutoCloseable oldCdc = withCdcSession(ctx)) {
            _inner.handlerRemoved(ctx);
        }
    }

    private AutoCloseable withCdcSession(ChannelHandlerContext ctx)
    {
        if (CDC.getSession() == null) {
            String session = ctx.channel().attr(SESSION).get();
            if (session != null) {
                CDC captured = new CDC();
                CDC.setSession(session);
                return captured;
            }
        }
        return () -> {};
    }
}
