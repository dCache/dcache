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
package org.dcache.http;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adjust response headers based on whether or not the connection is to
 * be closed.  Note, this class name is based on the miss-named "HTTP Keep-Alive"
 * concept and is unrelated to TCP Keep-Alive.
 */
public class KeepAliveHandler extends ChannelDuplexHandler
{
    private boolean _isKeepAlive;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof HttpRequest) {
            _isKeepAlive = HttpHeaders.isKeepAlive((HttpRequest) msg);
        }

        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
            throws Exception
    {
        if (message instanceof HttpResponse) {
            HttpHeaders.setKeepAlive((HttpResponse) message, _isKeepAlive);
        }

        if (message instanceof LastHttpContent && !_isKeepAlive && promise != null) {
            promise.addListener(ChannelFutureListener.CLOSE);
        }

        super.write(context, message, promise);
    }
}
