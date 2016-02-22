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
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.LastHttpContent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * Add the HTTP KeepAlive related response header when appropriate and
 * ensure the connection is terminated once advertised.
 * <p>
 * See Section 8 of RFC-2616 for more details.
 */
public class KeepAliveHandler extends ChannelDuplexHandler
{
    private static final Logger LOG = LoggerFactory.getLogger(KeepAliveHandler.class);

    private boolean _hasPreviousRequest;
    private boolean _isLastRequestKeepAlive;
    private final Deque<Boolean> _inflightKeepAlive = new ArrayDeque();

    @Override
    public void channelRead(ChannelHandlerContext context, Object message) throws Exception
    {
        if (message instanceof HttpRequest) {
            if (_hasPreviousRequest && !_isLastRequestKeepAlive) {
                /*
                 * The client has attempted to send a further request after
                 * the previous request signaled the connection should be closed.
                 *
                 * For HTTP/1.1, this is plain broken: RFC 2616 (8.1.2)
                 *     "Once a close has been signaled, the client MUST NOT
                 *     send any more requests on that connection."
                 *
                 * For HTTP/1.0, this is undefined.
                 *
                 * For HTTP/1.1, dCache is required not to generate any further
                 * responses: RFC 2616 (8.1.2.1)
                 *     "If either the client or the server sends the close
                 *     token in the Connection header, that request becomes
                 *     the last one for the connection."
                 *
                 * Therefore, the request is simply dropped.  As soon as the
                 * server side of the TCP connection is closed, the OS will
                 * reply to any further traffic with a RST, tearing down the
                 * client side of the TCP connection.
                 */
                LOG.debug("Broken client sent request after previously asking " +
                        "the connection be closed.");
                return;
            }

            _isLastRequestKeepAlive = HttpHeaders.isKeepAlive((HttpRequest) message);
            _inflightKeepAlive.offerLast(_isLastRequestKeepAlive);
            _hasPreviousRequest = true;
        }

        super.channelRead(context, message);
    }

    @Override
    public void write(ChannelHandlerContext context, Object message, ChannelPromise promise)
            throws Exception
    {
        boolean is100Continue = false;

        if (message instanceof HttpResponse) {
            HttpResponse response = (HttpResponse) message;
            is100Continue = response.getStatus().equals(HttpResponseStatus.CONTINUE);

            boolean keepAlive = _inflightKeepAlive.getFirst();

            /*
             * An upstream handler can request the connection be closed even if
             * the client make no such request.
             */
            if (keepAlive && !HttpHeaders.isKeepAlive(response)) {
                _inflightKeepAlive.removeFirst();
                _inflightKeepAlive.addFirst(Boolean.FALSE);
                keepAlive = false;
            }

            /* REVISIT: It is not clear from RFC-2616 whether, when the client
             * issues a request with both the "Expect: 100-continue" and the
             * "Connection: close" headers, the initial CONTINUE response and
             * the final response should both include the server's
             * "Connection: close" header, or just the initial CONTINUE, or
             * just the final response.
             *
             * We choose (somewhat arbitrarily) not to send the
             * "Connection: close" header with CONTINUE responses.
             */
            if (!is100Continue) {
                HttpHeaders.setKeepAlive(response, keepAlive);
            }
        }

        ChannelFuture writePromise = context.write(message, promise);

        /*
         * Netty (currently) requires that the message(s) for the 100-continue
         * partial response contain a LastHttpContent message; therefore, an
         * HTTP PUT request with the "Expect: 100-continue" header will
         * generate two LastHttpContent messages.
         */
        if (message instanceof LastHttpContent && !is100Continue) {
            boolean keepAlive = _inflightKeepAlive.remove();

            if (!keepAlive) {
                writePromise.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
