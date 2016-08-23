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
package org.dcache.xrootd.plugins;

import com.google.common.net.HostAndPort;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import org.slf4j.Logger;

import java.net.InetSocketAddress;
import java.util.Objects;

import dmg.cells.nucleus.CDC;

import org.dcache.util.NetLoggerBuilder;

import static org.dcache.util.NetLoggerBuilder.Level.INFO;

/**
 * Access log handler used when proxy protocol support is enabled.
 *
 * We delay the connection start event until the proxy message is received. Once received,
 * we replace this handler with a regular access logger. If a proxy message is not received
 * or indicates that the connection is a health check, we keep this handler and suppress the
 * connection event.
 */
@ChannelHandler.Sharable
public class ProxyAccessLogHandler extends AccessLogHandler
{
    private final AccessLogHandler handler;

    public ProxyAccessLogHandler(Logger logger, AccessLogHandler handler)
    {
        super(logger);
        this.handler = handler;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage proxyMessage = (HAProxyMessage) msg;
            if (proxyMessage.command() == HAProxyCommand.PROXY) {
                InetSocketAddress remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
                InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
                String sourceAddress = proxyMessage.sourceAddress();
                String destinationAddress = proxyMessage.destinationAddress();

                if (proxyMessage.proxiedProtocol() == HAProxyProxiedProtocol.UNKNOWN) {
                    NetLoggerBuilder log = new NetLoggerBuilder(INFO,
                                                                "org.dcache.xrootd.connection.start").omitNullValues();
                    log.add("session", CDC.getSession());
                    log.add("socket.remote", remoteAddress);
                    log.add("socket.local", localAddress);
                    log.toLogger(logger);
                    ctx.channel().pipeline().replace(this, null, handler);
                } else if (!Objects.equals(destinationAddress, localAddress.getAddress().getHostAddress())) {
                    /* The above check is a workaround for what looks like a bug in HAProxy - health checks
                     * should generate a LOCAL command, but it appears they do actually use PROXY.
                     */
                    NetLoggerBuilder log = new NetLoggerBuilder(INFO,
                                                                "org.dcache.xrootd.connection.start").omitNullValues();
                    log.add("session", CDC.getSession());
                    log.add("socket.remote",
                            HostAndPort.fromParts(sourceAddress, proxyMessage.sourcePort()));
                    log.add("socket.proxy",
                            HostAndPort.fromParts(destinationAddress, proxyMessage.destinationPort()));
                    log.add("socket.local", localAddress);
                    log.toLogger(logger);
                    ctx.channel().pipeline().replace(this, null, handler);
                }
            }
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        ctx.fireChannelActive();
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception
    {
        ctx.fireChannelInactive();
    }
}
