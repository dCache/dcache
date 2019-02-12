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

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;

import org.dcache.util.ChannelCdcSessionHandlerWrapper;

public class ProxyAccessLogHandlerFactory extends AccessLogHandlerFactory
{
    private final ChannelHandler proxyAccessLogHandler = new ChannelCdcSessionHandlerWrapper(
                new ProxyAccessLogHandler(accessLogger, handler) {
                    @Override
                    protected void replaceWith(ChannelHandlerContext ctx, ChannelHandler handler)
                    {
                        ctx.pipeline().replace(ProxyAccessLogHandlerFactory.this.proxyAccessLogHandler,
                                null, handler);
                    }
                }
            );

    @Override
    public ChannelHandler createHandler()
    {
        return proxyAccessLogHandler;
    }
}
