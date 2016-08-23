/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2014 Deutsches Elektronen-Synchrotron
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
package org.dcache.xrootd;

import com.google.common.net.InetAddresses;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.Objects;

import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.LocateRequest;
import org.dcache.xrootd.protocol.messages.LocateResponse;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
import org.dcache.xrootd.protocol.messages.SetRequest;
import org.dcache.xrootd.protocol.messages.SetResponse;
import org.dcache.xrootd.protocol.messages.XrootdResponse;

public class AbstractXrootdRequestHandler extends XrootdRequestHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractXrootdRequestHandler.class);

    private boolean _isHealthCheck;

    private InetSocketAddress _localAddress;

    private InetSocketAddress _remoteAddress;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        _localAddress = (InetSocketAddress) ctx.channel().localAddress();
        _remoteAddress = (InetSocketAddress) ctx.channel().remoteAddress();
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception
    {
        if (msg instanceof HAProxyMessage) {
            HAProxyMessage proxyMessage = (HAProxyMessage) msg;
            switch (proxyMessage.command()) {
            case LOCAL:
                _isHealthCheck = true;
                break;
            case PROXY:
                String sourceAddress = proxyMessage.sourceAddress();
                String destinationAddress = proxyMessage.destinationAddress();
                InetSocketAddress localAddress = (InetSocketAddress) ctx.channel().localAddress();
                if (proxyMessage.proxiedProtocol() == HAProxyProxiedProtocol.TCP4 ||
                    proxyMessage.proxiedProtocol() == HAProxyProxiedProtocol.TCP6) {
                    if (Objects.equals(destinationAddress, localAddress.getAddress().getHostAddress())) {
                        /* Workaround for what looks like a bug in HAProxy - health checks should
                         * generate a LOCAL command, but it appears they do actually use PROXY.
                         */
                        _isHealthCheck = true;
                    } else {
                        _localAddress = new InetSocketAddress(InetAddresses.forString(destinationAddress), proxyMessage.destinationPort());
                        _remoteAddress = new InetSocketAddress(InetAddresses.forString(sourceAddress), proxyMessage.sourcePort());
                    }
                }
                break;
            }
        } else {
            super.channelRead(ctx, msg);
        }
    }

    @Override
    protected XrootdResponse<ProtocolRequest> doOnProtocolRequest(ChannelHandlerContext ctx, ProtocolRequest msg)
            throws XrootdException
    {
        return new ProtocolResponse(msg, XrootdProtocol.DATA_SERVER);
    }

    @Override
    protected XrootdResponse<LocateRequest> doOnLocate(ChannelHandlerContext ctx, LocateRequest msg) throws XrootdException
    {
        /* To avoid duplicate name space lookups, we always just return ourselves no matter
         * whether the file exists or not.
         */
        return new LocateResponse(msg, new LocateResponse.InfoElement(
                getLocalAddress(), LocateResponse.Node.SERVER, LocateResponse.Access.READ));
    }

    @Override
    protected XrootdResponse<SetRequest> doOnSet(ChannelHandlerContext ctx, SetRequest request) throws XrootdException
    {
        /* The xrootd spec states that we should include 80 characters in our log.
         */
        final String APPID_PREFIX = "appid ";
        final int APPID_PREFIX_LENGTH = APPID_PREFIX.length();
        final int APPID_MSG_LENGTH = 80;
        String data = request.getData();
        if (data.startsWith(APPID_PREFIX)) {
            LOGGER.info(data.substring(APPID_PREFIX_LENGTH,
                                       Math.min(APPID_PREFIX_LENGTH + APPID_MSG_LENGTH, data.length())));
        }
        return new SetResponse(request, "");
    }

    protected InetSocketAddress getLocalAddress()
    {
        return _localAddress;
    }

    protected InetSocketAddress getRemoteAddress()
    {
        return _remoteAddress;
    }

    protected boolean isHealthCheck()
    {
        return _isHealthCheck;
    }
}
