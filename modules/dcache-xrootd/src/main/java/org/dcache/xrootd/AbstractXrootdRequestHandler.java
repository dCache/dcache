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

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdRequestHandler;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.LocateRequest;
import org.dcache.xrootd.protocol.messages.LocateResponse;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
import org.dcache.xrootd.protocol.messages.SetRequest;
import org.dcache.xrootd.protocol.messages.SetResponse;

public class AbstractXrootdRequestHandler extends XrootdRequestHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractXrootdRequestHandler.class);

    @Override
    protected AbstractResponseMessage
        doOnProtocolRequest(ChannelHandlerContext ctx,
                            MessageEvent event, ProtocolRequest msg)
        throws XrootdException
    {
        return new ProtocolResponse(msg, XrootdProtocol.DATA_SERVER);
    }

    @Override
    protected LocateResponse doOnLocate(ChannelHandlerContext ctx, MessageEvent e,
                                        LocateRequest msg) throws XrootdException
    {
        /* To avoid duplicate name space lookups, we always just return ourselves no matter
         * whether the file exists or not.
         */
        return new LocateResponse(msg,
                                  new LocateResponse.InfoElement(
                                          (InetSocketAddress) e.getChannel().getLocalAddress(),
                                          LocateResponse.Node.SERVER,
                                          LocateResponse.Access.READ));
    }

    @Override
    protected Object doOnSet(ChannelHandlerContext ctx, MessageEvent event, SetRequest request) throws XrootdException
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
}
