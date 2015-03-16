/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013-2015 Deutsches Elektronen-Synchrotron
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

import com.google.common.collect.ImmutableMap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;

import org.dcache.pool.movers.NettyMover;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.util.NetworkUtils;

/**
 * Netty-based HTTP transfer service.
 *
 * The service generates a UUID that identifies the transfer and sends it back
 * as a part of the address information to the door.
 *
 * This UUID has to be included in client requests to the netty server, so the
 * netty server can extract the right mover.
 *
 * The netty server are started on demand and shared by all http transfers of
 * a pool. All transfers are handled on the same port.
 */
public class HttpTransferService extends NettyTransferService<HttpProtocolInfo>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTransferService.class);

    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";

    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private int chunkSize;
    private ImmutableMap<String,String> customHeaders;

    public HttpTransferService()
    {
        super("http");
    }

    public int getChunkSize()
    {
        return chunkSize;
    }

    @Required
    public void setChunkSize(int chunkSize)
    {
        this.chunkSize = chunkSize;
    }

    @Required
    public void setCustomHeaders(ImmutableMap<String,String> headers)
    {
        customHeaders = headers;
    }

    @Override
    protected UUID createUuid(HttpProtocolInfo protocolInfo)
    {
        return UUID.randomUUID();
    }

    /**
     * Send the network address of this mover to the door, along with the UUID
     * identifying it.
     */
    @Override
    protected void sendAddressToDoor(NettyMover<HttpProtocolInfo> mover, int port)
            throws SocketException, CacheException
    {
        HttpProtocolInfo protocolInfo = mover.getProtocolInfo();
        String uri;
        try {
            uri = getUri(protocolInfo, port, mover.getUuid()).toASCIIString();
        } catch (URISyntaxException e) {
            throw new RuntimeException("Failed to create URI for HTTP mover. Please report to support@dcache.org", e);
        }
        CellAddressCore httpDoor = new CellAddressCore(
                protocolInfo.getHttpDoorCellName(), protocolInfo.getHttpDoorDomainName());
        LOGGER.debug("Sending redirect URI {} to {}", uri, httpDoor);
        HttpDoorUrlInfoMessage httpDoorMessage =
                new HttpDoorUrlInfoMessage(mover.getFileAttributes().getPnfsId().getId(), uri);
        httpDoorMessage.setId(protocolInfo.getSessionId());

        doorStub.notify(new CellPath(httpDoor), httpDoorMessage);
    }

    private URI getUri(HttpProtocolInfo protocolInfo, int port, UUID uuid)
            throws SocketException, CacheException, URISyntaxException
    {
        String path = protocolInfo.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        InetAddress localIP =
                NetworkUtils.getLocalAddress(protocolInfo.getSocketAddress().getAddress());
        return new URI(PROTOCOL_HTTP,
                null,
                localIP.getCanonicalHostName(),
                port,
                path,
                UUID_QUERY_PARAM + QUERY_PARAM_ASSIGN + uuid.toString(),
                null);
    }

    @Override
    protected void initChannel(Channel ch) throws Exception
    {
        super.initChannel(ch);

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("decoder", new HttpRequestDecoder());
        pipeline.addLast("encoder", new HttpResponseEncoder());

        if (LOGGER.isDebugEnabled()) {
            pipeline.addLast("logger", new LoggingHandler());
        }
        pipeline.addLast("idle-state-handler",
                         new IdleStateHandler(0,
                                              0,
                                              clientIdleTimeout,
                                              clientIdleTimeoutUnit));
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());

        if (!customHeaders.isEmpty()) {
            pipeline.addLast("custom-headers", new CustomResponseHeadersHandler(customHeaders));
        }

        pipeline.addLast("transfer", new HttpPoolRequestHandler(this, chunkSize));
    }
}
