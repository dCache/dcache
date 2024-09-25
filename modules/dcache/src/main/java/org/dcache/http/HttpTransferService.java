/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013-2024 Deutsches Elektronen-Synchrotron
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

import static io.netty.handler.codec.http.HttpHeaderNames.AUTHORIZATION;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_MD5;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpMethod.GET;
import static io.netty.handler.codec.http.HttpMethod.HEAD;
import static io.netty.handler.codec.http.HttpMethod.PUT;

import com.google.common.collect.ImmutableMap;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import org.dcache.pool.movers.NettyMover;
import org.dcache.pool.movers.NettyTransferService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Netty-based HTTP transfer service.
 * <p>
 * The service generates a UUID that identifies the transfer and sends it back as a part of the
 * address information to the door.
 * <p>
 * This UUID has to be included in client requests to the netty server, so the netty server can
 * extract the right mover.
 * <p>
 * The netty server are started on demand and shared by all http transfers of a pool. All transfers
 * are handled on the same port.
 */
public class HttpTransferService extends NettyTransferService<HttpProtocolInfo> {

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTransferService.class);

    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";

    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private int chunkSize;
    private ImmutableMap<String, String> customHeaders;

    public HttpTransferService() {
        super("http");
    }

    CorsConfigBuilder corsConfigBuilder() {
        return CorsConfigBuilder.forAnyOrigin()
              .allowNullOrigin()
              .allowedRequestMethods(GET, PUT, HEAD)
              .allowedRequestHeaders(CONTENT_TYPE, AUTHORIZATION, CONTENT_MD5,
                    "Want-Digest", "suppress-www-authenticate");
    }

    public int getChunkSize() {
        return chunkSize;
    }

    @Required
    public void setChunkSize(int chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Required
    public void setCustomHeaders(ImmutableMap<String, String> headers) {
        customHeaders = headers;
    }

    @Override
    protected UUID createUuid(HttpProtocolInfo protocolInfo) {
        return UUID.randomUUID();
    }

    /**
     * Send the network address of this mover to the door, along with the UUID identifying it.
     */
    @Override
    protected void sendAddressToDoor(NettyMover<HttpProtocolInfo> mover, InetSocketAddress localEndppoint)
          throws SocketException, CacheException {
        HttpProtocolInfo protocolInfo = mover.getProtocolInfo();
        String uri;
        try {
            uri = getUri(protocolInfo, localEndppoint, mover.getUuid()).toASCIIString();
        } catch (URISyntaxException e) {
            throw new RuntimeException(
                  "Failed to create URI for HTTP mover. Please report to support@dcache.org", e);
        }
        CellAddressCore httpDoor = new CellAddressCore(
              protocolInfo.getHttpDoorCellName(), protocolInfo.getHttpDoorDomainName());
        LOGGER.debug("Sending redirect URI {} to {}", uri, httpDoor);
        HttpDoorUrlInfoMessage httpDoorMessage =
              new HttpDoorUrlInfoMessage(mover.getFileAttributes().getPnfsId().toString(), uri);
        httpDoorMessage.setId(protocolInfo.getSessionId());

        doorStub.notify(new CellPath(httpDoor), httpDoorMessage);
    }

    protected URI getUri(HttpProtocolInfo protocolInfo, InetSocketAddress localEndpoint, UUID uuid)
          throws SocketException, CacheException, URISyntaxException {
        String path = protocolInfo.getPath();
        if (!path.startsWith("/")) {
            path = "/" + path;
        }
        return new URI(PROTOCOL_HTTP,
              null,
              localEndpoint.getAddress().getHostAddress(),
              localEndpoint.getPort(),
              path,
              UUID_QUERY_PARAM + QUERY_PARAM_ASSIGN + uuid.toString(),
              null);
    }

    @Override
    protected void initChannel(Channel ch) throws Exception {
        super.initChannel(ch);
        addChannelHandlers(ch.pipeline());
    }

    /**
     * Indicates whatever under laying socket setup supports in-kernel zero-copy
     */
    protected boolean canZeroCopy() {
        return true;
    }

    protected void addChannelHandlers(ChannelPipeline pipeline) throws Exception {
        // construct HttpRequestDecoder as netty defaults, except configurable chunk size
        pipeline.addLast("decoder", new HttpRequestDecoder(4096, 8192, getChunkSize(), true));
        pipeline.addLast("encoder", new HttpResponseEncoder());

        if (LOGGER.isDebugEnabled()) {
            pipeline.addLast("logger", new LoggingHandler());
        }
        pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
        pipeline.addLast("keepalive", new KeepAliveHandler());

        if (!customHeaders.isEmpty()) {
            pipeline.addLast("custom-headers", new CustomResponseHeadersHandler(customHeaders));
        }

        pipeline.addLast("cors", new CorsHandler(corsConfigBuilder().build()));

        pipeline.addLast("transfer", new HttpPoolRequestHandler(this, chunkSize, canZeroCopy()));
    }
}
