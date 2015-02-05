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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.CompletionHandler;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.movers.AbstractNettyTransferService;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.NettyMover;
import org.dcache.util.NetworkUtils;
import org.dcache.util.TryCatchTemplate;

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
public class HttpTransferService extends AbstractNettyTransferService<HttpProtocolInfo>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTransferService.class);

    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";

    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private long connectTimeout;
    private TimeUnit connectTimeoutUnit;
    private int chunkSize;

    public HttpTransferService()
    {
        super("http");
    }

    public long getConnectTimeout()
    {
        return connectTimeout;
    }

    @Required
    public void setConnectTimeout(long connectTimeout)
    {
        this.connectTimeout = connectTimeout;
    }

    public TimeUnit getConnectTimeoutUnit()
    {
        return connectTimeoutUnit;
    }

    @Required
    public void setConnectTimeoutUnit(TimeUnit connectTimeoutUnit)
    {
        this.connectTimeoutUnit = connectTimeoutUnit;
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

    @Override
    public Cancellable execute(final NettyMover<HttpProtocolInfo> mover, CompletionHandler<Void, Void> completionHandler) throws Exception
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            public void execute()
                    throws IOException, CacheException, NoRouteToCellException
            {
                UUID uuid = UUID.randomUUID();
                MoverChannel<HttpProtocolInfo> channel = autoclose(mover.open());
                setCancellable(register(channel, uuid, connectTimeoutUnit.toMillis(connectTimeout), this));
                sendAddressToDoor(mover, getServerAddress().getPort(), uuid);
            }

            @Override
            public void onFailure(Throwable t, Void attachment) throws CacheException
            {
                if (t instanceof DiskErrorCacheException) {
                    faultListener.faultOccurred(new FaultEvent("repository", FaultAction.DISABLED,
                            t.getMessage(), t));
                } else if (t instanceof NoRouteToCellException) {
                    throw new CacheException("Failed to send redirect message to door: " + t.getMessage(), t);
                }
            }
        };
    }

    /**
     * Send the network address of this mover to the door, along with the UUID
     * identifying it
     */
    private void sendAddressToDoor(NettyMover<HttpProtocolInfo> mover, int port, UUID uuid)
            throws SocketException, CacheException, NoRouteToCellException
    {
        HttpProtocolInfo protocolInfo = mover.getProtocolInfo();
        String uri;
        try {
            uri = getUri(protocolInfo, port, uuid).toASCIIString();
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
    protected ChannelInitializer newChannelInitializer()
    {
        return new HttpChannelInitializer();
    }

    /**
     * Factory that creates new server handler.
     *
     * The pipeline can handle HTTP compression and chunked transfers.
     *
     * @author tzangerl
     *
     */
    class HttpChannelInitializer extends ChannelInitializer
    {
        @Override
        protected void initChannel(Channel ch) throws Exception
        {
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
            pipeline.addLast("transfer", new HttpPoolRequestHandler(HttpTransferService.this, chunkSize));
        }
    }
}
