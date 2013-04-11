/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013 Deutsches Elektronen-Synchrotron
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

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.CompletionHandler;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.HttpDoorUrlInfoMessage;
import diskCacheV111.vehicles.HttpProtocolInfo;
import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.repository.ReplicaDescriptor;
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
public class HttpTransferService extends AbstractCellComponent implements MoverFactory, TransferService<HttpMover>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpTransferService.class);

    public static final String UUID_QUERY_PARAM = "dcache-http-uuid";
    private static final String QUERY_PARAM_ASSIGN = "=";
    private static final String PROTOCOL_HTTP = "http";

    private PostTransferService postTransferService;
    private FaultListener faultListener;
    private ChecksumModule checksumModule;
    private long connectTimeout;
    private int diskThreads;
    private int maxMemoryPerConnection;
    private int maxMemory;
    private int chunkSize;
    private long clientIdleTimeout;
    private Integer socketThreads;

    private HttpPoolNettyServer server;

    @Required
    public void setPostTransferService(
            PostTransferService postTransferService)
    {
        this.postTransferService = postTransferService;
    }

    @Required
    public void setFaultListener(FaultListener faultListener)
    {
        this.faultListener = faultListener;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule)
    {
        this.checksumModule = checksumModule;
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

    public int getDiskThreads()
    {
        return diskThreads;
    }

    @Required
    public void setDiskThreads(int diskThreads)
    {
        this.diskThreads = diskThreads;
    }

    public int getMaxMemoryPerConnection()
    {
        return maxMemoryPerConnection;
    }

    @Required
    public void setMaxMemoryPerConnection(int maxMemoryPerConnection)
    {
        this.maxMemoryPerConnection = maxMemoryPerConnection;
    }

    public int getMaxMemory()
    {
        return maxMemory;
    }

    @Required
    public void setMaxMemory(int maxMemory)
    {
        this.maxMemory = maxMemory;
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

    public long getClientIdleTimeout()
    {
        return clientIdleTimeout;
    }

    @Required
    public void setClientIdleTimeout(long clientIdleTimeout)
    {
        this.clientIdleTimeout = clientIdleTimeout;
    }

    public String getSocketThreads()
    {
        return (socketThreads == null) ? null : String.valueOf(socketThreads);
    }

    public void setSocketThreads(String socketThreads)
    {
        this.socketThreads = Strings.isNullOrEmpty(socketThreads) ? null : Integer.valueOf(socketThreads);
    }

    @PostConstruct
    public void init()
    {
        if (socketThreads == null) {
            server = new HttpPoolNettyServer(diskThreads,
                    maxMemoryPerConnection,
                    maxMemory,
                    chunkSize,
                    clientIdleTimeout);
        } else {
            server = new HttpPoolNettyServer(diskThreads,
                    maxMemoryPerConnection,
                    maxMemory,
                    chunkSize,
                    clientIdleTimeout,
                    socketThreads);
        }
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
                                CellPath pathToDoor) throws CacheException
    {
        ChecksumFactory checksumFactory;
        if (checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER)) {
            try {
                checksumFactory = checksumModule.getPreferredChecksumFactory(handle);
            } catch (NoSuchAlgorithmException e) {
                throw new CacheException("Failed to instantiate HTTP mover due to unsupported checksum type: " + e.getMessage(), e);
            }
        } else {
            checksumFactory = null;
        }
        return new HttpMover(handle, message, pathToDoor, this, postTransferService, checksumFactory);
    }

    @Override
    public Cancellable execute(final HttpMover mover, CompletionHandler<Void, Void> completionHandler) throws Exception
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            public void execute()
                    throws IOException, CacheException, NoRouteToCellException
            {
                UUID uuid = UUID.randomUUID();
                MoverChannel<HttpProtocolInfo> channel = autoclose(mover.open());
                setCancellable(server.register(channel, uuid, connectTimeout, this));
                sendAddressToDoor(mover, server.getServerAddress().getPort(), uuid);
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
    private void sendAddressToDoor(HttpMover mover, int port, UUID uuid)
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
        sendMessage(new CellMessage(new CellPath(httpDoor), httpDoorMessage));
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
}
