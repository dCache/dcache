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
package org.dcache.xrootd.pool;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.CompletionHandler;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.AbstractCellComponent;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.movers.Mover;
import org.dcache.pool.movers.MoverChannel;
import org.dcache.pool.movers.MoverFactory;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.NetworkUtils;
import org.dcache.util.TryCatchTemplate;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;

/**
 * xrootd transfer service.
 *
 * The transfer service uses a Netty server. The Netty server is started dynamically
 * as soon as any xrootd movers have been executed. The server shuts down once the
 * last xrootd movers terminates.
 *
 * Xrootd movers are registered with the Netty server using a UUID. The UUID is
 * relayed to the door which includes it in the xrootd redirect sent to the client.
 * The redirected client will include the UUID when connecting to the pool and
 * serves as an one-time authorization token and as a means of binding the client
 * request to the correct mover.
 *
 * A transfer is considered to have succeeded if at least one file was opened and
 * all opened files were closed again.
 *
 * Open issues:
 *
 * * Write calls blocked on space allocation may starve read
 *   calls. This is because both are served by the same thread
 *   pool. This should be changed such that separate thread pools are
 *   used (may fix later).
 *
 * * Write messages are currently processed as one big message. If the
 *   client chooses to upload a huge file as a single write message,
 *   then the pool will run out of memory. We can fix this by breaking
 *   a write message into many small blocks. The old mover suffers
 *   from the same problem (may fix later).
 *
 * * At least for vector read, the behaviour when reading beyond the
 *   end of the file is wrong.
 */
public class XrootdTransferService
        extends AbstractCellComponent implements MoverFactory, TransferService<XrootdMover>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(XrootdTransferService.class);

    private static final long CONNECT_TIMEOUT =
            TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES);

    private PostTransferService postTransferService;
    private FaultListener faultListener;
    private int diskThreads;
    private int maxMemoryPerConnection;
    private int maxMemory;
    private long clientIdleTimeout;
    private Integer socketThreads;
    private List<ChannelHandlerFactory> plugins;

    private XrootdPoolNettyServer server;

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
    public void setDiskThreads(int threads)
    {
        this.diskThreads = threads;
    }

    public int getDiskThreads()
    {
        return diskThreads;
    }

    @Required
    public void setMaxMemoryPerConnection(int bytes)
    {
        this.maxMemoryPerConnection = bytes;
    }

    public int getMaxMemoryPerConnection()
    {
        return maxMemoryPerConnection;
    }

    @Required
    public void setMaxMemory(int bytes)
    {
        this.maxMemory = bytes;
    }

    public int getMaxMemory()
    {
        return maxMemory;
    }

    @Required
    public void setClientIdleTimeout(long clientIdleTimeout)
    {
        this.clientIdleTimeout = clientIdleTimeout;
    }

    public long getClientIdleTimeout()
    {
        return clientIdleTimeout;
    }

    public void setSocketThreads(String socketThreads)
    {
        this.socketThreads = Strings.isNullOrEmpty(socketThreads) ? null : Integer.parseInt(socketThreads);
    }

    public String getSocketThreads()
    {
        return (socketThreads == null) ? null : String.valueOf(socketThreads);
    }

    @Required
    public void setPlugins(List<ChannelHandlerFactory> plugins)
    {
        this.plugins = plugins;
    }

    public List<ChannelHandlerFactory> getPlugins()
    {
        return plugins;
    }

    @PostConstruct
    private synchronized void init() throws Exception
    {
        if (socketThreads == null) {
            server = new XrootdPoolNettyServer(
                    diskThreads,
                    maxMemoryPerConnection,
                    maxMemory,
                    clientIdleTimeout,
                    plugins);
        } else {
            server = new XrootdPoolNettyServer(
                    diskThreads,
                    maxMemoryPerConnection,
                    maxMemory,
                    clientIdleTimeout,
                    plugins,
                    socketThreads);
        }
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
                             CellPath pathToDoor) throws CacheException
    {
        return new XrootdMover(handle, message, pathToDoor, this, postTransferService);
    }

    @Override
    public Cancellable execute(final XrootdMover mover, CompletionHandler<Void, Void> completionHandler)
            throws IOException, CacheException, NoRouteToCellException
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            public void execute()
                    throws IOException, CacheException, NoRouteToCellException
            {
                UUID uuid = mover.getProtocolInfo().getUUID();
                MoverChannel<XrootdProtocolInfo> channel = autoclose(mover.open());
                setCancellable(server.register(channel, uuid, CONNECT_TIMEOUT, this));
                sendAddressToDoor(mover, server.getServerAddress().getPort());
            }

            @Override
            public void onFailure(Throwable t, Void attachment)
                    throws CacheException
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
     * Sends our address to the door. Copied from the old xrootd mover.
     */
    private void sendAddressToDoor(XrootdMover mover, int port)
            throws SocketException, CacheException, NoRouteToCellException
    {
        XrootdProtocolInfo protocolInfo = mover.getProtocolInfo();
        InetAddress localIP = NetworkUtils.getLocalAddress(protocolInfo.getSocketAddress().getAddress());
        CellPath cellpath = protocolInfo.getXrootdDoorCellPath();
        XrootdDoorAdressInfoMessage doorMsg =
                new XrootdDoorAdressInfoMessage(protocolInfo.getXrootdFileHandle(), new InetSocketAddress(localIP, port));
        sendMessage(new CellMessage(cellpath, doorMsg));
        LOGGER.debug("sending redirect {} to Xrootd-door {}", localIP, cellpath);
    }
}
