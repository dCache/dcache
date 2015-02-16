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
package org.dcache.pool.movers;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.security.NoSuchAlgorithmException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.ChecksumFactory;
import diskCacheV111.util.DiskErrorCacheException;
import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.PoolIoFileMessage;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;

import org.dcache.cells.CellStub;
import org.dcache.pool.FaultAction;
import org.dcache.pool.FaultEvent;
import org.dcache.pool.FaultListener;
import org.dcache.pool.classic.Cancellable;
import org.dcache.pool.classic.ChecksumModule;
import org.dcache.pool.classic.PostTransferService;
import org.dcache.pool.classic.TransferService;
import org.dcache.pool.repository.ReplicaDescriptor;
import org.dcache.util.CDCThreadFactory;
import org.dcache.util.PortRange;
import org.dcache.util.TryCatchTemplate;
import org.dcache.vehicles.FileAttributes;

/**
 * Abstract base class for Netty based transfer services. This class provides
 * most methods needed by a pool-side Netty mover.
 *
 * TODO: Cancellation currently doesn't close the netty channel. We rely
 * on the mover closing the MoverChannel, thus as a side effect causing
 * the Netty channel to close.
 */
public abstract class AbstractNettyTransferService<P extends ProtocolInfo>
    implements TransferService<NettyMover<P>>, MoverFactory
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AbstractNettyTransferService.class);

    /** Manages connection timeouts. */
    private ScheduledExecutorService timeoutScheduler;

    /** Event loop for the server channel. */
    private NioEventLoopGroup acceptGroup;

    /** Event loop for the child channels. */
    private NioEventLoopGroup socketGroup;

    /** Shared Netty server channel. */
    private Channel serverChannel;

    /** All open Netty cild channels. */
    private ChannelGroup openChannels = new DefaultChannelGroup(GlobalEventExecutor.INSTANCE);

    /** Socket address of the last server channel created. */
    private InetSocketAddress lastServerAddress;

    /** Port range in which Netty will listen. */
    private PortRange portRange;

    /** UUID to Entry map. */
    private final ConcurrentMap<UUID, Entry> uuids = Maps.newConcurrentMap();

    /** Mover channel to Entry map. */
    private final ConcurrentMap<MoverChannel<P>, Entry> files = Maps.newConcurrentMap();

    /** Server name. */
    private String name;

    /** Number of IO threads. */
    private int threads;

    /** Service to post process movers. */
    private PostTransferService postTransferService;

    /** Listener for critical pool faults. */
    protected FaultListener faultListener;

    /** Service to calculsate and verify checksums. */
    protected ChecksumModule checksumModule;

    /** Timeout for when to disconnect an idle client. */
    protected long clientIdleTimeout;
    protected TimeUnit clientIdleTimeoutUnit;

    /** Timeout for when to give up waiting for a client connection. */
    private long connectTimeout;
    private TimeUnit connectTimeoutUnit;

    /** Communication stub for talking to doors. */
    protected CellStub doorStub;

    public AbstractNettyTransferService(String name)
    {
        this.name = name;
    }

    @Required
    public void setChecksumModule(ChecksumModule checksumModule)
    {
        this.checksumModule = checksumModule;
    }

    @Required
    public void setThreads(int threads)
    {
        this.threads = threads;
    }

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

    public long getClientIdleTimeout()
    {
        return clientIdleTimeout;
    }

    @Required
    public void setClientIdleTimeout(long clientIdleTimeout)
    {
        this.clientIdleTimeout = clientIdleTimeout;
    }

    public TimeUnit getClientIdleTimeoutUnit()
    {
        return clientIdleTimeoutUnit;
    }

    @Required
    public void setClientIdleTimeoutUnit(TimeUnit clientIdleTimeoutUnit)
    {
        this.clientIdleTimeoutUnit = clientIdleTimeoutUnit;
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


    @Required
    public void setDoorStub(CellStub stub)
    {
        this.doorStub = stub;
    }

    @Required
    public void setPortRange(PortRange portRange)
    {
        this.portRange = portRange;
    }

    public PortRange getPortRange()
    {
        return portRange;
    }

    protected void initChannel(Channel ch) throws Exception
    {
        ch.pipeline().addLast(new ChannelInboundHandlerAdapter() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception
            {
                openChannels.add(ctx.channel());
                super.channelActive(ctx);
            }

            @Override
            public void channelInactive(ChannelHandlerContext ctx) throws Exception
            {
                super.channelInactive(ctx);
                openChannels.remove(ctx.channel());
                conditionallyStopServer();
            }
        });
    }

    /**
     * Start netty server.
     *
     * @throws IOException Starting the server failed
     */
    protected synchronized void startServer() throws IOException {
        if (serverChannel == null) {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(acceptGroup, socketGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, false)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(new ChannelInitializer<Channel>()
                    {
                        @Override
                        protected void initChannel(Channel ch) throws Exception
                        {
                            AbstractNettyTransferService.this.initChannel(ch);
                        }
                    });

            serverChannel = portRange.bind(bootstrap);
            lastServerAddress = (InetSocketAddress) serverChannel.localAddress();
            LOGGER.debug("Started {} on {}", getClass().getSimpleName(), lastServerAddress);
        }
    }

    /**
     * Stop netty server.
     */
    protected synchronized void stopServer()
    {
        if (serverChannel != null) {
            LOGGER.debug("Stopping {} on {}", getClass().getSimpleName(), lastServerAddress);
            serverChannel.close();
            serverChannel = null;
        }
    }

    @PostConstruct
    public synchronized void init()
    {
        timeoutScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(name + "-connect-timeout").build());
        acceptGroup = new NioEventLoopGroup(0, new CDCThreadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-listen-%d").build()));
        socketGroup = new NioEventLoopGroup(threads, new CDCThreadFactory(new ThreadFactoryBuilder().setNameFormat(
                name + "-net-%d").build()));
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        stopServer();
        timeoutScheduler.shutdown();

        acceptGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
        socketGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);

        try {
            if (timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                acceptGroup.terminationFuture().sync();
                socketGroup.terminationFuture().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Start server if there are any registered channels.
     */
    protected synchronized void conditionallyStartServer() throws IOException {
        if (!uuids.isEmpty()) {
            startServer();
        }
    }

    /**
     * Stop server if there are no channels.
     */
    protected synchronized void conditionallyStopServer() {
        if (openChannels.isEmpty() && uuids.isEmpty()) {
            stopServer();
        }
    }

    /**
     * @return The address to which the server channel was last bound.
     */
    public synchronized InetSocketAddress getServerAddress() {
        return lastServerAddress;
    }

    private ChecksumFactory getChecksumFactoryFor(ReplicaDescriptor handle) throws CacheException
    {
        if (checksumModule.hasPolicy(ChecksumModule.PolicyFlag.ON_TRANSFER)) {
            try {
                return checksumModule.getPreferredChecksumFactory(handle);
            } catch (NoSuchAlgorithmException e) {
                throw new CacheException("Failed to instantiate mover due to unsupported checksum type: " + e.getMessage(), e);
            }
        }
        return null;
    }

    @Override
    public Mover<?> createMover(ReplicaDescriptor handle, PoolIoFileMessage message,
                                CellPath pathToDoor) throws CacheException
    {
        return new NettyMover<>(handle, message, pathToDoor, this, getChecksumFactoryFor(handle));
    }

    @Override
    public Cancellable executeMover(final NettyMover<P> mover, CompletionHandler<Void, Void> completionHandler)
            throws IOException, CacheException, NoRouteToCellException
    {
        return new TryCatchTemplate<Void, Void>(completionHandler) {
            @Override
            public void execute()
                    throws Exception
            {
                UUID uuid = createUuid(mover);
                MoverChannel<P> channel = autoclose(mover.open());
                setCancellable(register(channel, uuid, connectTimeoutUnit.toMillis(connectTimeout), this));
                sendAddressToDoor(mover, getServerAddress().getPort(), uuid);
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

    @Override
    public void closeMover(NettyMover<P> mover, CompletionHandler<Void, Void> completionHandler)
    {
        postTransferService.execute(mover, completionHandler);
    }

    private synchronized Cancellable register(
            MoverChannel<P> file, UUID uuid, long connectTimeout, CompletionHandler<Void, Void> completionHandler)
        throws IOException
    {
        Entry entry = new Entry(file, uuid, connectTimeout, completionHandler);

        if (uuids.putIfAbsent(uuid, entry) != null) {
            throw new IllegalStateException("UUID conflict");
        }
        if (files.putIfAbsent(file, entry) != null) {
            uuids.remove(uuid);
            throw new IllegalStateException("Mover is already registered");
        }

        conditionallyStartServer();
        return entry;
    }

    private synchronized void unregister(Entry entry)
    {
        files.remove(entry._file);
        uuids.remove(entry._uuid);
        conditionallyStopServer();
    }

    public FileAttributes getFileAttributes(UUID uuid)
    {
        Entry entry = uuids.get(uuid);
        return (entry == null) ? null : entry.getFileAttributes();
    }

    public MoverChannel<P> openFile(UUID uuid, boolean exclusive)
    {
        Entry entry = uuids.get(uuid);
        return (entry == null) ? null : entry.open(exclusive);
    }

    public void closeFile(MoverChannel<P> channel)
    {
        Entry entry = files.get(channel);
        if (entry != null) {
            entry.close();
        }
    }

    public void closeFile(MoverChannel<P> channel, Exception exception)
    {
        Entry entry = files.get(channel);
        if (entry != null) {
            entry.close(exception);
        }
    }

    private class Entry implements Cancellable
    {
        private final Sync _sync = new Sync();
        private final MoverChannel<P> _file;
        private final UUID _uuid;
        private final Future<?> _timeout;
        private final CompletionHandler<Void, Void> _completionHandler;
        private final CDC _cdc = new CDC();

        Entry(MoverChannel<P> file, UUID uuid, final long connectTimeout, CompletionHandler<Void, Void> completionHandler) {
            _file = file;
            _uuid = uuid;
            _completionHandler = completionHandler;
            _timeout = timeoutScheduler.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    try (CDC ignored = _cdc.restore()) {
                        if (_sync.onTimeout()) {
                            _completionHandler.failed(new TimeoutCacheException("No connection from client after " +
                                    TimeUnit.MILLISECONDS.toSeconds(connectTimeout) + " seconds. Giving up."), null);
                        }
                    }
                }
            }, connectTimeout, TimeUnit.MILLISECONDS);
        }

        MoverChannel<P> open(boolean exclusive) {
            return _sync.open(exclusive);
        }

        void close() {
            try (CDC ignored = _cdc.restore()) {
                if (_sync.onClose()) {
                    _completionHandler.completed(null, null);
                }
            }
        }

        void close(Exception exception) {
            try (CDC ignored = _cdc.restore()) {
                if (_sync.onFailure()) {
                    _completionHandler.failed(exception, null);
                }
            }
        }

        @Override
        public void cancel()
        {
            try (CDC ignored = _cdc.restore()) {
                if (_sync.onCancel()) {
                    _completionHandler.failed(new InterruptedException("Transfer was interrupted"), null);
                }
            }
        }

        public FileAttributes getFileAttributes()
        {
            return _file.getFileAttributes();
        }

        private class Sync
        {
            private int _open;
            private boolean _isExclusive;
            private boolean _isClosed;

            synchronized MoverChannel<P> open(boolean exclusive) {
                if (_isExclusive || _isClosed) {
                    return null;
                }
                _isExclusive = exclusive;
                _open++;
                _timeout.cancel(false);
                return _file;
            }

            synchronized boolean onClose() {
                _open--;
                return _open <= 0 && unregister();
            }

            synchronized boolean onFailure() {
                _open--;
                return unregister();
            }

            synchronized boolean onCancel()
            {
                return unregister();
            }

            synchronized boolean onTimeout()
            {
                return (_open == 0) && unregister();
            }

            private boolean unregister()
            {
                if (!_isClosed) {
                    _isClosed = true;
                    _timeout.cancel(false);
                    AbstractNettyTransferService.this.unregister(Entry.this);
                    return true;
                }
                return false;
            }
        }
    }

    protected abstract void sendAddressToDoor(NettyMover<P> mover, int port, UUID uuid)
        throws Exception;

    protected abstract UUID createUuid(NettyMover<P> mover);
}
