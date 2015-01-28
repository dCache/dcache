package org.dcache.pool.movers;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;

import org.dcache.pool.classic.Cancellable;
import org.dcache.util.CDCThreadFactory;
import org.dcache.util.PortRange;
import org.dcache.vehicles.FileAttributes;

/**
 * Abstract base class for all netty servers running on the pool
 * dispatching movers. This class provides most methods needed by a
 * pool-side netty mover.
 *
 * TODO: Cancellation currently doesn't close the netty channel. We rely
 * on the mover closing the MoverChannel, thus as a side effect causing
 * the Netty channel to close.
 *
 * @author tzangerl
 */
public abstract class AbstractNettyServer<T extends ProtocolInfo>
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(AbstractNettyServer.class);

    /**
     * Manages connection timeouts.
     */
    private final ScheduledExecutorService _timeoutScheduler;


    private final NioEventLoopGroup _acceptGroup;
    private final NioEventLoopGroup _socketGroup;

    /**
     * Shared Netty server channel
     */
    private Channel _serverChannel;

    /**
     * Socket address of the last server channel created.
     */
    private InetSocketAddress _lastServerAddress;

    private PortRange _portRange = new PortRange(0);

    private final ConcurrentMap<UUID,Entry> _uuids =
        Maps.newConcurrentMap();
    private final ConcurrentMap<MoverChannel<T>,Entry> _channels =
        Maps.newConcurrentMap();

    public AbstractNettyServer(String name, int threads)
    {
        _timeoutScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(name + "-connect-timeout").build());
        _acceptGroup = new NioEventLoopGroup(0, new CDCThreadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-listen-%d").build()));
        _socketGroup = new NioEventLoopGroup(threads, new CDCThreadFactory(new ThreadFactoryBuilder().setNameFormat(name + "-net-%d").build()));
    }

    /**
     * Start netty server.
     *
     * @throws IOException Starting the server failed
     */
    protected synchronized void startServer() throws IOException {
        if (_serverChannel == null) {
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group(_acceptGroup, _socketGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.TCP_NODELAY, false)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childHandler(newChannelInitializer());

            _serverChannel = _portRange.bind(bootstrap);
            _lastServerAddress = (InetSocketAddress) _serverChannel.localAddress();
            LOGGER.debug("Started {} on {}", getClass().getSimpleName(), _lastServerAddress);
        }
    }

    /**
     * Stop netty server.
     */
    protected synchronized void stopServer()
    {
        if (_serverChannel != null) {
            LOGGER.debug("Stopping {} on {}", getClass().getSimpleName(), _lastServerAddress);
            _serverChannel.close();
            _serverChannel = null;
        }
    }

    public synchronized void shutdown()
    {
        stopServer();
        _timeoutScheduler.shutdown();

        _acceptGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
        _socketGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);

        try {
            if (_timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                _acceptGroup.terminationFuture().sync();
                _socketGroup.terminationFuture().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Start server if there are any registered channels.
     */
    protected synchronized void conditionallyStartServer() throws IOException {
        if (!_uuids.isEmpty()) {
            startServer();
        }
    }

    /**
     * Stop server if there are no channels.
     */
    protected synchronized void conditionallyStopServer() {
        if (_uuids.isEmpty()) {
            stopServer();
        }
    }

    /**
     * @return The address to which the server channel was last bound.
     */
    public synchronized InetSocketAddress getServerAddress() {
        return _lastServerAddress;
    }

    public synchronized Cancellable register(
            MoverChannel<T> channel, UUID uuid, long connectTimeout, CompletionHandler<Void, Void> completionHandler)
        throws IOException
    {
        Entry entry = new Entry(channel, uuid, connectTimeout, completionHandler);

        if (_uuids.putIfAbsent(uuid, entry) != null) {
            throw new IllegalStateException("UUID conflict");
        }
        if (_channels.putIfAbsent(channel, entry) != null) {
            _uuids.remove(uuid);
            throw new IllegalStateException("Mover is already registered");
        }

        conditionallyStartServer();
        return entry;
    }

    private synchronized void unregister(Entry entry)
    {
        _channels.remove(entry._channel);
        _uuids.remove(entry._uuid);
        conditionallyStopServer();
    }

    public FileAttributes getFileAttributes(UUID uuid)
    {
        Entry entry = _uuids.get(uuid);
        return (entry == null) ? null : entry.getFileAttributes();
    }

    public MoverChannel<T> open(UUID uuid, boolean exclusive)
    {
        Entry entry = _uuids.get(uuid);
        return (entry == null) ? null : entry.open(exclusive);
    }

    public void close(MoverChannel<T> channel)
    {
        Entry entry = _channels.get(channel);
        if (entry != null) {
            entry.close();
        }
    }

    public void close(MoverChannel<T> channel, Exception exception)
    {
        Entry entry = _channels.get(channel);
        if (entry != null) {
            entry.close(exception);
        }
    }

    private class Entry implements Cancellable
    {
        private final Sync _sync = new Sync();
        private final MoverChannel<T> _channel;
        private final UUID _uuid;
        private final Future<?> _timeout;
        private final CompletionHandler<Void, Void> _completionHandler;
        private final CDC _cdc = new CDC();

        Entry(MoverChannel<T> channel, UUID uuid, final long connectTimeout, CompletionHandler<Void, Void> completionHandler) {
            _channel = channel;
            _uuid = uuid;
            _completionHandler = completionHandler;
            _timeout = _timeoutScheduler.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    try (CDC ignored = _cdc.restore()) {
                        if (_sync.timeout()) {
                            _completionHandler.failed(new TimeoutCacheException("No connection from client after " +
                                    TimeUnit.MILLISECONDS.toSeconds(connectTimeout) + " seconds. Giving up."), null);
                        }
                    }
                }
            }, connectTimeout, TimeUnit.MILLISECONDS);
        }

        MoverChannel<T> open(boolean exclusive) {
            return _sync.open(exclusive);
        }

        void close() {
            close(null);
        }

        void close(Exception exception) {
            try (CDC ignored = _cdc.restore()) {
                if (_sync.close(exception)) {
                    if (exception != null) {
                        _completionHandler.failed(exception, null);
                    } else {
                        _completionHandler.completed(null, null);
                    }
                }
            }
        }

        @Override
        public void cancel()
        {
            try (CDC ignored = _cdc.restore()) {
                if (_sync.cancel()) {
                    _completionHandler.failed(new InterruptedException("Transfer was interrupted"), null);
                }
            }
        }

        public FileAttributes getFileAttributes()
        {
            return _channel.getFileAttributes();
        }

        private class Sync
        {
            private int _open;
            private boolean _isExclusive;
            private boolean _isClosed;

            synchronized MoverChannel<T> open(boolean exclusive) {
                if (_isExclusive || _isClosed) {
                    return null;
                }
                _isExclusive = exclusive;
                _open++;
                _timeout.cancel(false);
                return _channel;
            }

            synchronized boolean close(Exception exception) {
                _open--;
                return (exception != null || _open <= 0) && close();
            }

            synchronized boolean cancel()
            {
                return close();
            }

            synchronized boolean timeout()
            {
                return (_open == 0) && close();
            }

            private boolean close()
            {
                if (!_isClosed) {
                    _isClosed = true;
                    _timeout.cancel(false);
                    unregister(Entry.this);
                    return true;
                }
                return false;
            }
        }
    }

    /**
     * Child classes should produce suitable pipeline factories here.
     * @return ChannelPipelineFactory adapted to child class.
     */
    protected abstract ChannelInitializer newChannelInitializer();

    public PortRange getPortRange() {
        return _portRange;
    }

    public void setPortRange(PortRange portRange) {
        _portRange = portRange;
    }
}
