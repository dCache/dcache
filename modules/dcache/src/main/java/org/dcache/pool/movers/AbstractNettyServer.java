package org.dcache.pool.movers;

import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.CompletionHandler;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CDC;

import org.dcache.pool.classic.Cancellable;
import org.dcache.util.PortRange;
import org.dcache.vehicles.FileAttributes;

/**
 * Abstract base class for all netty servers running on the pool
 * dispatching movers. This class provides most methods needed by a
 * pool-side netty mover. Minimally, extending classes need to provide
 * their own channel-pipelines, their port-range and the logic used
 * for starting/stopping the server.
 *
 * TODO: Cancellation currently doesn't close the netty channel. We rely
 * on the mover closing the MoverChannel, thus as a side effect causing
 * the Netty channel to close.
 *
 * @author tzangerl
 */
public abstract class AbstractNettyServer<T extends ProtocolInfo>
{
    private final static Logger _logger =
        LoggerFactory.getLogger(AbstractNettyServer.class);

    /**
     * Shared thread pool accepting TCP connections.
     */
    private final ExecutorService _acceptExecutor;

    /**
     * Shared thread pool performing non-blocking socket IO.
     */
    private final ExecutorService _socketExecutor;

    /**
     * Shared thread pool performing blocking disk IO.
     */
    private final ExecutorService _diskExecutor;

    /**
     * Manages connection timeouts.
     */
    private final ScheduledExecutorService _timeoutScheduler;

    /**
     * Number of threads accepting connections.
     */
    private final int _socketThreads;

    /**
     * Shared Netty server channel
     */
    private Channel _serverChannel;

    /**
     * Socket address of the last server channel created.
     */
    private InetSocketAddress _lastServerAddress;

    /**
     * Netty channel factory.
     */
    private ChannelFactory _channelFactory;

    private PortRange _portRange = new PortRange(0);

    private final ConcurrentMap<UUID,Entry> _uuids =
        Maps.newConcurrentMap();
    private final ConcurrentMap<MoverChannel<T>,Entry> _channels =
        Maps.newConcurrentMap();

    /**
     * Switch Netty to slf4j for logging. Should be moved somewhere
     * else.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public AbstractNettyServer(
            String name,
            int threadPoolSize,
            int memoryPerConnection,
            int maxMemory,
            int socketThreads)
    {
        _timeoutScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder().setNameFormat(name + "-connect-timeout").build());
        _diskExecutor =
            new OrderedMemoryAwareThreadPoolExecutor(
                    threadPoolSize, memoryPerConnection, maxMemory, 30, TimeUnit.SECONDS,
                    new ThreadFactoryBuilder().setNameFormat(name + "-disk-%d").build());
        _acceptExecutor =
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(name + "-listen-%d").build());
        _socketExecutor =
                Executors.newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(name + "-net-%d").build());
        _socketThreads = socketThreads;
    }

    /**
     * Start netty server.
     *
     * @throws IOException Starting the server failed
     */
    protected synchronized void startServer() throws IOException {
        if (_serverChannel == null) {
            if (_channelFactory == null) {
                if (_socketThreads == -1) {
                    _channelFactory =
                            new NioServerSocketChannelFactory(_acceptExecutor,
                                    _socketExecutor);
                } else {
                    _channelFactory =
                            new NioServerSocketChannelFactory(_acceptExecutor,
                                    _socketExecutor,
                                    _socketThreads);
                }
            }

            ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
            bootstrap.setOption("child.tcpNoDelay", false);
            bootstrap.setOption("child.keepAlive", true);
            bootstrap.setPipelineFactory(newPipelineFactory());

            _serverChannel = _portRange.bind(bootstrap);
            _lastServerAddress = (InetSocketAddress) _serverChannel.getLocalAddress();
            _logger.debug("Started {} on {}", getClass().getSimpleName(), _lastServerAddress);
        }
    }

    /**
     * Stop netty server.
     */
    protected synchronized void stopServer()
    {
        if (_serverChannel != null) {
            _logger.debug("Stopping {} on {}", getClass().getSimpleName(), _lastServerAddress);
            _serverChannel.close();
            _serverChannel = null;
        }
    }

    public synchronized void shutdown()
    {
        stopServer();
        if (_channelFactory != null) {
            _timeoutScheduler.shutdown();
            _channelFactory.releaseExternalResources();
            _channelFactory = null;
            _acceptExecutor.shutdown();
            _socketExecutor.shutdown();
            _diskExecutor.shutdown();
            try {
                if (_timeoutScheduler.awaitTermination(3, TimeUnit.SECONDS)) {
                    if (_acceptExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                        if (_socketExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                            _diskExecutor.awaitTermination(3, TimeUnit.SECONDS);
                        }
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
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
                    if (_sync.timeout()) {
                        try (CDC ignored = _cdc.restore()) {
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
            if (_sync.close(exception)) {
                try (CDC ignored = _cdc.restore()) {
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
            if (_sync.cancel()) {
                try (CDC ignored = _cdc.restore()) {
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
    protected abstract ChannelPipelineFactory newPipelineFactory();

    protected Executor getDiskExecutor() {
        return _diskExecutor;
    }

    protected Executor getAcceptExecutor() {
        return _acceptExecutor;
    }

    protected Executor getSocketExecutor() {
        return _socketExecutor;
    }

    public PortRange getPortRange() {
        return _portRange;
    }

    public void setPortRange(PortRange portRange) {
        _portRange = portRange;
    }
}
