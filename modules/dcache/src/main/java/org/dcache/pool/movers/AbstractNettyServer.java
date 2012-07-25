package org.dcache.pool.movers;

import com.google.common.collect.Maps;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CDC;
import org.dcache.commons.util.NDC;
import org.dcache.util.PortRange;
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

import diskCacheV111.util.TimeoutCacheException;
import diskCacheV111.vehicles.ProtocolInfo;

/**
 * Abstract base class for all netty servers running on the pool
 * dispatching movers. This class provides most methods needed by a
 * pool-side netty mover. Minimally, extending classes need to provide
 * their own channel-pipelines, their port-range and the logic used
 * for starting/stopping the server.
 *
 * @author tzangerl
 *
 */
public abstract class AbstractNettyServer<T extends ProtocolInfo>
{
    private final static Logger _logger =
        LoggerFactory.getLogger(AbstractNettyServer.class);

    /**
     * Shared thread pool accepting TCP connections.
     */
    private final Executor _acceptExecutor;

    /**
     * Shared thread pool performing non-blocking socket IO.
     */
    private final Executor _socketExecutor;

    /**
     * Shared thread pool performing blocking disk IO.
     */
    private final Executor _diskExecutor;

    /**
     * Shared Netty server channel
     */
    private Channel _serverChannel;

    /**
     * Shared Netty channel factory.
     */
    private final ChannelFactory _channelFactory;

    private PortRange _portRange = new PortRange(0);

    private final ConcurrentMap<UUID,Entry<T>> _uuids =
        Maps.newConcurrentMap();
    private final ConcurrentMap<MoverChannel<T>,Entry<T>> _channels =
        Maps.newConcurrentMap();

    /**
     * Switch Netty to slf4j for logging. Should be moved somewhere
     * else.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public AbstractNettyServer(int threadPoolSize,
                           int memoryPerConnection,
                           int maxMemory,
                           int socketThreads) {
        /* The disk executor handles the http request
         * processing. This boils down to reading and writing from
         * disk.
         */
        _diskExecutor =
            new OrderedMemoryAwareThreadPoolExecutor(threadPoolSize,
                                                     memoryPerConnection,
                                                     maxMemory);

        _acceptExecutor = Executors.newCachedThreadPool();
        _socketExecutor = Executors.newCachedThreadPool();

        if (socketThreads == -1) {
            _channelFactory =
                new NioServerSocketChannelFactory(_acceptExecutor,
                                                  _socketExecutor);
        } else {
            _channelFactory =
                new NioServerSocketChannelFactory(_acceptExecutor,
                                                  _socketExecutor,
                                                  socketThreads);
        }
    }

    /**
     * Start netty server.
     *
     * @throws IOException Starting the server failed
     */
    protected synchronized void startServer() throws IOException {
        if (_serverChannel == null) {
            _logger.debug("Starting server.");
            ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
            bootstrap.setOption("child.tcpNoDelay", false);
            bootstrap.setOption("child.keepAlive", true);
            bootstrap.setPipelineFactory(newPipelineFactory());

            NDC ndc = NDC.cloneNdc();
            try {
                NDC.clear();
                _serverChannel = _portRange.bind(bootstrap);
            } finally {
                NDC.set(ndc);
            }
        }
    }

    /**
     * Stop netty server.
     */
    protected synchronized void stopServer() throws IOException {
        if (_serverChannel != null) {
            _logger.debug("Stopping server.");
            _serverChannel.close();
            _serverChannel = null;
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
    protected synchronized void conditionallyStopServer() throws IOException {
        if (_uuids.isEmpty()) {
            stopServer();
        }
    }

    /**
     * @return The address to which the current server channel is bound
     * @throws IOException server is not running
     */
    public synchronized InetSocketAddress getServerAddress() throws IOException {
        if (!isRunning()) {
            throw new IOException("Cannot get server address as server " +
                                  "channel is not bound!");
        }

        return (InetSocketAddress) _serverChannel.getLocalAddress();
    }

    /**
     * The server is running if there is a server channel that is bound and
     * open
     * @return true, if above condition holds, false otherwise
     */
    protected synchronized boolean isRunning() {
        return (_serverChannel != null &&
                _serverChannel.isBound() &&
                _serverChannel.isOpen());
    }

    public UUID register(MoverChannel<T> channel)
        throws IOException
    {
        return register(channel, UUID.randomUUID());
    }

    public UUID register(MoverChannel<T> channel, UUID uuid)
        throws IOException
    {
        Entry<T> entry = new Entry(channel, uuid);

        if (_uuids.putIfAbsent(uuid, entry) != null) {
            throw new IllegalStateException("UUID conflict");
        }
        if (_channels.putIfAbsent(channel, entry) != null) {
            _uuids.remove(uuid);
            throw new IllegalStateException("Mover is already registered");
        }

        conditionallyStartServer();
        return uuid;
    }

    public void unregister(MoverChannel<T> channel)
        throws IOException
    {
        Entry<T> entry = _channels.remove(channel);
        if (entry != null) {
            _uuids.remove(entry.getUUID());
        }

        conditionallyStopServer();
    }

    public void await(MoverChannel<T> channel, long timeout)
            throws TimeoutCacheException, InterruptedException,
                   InvocationTargetException
    {
        Entry<T> entry = _channels.get(channel);
        if (entry == null) {
            throw new IllegalStateException("");
        }
        entry.await(timeout);
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

    private static class Entry<T extends ProtocolInfo>
    {
        private final MoverChannel<T> _channel;
        private final UUID _uuid;
        private int _open;
        private boolean _isExclusive;
        private boolean _isClosed;
        private Exception _exception;

        Entry(MoverChannel<T> channel, UUID uuid) {
            _channel = channel;
            _uuid = uuid;
        }

        UUID getUUID() {
            return _uuid;
        }

        synchronized MoverChannel<T> open(boolean exclusive) {
            if (_isExclusive || _isClosed) {
                return null;
            }
            _isExclusive = exclusive;
            _open++;
            return _channel;
        }

        synchronized void close() {
            close(null);
        }

        synchronized void close(Exception exception) {
            _open--;
            if (exception != null) {
                _exception = exception;
                _isClosed = true;
            } else if (_open <= 0) {
                _isClosed = true;
            }
            notifyAll();
        }

        synchronized void await(long timeout)
                throws InvocationTargetException, InterruptedException,
                       TimeoutCacheException
        {
            try {
                long deadline = System.currentTimeMillis() + timeout;
                while (System.currentTimeMillis() < deadline &&
                       _open == 0 && !_isClosed) {
                    wait(deadline - System.currentTimeMillis());
                }
                if (_open == 0 && !_isClosed) {
                    throw new TimeoutCacheException("No connection from client after " +
                                                    TimeUnit.MILLISECONDS.toSeconds(timeout) +
                                                    " seconds. Giving up.");
                }
                while (!_isClosed) {
                    wait();
                }
                if (_exception != null) {
                    throw new InvocationTargetException(_exception);
                }
            } finally {
                _isClosed = true;
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
