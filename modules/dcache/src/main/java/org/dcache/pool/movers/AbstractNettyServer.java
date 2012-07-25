package org.dcache.pool.movers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import dmg.cells.nucleus.CDC;
import org.dcache.commons.util.NDC;
import org.dcache.util.PortRange;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;

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
public abstract class AbstractNettyServer<T>
{
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
     * Used to generate channel-idle events for the pool handler
     */
    private final Timer _timer;
    private final long _clientIdleTimeout;

    /**
     * Shared Netty server channel
     */
    private Channel _serverChannel;

    /**
     * Shared Netty channel factory.
     */
    private final ChannelFactory _channelFactory;

    private Map<UUID, T> _moversPerUUID =
        new HashMap<UUID, T>();
    private int _numberClientConnections;


    public AbstractNettyServer(int threadPoolSize,
                           int memoryPerConnection,
                           int maxMemory,
                           int clientIdleTimeout,
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



        _clientIdleTimeout = clientIdleTimeout;
        _timer = new HashedWheelTimer();
    }

    /**
     * Start a pool netty server with the embedded channel pipeline
     * @throws IOException Starting the server failed
     * @throws IllegalStateException Server has already been started
     */
    protected synchronized void startServer() throws IOException {

        if (_serverChannel != null) {
            throw new IllegalStateException("Server channel seems to be in " +
                                             "use, refuse to start new one.");
        }

        PortRange range = getPortRange();

        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setPipelineFactory(newPipelineFactory());

        NDC ndc = NDC.cloneNdc();
        try {
            NDC.clear();
            _serverChannel = range.bind(bootstrap);
        } finally {
            NDC.set(ndc);
        }
    }

    /**
     * The server is running if there is a server channel that is bound and
     * open
     * @return true, if above condition holds, false otherwise
     */
    protected synchronized void stopServer() throws IOException {
        if (_serverChannel != null) {
            _serverChannel.close();
            _serverChannel = null;
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

    public T getMover(UUID uuid) {
        return _moversPerUUID.get(uuid);
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

    public synchronized void register(UUID uuid, T mover)
        throws IOException{

        _moversPerUUID.put(uuid, mover);
        toggleServer();
    }

    public synchronized void unregister(UUID uuid) throws IOException {
        _moversPerUUID.remove(uuid);
        toggleServer();
    }

    public synchronized void clientConnected() throws IOException {
        _numberClientConnections++;
        toggleServer();
    }

    public synchronized void clientDisconnected() throws IOException {
        _numberClientConnections--;
        toggleServer();
    }

    public int getConnectedClients() {
        return _numberClientConnections;
    }

    protected Map<UUID, T> getMoversPerUUID() {
        return _moversPerUUID;
    }

    /**
     * Child classes know best about the portrange within which their
     * service should run.
     * @return the portrange within which the service should run.
     */
    protected abstract PortRange getPortRange();

    /**
     * Child classes should decide upon the criteria needed for starting/
     * stopping the server
     */
    protected abstract void toggleServer() throws IOException;

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

    protected long getClientIdleTimeout() {
        return _clientIdleTimeout;
    }

    protected Executor getSocketExecutor() {
        return _socketExecutor;
    }

    protected Timer getTimer() {
        return _timer;
    }
}
