package org.dcache.http;

import static org.jboss.netty.channel.Channels.pipeline;



import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import org.dcache.util.PortRange;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.execution.OrderedMemoryAwareThreadPoolExecutor;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.Timer;
import org.jboss.netty.util.HashedWheelTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for encapsulating the netty HTTP server that serves client
 * connections to the mover.
 *
 * @author tzangerl
 *
 */
public class HttpPoolNettyServer {
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolNettyServer.class);

    private Map<UUID, HttpProtocol_2> _moversPerUUID =
        new HashMap<UUID, HttpProtocol_2>();

    /** The accept executor is used for accepting TCP
     * connections. An accept task will be submitted per server
     * socket.
     */
    private final Executor _acceptExecutor;

    /** The socket executor handles socket IO. Netty submits a
     * number of workers to this executor and each worker is
     * assigned a share of the connections.
     */
    private final Executor _socketExecutor;

    /** The disk executor handles the Xrootd request
     * processing. This boils down to reading and writing from
     * disk.
     */
    private final Executor _diskExecutor;

    private final ChannelFactory _channelFactory;

    /**
     * Keep alive (idle) timeouts
     */
    private final Timer _timer;

    private final int _maxChunkSize;

    private static Channel _serverChannel;
    /**
     * Timeout for the interval that the server handler will keep waiting for
     * further requests to enable HTTP Keep Alive. Its value is in seconds.
     */
    private final int _clientIdleTimeout;

    private static final int DEFAULT_PORTRANGE_LOWER_BOUND = 20000;
    private static final int DEFAULT_PORTRANGE_UPPER_BOUND = 25000;

    public HttpPoolNettyServer(int threadPoolSize,
                               int memoryPerConnection,
                               int maxMemory,
                               int maxChunkSize,
                               int clientIdleTimeout) {
        this(threadPoolSize,
             memoryPerConnection,
             maxMemory,
             maxChunkSize,
             clientIdleTimeout,
             -1);
    }

    public HttpPoolNettyServer(int threadPoolSize,
                               int memoryPerConnection,
                               int maxMemory,
                               int maxChunkSize,
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

        _maxChunkSize = maxChunkSize;

        _clientIdleTimeout = clientIdleTimeout;
        _timer = new HashedWheelTimer();
    }

    /**
     * Start a pool netty server with the embedded channel pipeline
     * @throws IOException Starting the server failed
     * @throws IllegalStateException Server has already been started
     */
    synchronized void startServer() throws IOException {

        if (_serverChannel != null) {
            throw new IllegalStateException("Server channel seems to be in " +
                                             "use, refuse to start new one.");
        }

        String portRange = System.getProperty("org.globus.tcp.port.range");
        PortRange range;
        if (portRange != null) {
            range = PortRange.valueOf(portRange);
        } else {
            range = new PortRange(DEFAULT_PORTRANGE_LOWER_BOUND,
                                  DEFAULT_PORTRANGE_UPPER_BOUND);
        }

        _logger.info("Binding a new server channel.");
        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("child.tcpNoDelay", false);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.setPipelineFactory(new HttpPoolPipelineFactory());

        _serverChannel = range.bind(bootstrap);
    }

    /**
     * The server is running if there is a server channel that is bound and
     * open
     * @return true, if above condition holds, false otherwise
     */
    synchronized void stopServer() throws IOException {
        if (_serverChannel != null) {
            _serverChannel.close();
            _serverChannel = null;
        }
    }

    /**
     * @return The address to which the current server channel is bound
     * @throws IOException server is not running
     */
    synchronized InetSocketAddress getServerAddress() throws IOException {
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
    synchronized boolean isRunning() {
        return (_serverChannel != null &&
                _serverChannel.isBound() &&
                _serverChannel.isOpen());
    }

    synchronized void register(UUID uuid, HttpProtocol_2 mover)
        throws IOException{

        _moversPerUUID.put(uuid, mover);

        if (!isRunning()) {
            startServer();
        }
    }

    synchronized void unregister(UUID uuid) throws IOException {
        _moversPerUUID.remove(uuid);

        if (_moversPerUUID.isEmpty() && isRunning()) {
            stopServer();
        }
    }

    synchronized HttpProtocol_2 getMover(UUID uuid) {
        return _moversPerUUID.get(uuid);
    }

    /**
     * Factory that creates new server handler.
     *
     * The pipeline can handle HTTP compression and chunked transfers.
     *
     * @author tzangerl
     *
     */
    class HttpPoolPipelineFactory implements ChannelPipelineFactory {

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = pipeline();

            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("compressor", new HttpContentCompressor());
            pipeline.addLast("aggregator", new HttpChunkAggregator(_maxChunkSize));
            pipeline.addLast("encoder", new HttpResponseEncoder());

            if (_logger.isDebugEnabled()) {
                pipeline.addLast("logger",
                                 new LoggingHandler(HttpProtocol_2.class));
            }
            pipeline.addLast("executor",
                             new ExecutionHandler(_diskExecutor));
            pipeline.addLast("idle-state-handler",
                             new IdleStateHandler(_timer, 0, 0, _clientIdleTimeout));
            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
            pipeline.addLast("transfer", new HttpPoolRequestHandler(HttpPoolNettyServer.this));

            return pipeline;
        }

    }
}
