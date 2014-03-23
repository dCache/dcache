package org.dcache.xrootd.pool;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.util.PortRange;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.stream.ChunkedResponseWriteHandler;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Pool-netty server tailored to the requirements of the xrootd protocol.
 * @author tzangerl
 *
 */
public class XrootdPoolNettyServer
    extends AbstractNettyServer<XrootdProtocolInfo>
{
    private final static Logger _logger =
        LoggerFactory.getLogger(XrootdPoolNettyServer.class);

    private static final PortRange DEFAULT_PORTRANGE = new PortRange(20000, 25000);

    /**
     * Used to generate channel-idle events for the pool handler
     */
    private final Timer _timer;

    private final long _clientIdleTimeout;
    private final int _maxFrameSize;

    private int _numberClientConnections;
    private List<ChannelHandlerFactory> _plugins;

    public XrootdPoolNettyServer(int threadPoolSize,
                                 int memoryPerConnection,
                                 int maxMemory,
                                 long clientIdleTimeout,
                                 int maxFrameSize,
                                 List<ChannelHandlerFactory> plugins) {
        this(threadPoolSize,
             memoryPerConnection,
             maxMemory,
             clientIdleTimeout,
             maxFrameSize,
             plugins,
             -1);
    }

    public XrootdPoolNettyServer(int threadPoolSize,
                                 int memoryPerConnection,
                                 int maxMemory,
                                 long clientIdleTimeout,
                                 int maxFrameSize,
                                 List<ChannelHandlerFactory> plugins,
                                 int socketThreads) {
        super("xrootd", threadPoolSize, memoryPerConnection, maxMemory, socketThreads);
        _clientIdleTimeout = clientIdleTimeout;
        _maxFrameSize = maxFrameSize;
        _plugins = plugins;
        _timer = new HashedWheelTimer();

        String range = System.getProperty("org.globus.tcp.port.range");
        PortRange portRange =
            (range != null) ? PortRange.valueOf(range) : DEFAULT_PORTRANGE;
        setPortRange(portRange);
    }

    public int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    public void shutdown()
    {
        super.shutdown();
        _timer.stop();
    }

    @Override
    protected ChannelPipelineFactory newPipelineFactory() {
        return new XrootdPoolPipelineFactory();
    }

    /**
     * Only shutdown the server if no client connection left.
     */
    @Override
    protected synchronized void conditionallyStopServer() {
        if (_numberClientConnections == 0) {
            super.conditionallyStopServer();
        }
    }

    public synchronized void clientConnected()
    {
        _numberClientConnections++;
    }

    public synchronized void clientDisconnected() {
        _numberClientConnections--;
        conditionallyStopServer();
    }

    private class XrootdPoolPipelineFactory implements ChannelPipelineFactory {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = pipeline();

            /* The disk executor is an OrderedMemoryAwareThreadPoolExecutor.  It only
             * knows how to estimate the size of ChannelBuffer, so we cannot place
             * decoded messages on the queue.
             */
            pipeline.addLast("executor", new ExecutionHandler(getDiskExecutor()));
            pipeline.addLast("encoder", new XrootdEncoder());
            pipeline.addLast("decoder", new XrootdDecoder());
            if (_logger.isDebugEnabled()) {
                pipeline.addLast("logger",
                                 new LoggingHandler(XrootdPoolNettyServer.class));
            }
            pipeline.addLast("handshake",
                             new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
            for (ChannelHandlerFactory plugin: _plugins) {
                pipeline.addLast("plugin:" + plugin.getName(),
                        plugin.createHandler());
            }
            pipeline.addLast("timeout", new IdleStateHandler(_timer,
                                                             0,
                                                             0,
                                                             _clientIdleTimeout,
                                                             TimeUnit.MILLISECONDS));
            pipeline.addLast("chunkedWriter", new ChunkedResponseWriteHandler());
            pipeline.addLast("transfer",
                             new XrootdPoolRequestHandler(XrootdPoolNettyServer.this));
            return pipeline;
        }
    }
}
