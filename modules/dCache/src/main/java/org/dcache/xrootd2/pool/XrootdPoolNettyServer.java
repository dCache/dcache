package org.dcache.xrootd2.pool;

import static org.jboss.netty.channel.Channels.pipeline;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.util.PortRange;
import org.dcache.xrootd2.core.XrootdDecoder;
import org.dcache.xrootd2.core.XrootdEncoder;
import org.dcache.xrootd2.core.XrootdHandshakeHandler;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pool-netty server tailored to the requirements of the xrootd protocol.
 * @author tzangerl
 *
 */
public class XrootdPoolNettyServer
    extends AbstractNettyServer<XrootdProtocol_3> {

    private final static Logger _logger =
        LoggerFactory.getLogger(XrootdPoolNettyServer.class);

    private static final PortRange DEFAULT_PORTRANGE = new PortRange(20000, 25000);

    public XrootdPoolNettyServer(int threadPoolSize,
                               int memoryPerConnection,
                               int maxMemory,
                               int clientIdleTimeout) {
        this(threadPoolSize,
             memoryPerConnection,
             maxMemory,
             clientIdleTimeout,
             -1);
    }

    public XrootdPoolNettyServer(int threadPoolSize,
                                 int memoryPerConnection,
                                 int maxMemory,
                                 int clientIdleTimeout,
                                 int socketThreads) {
        super(threadPoolSize,
              memoryPerConnection,
              maxMemory,
              clientIdleTimeout,
              socketThreads);
    }

    @Override
    protected ChannelPipelineFactory newPipelineFactory() {
        return new XrootdPoolPipelineFactory();
    }

    /**
     * Uses globus' TCP port range.
     */
    @Override
    protected PortRange getPortRange() {
        String portRange = System.getProperty("org.globus.tcp.port.range");
        PortRange range;
        if (portRange != null) {
            range = PortRange.valueOf(portRange);
        } else {
            range = DEFAULT_PORTRANGE;
        }

        return range;
    }

    /**
     * Shutdown the server if no client connections left and no active movers.
     * Start the server if either it is not yet running and a mover has been
     * started.
     */
    @Override
    protected void toggleServer() throws IOException {
        if (isRunning() &&
            getMoversPerUUID().isEmpty() &&
            getConnectedClients() == 0) {

                stopServer();
                _logger.debug("No movers, no connections, stopping server.");

            } else if (!isRunning() &&
                       !getMoversPerUUID().isEmpty()) {

                _logger.debug("Starting server.");
                startServer();

            }
    }

    private class XrootdPoolPipelineFactory implements ChannelPipelineFactory {
        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = pipeline();

            pipeline.addLast("encoder", new XrootdEncoder());
            pipeline.addLast("decoder", new XrootdDecoder());
            if (_logger.isDebugEnabled()) {
                pipeline.addLast("logger",
                                 new LoggingHandler(XrootdPoolNettyServer.class));
            }
            pipeline.addLast("handshake",
                             new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
            pipeline.addLast("executor", new ExecutionHandler(getDiskExecutor()));
            pipeline.addLast("timeout", new IdleStateHandler(getTimer(),
                                                             0,
                                                             0,
                                                             getClientIdleTimeout(),
                                                             TimeUnit.MILLISECONDS));
            pipeline.addLast("transfer",
                             new XrootdPoolRequestHandler(XrootdPoolNettyServer.this));
            return pipeline;
        }
    }
}
