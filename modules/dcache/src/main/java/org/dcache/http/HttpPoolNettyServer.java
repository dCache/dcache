package org.dcache.http;

import static org.jboss.netty.channel.Channels.pipeline;



import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.util.PortRange;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpContentCompressor;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used for encapsulating the netty HTTP server that serves client
 * connections to the mover.
 *
 * @author tzangerl
 *
 */
public class HttpPoolNettyServer
    extends AbstractNettyServer<HttpProtocol_2> {

    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolNettyServer.class);

    private final int _maxChunkSize;

    private static final PortRange DEFAULT_PORTRANGE = new PortRange(20000, 25000);

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
        super(threadPoolSize,
              memoryPerConnection,
              maxMemory,
              clientIdleTimeout,
              socketThreads);

        _maxChunkSize = maxChunkSize;
    }

    @Override
    protected ChannelPipelineFactory newPipelineFactory() {
        return new HttpPoolPipelineFactory();
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
        if (isRunning() && getMoversPerUUID().isEmpty()) {

                stopServer();
                _logger.debug("No movers, no connections, stopping server.");

        } else if (!isRunning() && !getMoversPerUUID().isEmpty()) {

                _logger.debug("Starting server.");
                startServer();

        }
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
                             new ExecutionHandler(getDiskExecutor()));
            pipeline.addLast("idle-state-handler",
                             new IdleStateHandler(getTimer(),
                                                  0,
                                                  0,
                                                  getClientIdleTimeout(),
                                                  TimeUnit.MILLISECONDS));
            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
            pipeline.addLast("transfer", new HttpPoolRequestHandler(HttpPoolNettyServer.this));

            return pipeline;
        }

    }
}
