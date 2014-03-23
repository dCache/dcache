package org.dcache.http;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.handler.stream.ChunkedWriteHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.HttpProtocolInfo;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.util.PortRange;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Class used for encapsulating the netty HTTP server that serves client
 * connections to the mover.
 *
 * @author tzangerl
 *
 */
public class HttpPoolNettyServer
    extends AbstractNettyServer<HttpProtocolInfo>
{
    private final static Logger _logger =
        LoggerFactory.getLogger(HttpPoolNettyServer.class);

    private final static PortRange DEFAULT_PORTRANGE =
        new PortRange(20000, 25000);

    private final Timer _timer;

    private final long _clientIdleTimeout;

    private final int _chunkSize;

    public HttpPoolNettyServer(int threadPoolSize,
                               int memoryPerConnection,
                               int maxMemory,
                               int chunkSize,
                               long clientIdleTimeout) {
        this(threadPoolSize,
             memoryPerConnection,
             maxMemory,
             chunkSize,
             clientIdleTimeout,
             -1);
    }

    public HttpPoolNettyServer(int threadPoolSize,
                               int memoryPerConnection,
                               int maxMemory,
                               int chunkSize,
                               long clientIdleTimeout,
                               int socketThreads) {
        super("http", threadPoolSize, memoryPerConnection, maxMemory, socketThreads);

        _clientIdleTimeout = clientIdleTimeout;
        _chunkSize = chunkSize;
        _timer = new HashedWheelTimer();

        String range = System.getProperty("org.globus.tcp.port.range");
        PortRange portRange =
            (range != null) ? PortRange.valueOf(range) : DEFAULT_PORTRANGE;
        setPortRange(portRange);
    }

    public void shutdown()
    {
        super.shutdown();
        _timer.stop();
    }

    @Override
    protected ChannelPipelineFactory newPipelineFactory() {
        return new HttpPoolPipelineFactory();
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

            /* The disk executor is an OrderedMemoryAwareThreadPoolExecutor.  It only
             * knows how to estimate the size of ChannelBuffer, so we cannot place
             * decoded messages on the queue.
             */
            pipeline.addLast("executor",
                             new ExecutionHandler(getDiskExecutor()));
            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("encoder", new HttpResponseEncoder());

            if (_logger.isDebugEnabled()) {
                pipeline.addLast("logger", new LoggingHandler(HttpPoolNettyServer.class));
            }
            pipeline.addLast("idle-state-handler",
                             new IdleStateHandler(_timer,
                                                  0,
                                                  0,
                                                  _clientIdleTimeout,
                                                  TimeUnit.MILLISECONDS));
            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
            pipeline.addLast("transfer", new HttpPoolRequestHandler(HttpPoolNettyServer.this, _chunkSize));

            return pipeline;
        }
    }
}
