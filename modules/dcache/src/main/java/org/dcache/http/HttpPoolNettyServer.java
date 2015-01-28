package org.dcache.http;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.stream.ChunkedWriteHandler;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.HttpProtocolInfo;

import org.dcache.pool.movers.AbstractNettyServer;
import org.dcache.util.PortRange;

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
    private static final Logger LOGGER =
        LoggerFactory.getLogger(HttpPoolNettyServer.class);

    private static final PortRange DEFAULT_PORTRANGE =
        new PortRange(20000, 25000);

    private final long _clientIdleTimeout;

    private final int _chunkSize;

    public HttpPoolNettyServer(int threadPoolSize,
                               int chunkSize,
                               long clientIdleTimeout)
    {
        super("http", threadPoolSize);

        _clientIdleTimeout = clientIdleTimeout;
        _chunkSize = chunkSize;

        String range = System.getProperty("org.globus.tcp.port.range");
        PortRange portRange =
            (range != null) ? PortRange.valueOf(range) : DEFAULT_PORTRANGE;
        setPortRange(portRange);
    }

    public void shutdown()
    {
        super.shutdown();
    }

    @Override
    protected ChannelInitializer newChannelInitializer() {
        return new HttpChannelInitializer();
    }

    /**
     * Factory that creates new server handler.
     *
     * The pipeline can handle HTTP compression and chunked transfers.
     *
     * @author tzangerl
     *
     */
    class HttpChannelInitializer extends ChannelInitializer
    {
        @Override
        protected void initChannel(Channel ch) throws Exception
        {
            ChannelPipeline pipeline = ch.pipeline();

            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("encoder", new HttpResponseEncoder());

            if (LOGGER.isDebugEnabled()) {
                pipeline.addLast("logger", new LoggingHandler(HttpPoolNettyServer.class));
            }
            pipeline.addLast("idle-state-handler",
                             new IdleStateHandler(0,
                                                  0,
                                                  _clientIdleTimeout,
                                                  TimeUnit.MILLISECONDS));
            pipeline.addLast("chunkedWriter", new ChunkedWriteHandler());
            pipeline.addLast("transfer", new HttpPoolRequestHandler(HttpPoolNettyServer.this, _chunkSize));
        }
    }
}
