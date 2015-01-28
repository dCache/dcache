package org.dcache.xrootd.pool;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
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

/**
 * Pool-netty server tailored to the requirements of the xrootd protocol.
 * @author tzangerl
 *
 */
public class XrootdPoolNettyServer
    extends AbstractNettyServer<XrootdProtocolInfo>
{
    private static final Logger LOGGER =
        LoggerFactory.getLogger(XrootdPoolNettyServer.class);

    private static final PortRange DEFAULT_PORTRANGE = new PortRange(20000, 25000);

    private final long _clientIdleTimeout;
    private final int _maxFrameSize;

    private int _numberClientConnections;
    private List<ChannelHandlerFactory> _plugins;

    public XrootdPoolNettyServer(int threadPoolSize,
                                 long clientIdleTimeout,
                                 int maxFrameSize,
                                 List<ChannelHandlerFactory> plugins) {
        super("xrootd", threadPoolSize);
        _clientIdleTimeout = clientIdleTimeout;
        _maxFrameSize = maxFrameSize;
        _plugins = plugins;

        String range = System.getProperty("org.globus.tcp.port.range");
        PortRange portRange =
            (range != null) ? PortRange.valueOf(range) : DEFAULT_PORTRANGE;
        setPortRange(portRange);
    }

    public int getMaxFrameSize()
    {
        return _maxFrameSize;
    }

    @Override
    protected ChannelInitializer newChannelInitializer() {
        return new XrootdPoolChannelInitializer();
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

    private class XrootdPoolChannelInitializer extends ChannelInitializer
    {
        @Override
        protected void initChannel(Channel ch) throws Exception
        {
            ChannelPipeline pipeline = ch.pipeline();

            pipeline.addLast("encoder", new XrootdEncoder());
            pipeline.addLast("decoder", new XrootdDecoder());
            if (LOGGER.isDebugEnabled()) {
                pipeline.addLast("logger", new LoggingHandler(XrootdPoolNettyServer.class));
            }
            pipeline.addLast("handshake",
                             new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
            for (ChannelHandlerFactory plugin: _plugins) {
                pipeline.addLast("plugin:" + plugin.getName(),
                        plugin.createHandler());
            }
            pipeline.addLast("timeout", new IdleStateHandler(0,
                                                             0,
                                                             _clientIdleTimeout,
                                                             TimeUnit.MILLISECONDS));
            pipeline.addLast("chunkedWriter", new ChunkedResponseWriteHandler());
            pipeline.addLast("transfer", new XrootdPoolRequestHandler(XrootdPoolNettyServer.this));
        }
    }
}
