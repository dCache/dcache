package org.dcache.xrootd2.door;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelFactory;
import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.springframework.beans.factory.annotation.Required;

import org.dcache.xrootd2.core.XrootdEncoder;
import org.dcache.xrootd2.core.XrootdDecoder;
import org.dcache.xrootd2.core.XrootdHandshakeHandler;
import org.dcache.xrootd2.core.ConnectionTracker;
import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.dcache.xrootd2.security.AbstractAuthenticationFactory;

/**
 * Netty based xrootd redirector. Could possibly be replaced by pure
 * spring configuration once we move to Netty 3.1.
 */
public class NettyXrootdServer
{
    private int _port;
    private int _backlog;
    private Executor _requestExecutor;
    private XrootdDoor _door;
    private AbstractAuthenticationFactory _authenticationFactory;
    private ChannelFactory _channelFactory;
    private ConnectionTracker _connectionTracker;

    public int getPort()
    {
        return _port;
    }

    public void setPort(int port)
    {
        _port = port;
    }

    public void setBacklog(int backlog)
    {
        _backlog = backlog;
    }

    public void setRequestExecutor(Executor executor)
    {
        _requestExecutor = executor;
    }

    public void setChannelFactory(ChannelFactory channelFactory)
    {
        _channelFactory = channelFactory;
    }

    public void setConnectionTracker(ConnectionTracker connectionTracker)
    {
        _connectionTracker = connectionTracker;
    }

    public void setDoor(XrootdDoor door)
    {
        _door = door;
    }

    @Required
    public void setAuthenticationFactory(AbstractAuthenticationFactory factory) {
        _authenticationFactory = factory;
    }

    public void init()
    {
        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("backlog", _backlog);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                public ChannelPipeline getPipeline()
                {
                    ChannelPipeline pipeline = pipeline();
                    pipeline.addLast("tracker", _connectionTracker);
                    pipeline.addLast("encoder", new XrootdEncoder());
                    pipeline.addLast("decoder", new XrootdDecoder());
                    pipeline.addLast("handshake", new XrootdHandshakeHandler(XrootdProtocol.LOAD_BALANCER));
                    pipeline.addLast("executor", new ExecutionHandler(_requestExecutor));
                    pipeline.addLast("redirector", new XrootdRedirectHandler(_door,
                                                                             _authenticationFactory));
                    return pipeline;
                }
            });

        bootstrap.bind(new InetSocketAddress(_port));
    }
}