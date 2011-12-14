package org.dcache.xrootd.door;

import java.net.InetSocketAddress;
import java.util.concurrent.Executor;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelFactory;
import static org.jboss.netty.channel.Channels.*;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.core.XrootdAuthorizationHandler;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.plugins.AuthenticationFactory;
import org.dcache.xrootd.plugins.AuthorizationFactory;

import org.dcache.auth.LoginStrategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Netty based xrootd redirector. Could possibly be replaced by pure
 * spring configuration once we move to Netty 3.1.
 */
public class NettyXrootdServer
{
    private static final Logger _log =
        LoggerFactory.getLogger(NettyXrootdServer.class);

    private int _port;
    private int _backlog;
    private Executor _requestExecutor;
    private XrootdDoor _door;
    private AuthenticationFactory _authenticationFactory;
    private AuthorizationFactory _authorizationFactory;
    private ChannelFactory _channelFactory;
    private ConnectionTracker _connectionTracker;
    private LoginStrategy _loginStrategy;
    private LoginStrategy _anonymousLoginStrategy;


    /**
     * Switch Netty to slf4j for logging.
     */
    static
    {
        InternalLoggerFactory.setDefaultFactory(new Slf4JLoggerFactory());
    }

    public int getPort()
    {
        return _port;
    }

    @Required
    public void setPort(int port)
    {
        _port = port;
    }

    @Required
    public void setBacklog(int backlog)
    {
        _backlog = backlog;
    }

    @Required
    public void setRequestExecutor(Executor executor)
    {
        _requestExecutor = executor;
    }

    @Required
    public void setChannelFactory(ChannelFactory channelFactory)
    {
        _channelFactory = channelFactory;
    }

    @Required
    public void setConnectionTracker(ConnectionTracker connectionTracker)
    {
        _connectionTracker = connectionTracker;
    }

    @Required
    public void setDoor(XrootdDoor door)
    {
        _door = door;
    }

    @Required
    public void setLoginStrategy(LoginStrategy loginStrategy)
    {
        _loginStrategy = loginStrategy;
    }

    @Required
    public void setAnonymousLoginStrategy(LoginStrategy loginStrategy)
    {
        _anonymousLoginStrategy = loginStrategy;
    }

    @Required
    public void setAuthenticationFactory(AuthenticationFactory factory)
    {
        _authenticationFactory = factory;
    }

    public AuthenticationFactory getAuthenticationFactory()
    {
        return _authenticationFactory;
    }

    @Required
    public void setAuthorizationFactory(AuthorizationFactory factory)
    {
        _authorizationFactory = factory;
    }

    public AuthorizationFactory getAuthorizationFactory()
    {
        return _authorizationFactory;
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
                    if (_log.isDebugEnabled()) {
                        pipeline.addLast("logger", new LoggingHandler(NettyXrootdServer.class));
                    }
                    pipeline.addLast("handshake", new XrootdHandshakeHandler(XrootdProtocol.LOAD_BALANCER));
                    pipeline.addLast("executor", new ExecutionHandler(_requestExecutor));
                    pipeline.addLast("authenticator", new LoginAuthenticationHandler(_authenticationFactory, _loginStrategy, _anonymousLoginStrategy));
                    pipeline.addLast("authorizer", new XrootdAuthorizationHandler(_authorizationFactory));
                    pipeline.addLast("redirector", new XrootdRedirectHandler(_door));
                    return pipeline;
                }
            });

        bootstrap.bind(new InetSocketAddress(_port));
    }
}