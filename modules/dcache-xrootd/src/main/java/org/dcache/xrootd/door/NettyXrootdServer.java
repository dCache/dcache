package org.dcache.xrootd.door;

import com.google.common.net.InetAddresses;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import diskCacheV111.util.FsPath;

import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.protocol.XrootdProtocol;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.jboss.netty.channel.Channels.pipeline;

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
    private ChannelFactory _channelFactory;
    private ConnectionTracker _connectionTracker;
    private List<ChannelHandlerFactory> _channelHandlerFactories;
    private FsPath _rootPath;
    private FsPath _uploadPath;
    private InetAddress _address;

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

    public String getAddress()
    {
        return (_address == null) ? null : _address.toString();
    }

    public void setAddress(String address) throws UnknownHostException
    {
        _address = (address == null) ? null : InetAddresses.forString(address);
    }

    public int getBacklog()
    {
        return _backlog;
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
    public void setChannelHandlerFactories(
            List<ChannelHandlerFactory> channelHandlerFactories)
    {
        _channelHandlerFactories = channelHandlerFactories;
    }

    /**
     * Sets the root path of the name space exported by this xrootd door.
     */
    @Required
    public void setRootPath(String s)
    {
        _rootPath = new FsPath(s);
    }

    public String getRootPath()
    {
        return Objects.toString(_rootPath, null);
    }

    public void setUploadPath(String uploadPath)
    {
        this._uploadPath = (isNullOrEmpty(uploadPath) ? null : new FsPath(uploadPath));
    }

    public String getUploadPath()
    {
        return Objects.toString(_uploadPath, null);
    }

    public void init()
    {
        ServerBootstrap bootstrap = new ServerBootstrap(_channelFactory);
        bootstrap.setOption("backlog", _backlog);
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("child.keepAlive", true);

        bootstrap.setPipelineFactory(new ChannelPipelineFactory() {
                @Override
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
                    for (ChannelHandlerFactory factory: _channelHandlerFactories) {
                        pipeline.addLast("plugin:" + factory.getName(), factory.createHandler());
                    }
                    pipeline.addLast("redirector", new XrootdRedirectHandler(_door, _rootPath, _uploadPath));
                    return pipeline;
                }
            });

        bootstrap.bind(new InetSocketAddress(_address, _port));
    }
}
