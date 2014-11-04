package org.dcache.xrootd.door;

import com.google.common.io.BaseEncoding;
import com.google.common.net.InetAddresses;
import com.google.common.primitives.Longs;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelEvent;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelUpstreamHandler;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.logging.LoggingHandler;
import org.jboss.netty.logging.InternalLoggerFactory;
import org.jboss.netty.logging.Slf4JLoggerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Executor;

import diskCacheV111.util.FsPath;

import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellInfo;
import dmg.cells.nucleus.CellMessageSender;
import dmg.util.TimebasedCounter;

import org.dcache.commons.util.NDC;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.protocol.XrootdProtocol;

import static org.jboss.netty.channel.Channels.pipeline;

/**
 * Netty based xrootd redirector. Could possibly be replaced by pure
 * spring configuration once we move to Netty 3.1.
 */
public class NettyXrootdServer implements CellMessageSender
{
    private static final Logger _log =
        LoggerFactory.getLogger(NettyXrootdServer.class);

    private static final BaseEncoding SESSION_ENCODING = BaseEncoding.base64().omitPadding();

    private static final TimebasedCounter sessionCounter = new TimebasedCounter();

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
    private String sessionPrefix;

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

    @Override
    public void setCellEndpoint(CellEndpoint endpoint)
    {
        CellInfo info = endpoint.getCellInfo();
        sessionPrefix = "door:" + info.getCellName() + "@" + info.getDomainName() + ":";
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

    public void setUploadPath(File uploadPath)
    {
        this._uploadPath = uploadPath.isAbsolute() ? new FsPath(uploadPath.getPath()) : null;
    }

    public File getUploadPath()
    {
        return (_uploadPath == null) ? null : new File(_uploadPath.toString());
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
                    String session = sessionPrefix + SESSION_ENCODING.encode(Longs.toByteArray(sessionCounter.next()));

                    ChannelPipeline pipeline = pipeline();
                    pipeline.addLast("session-1", new SessionHandler(session));
                    pipeline.addLast("tracker", _connectionTracker);
                    pipeline.addLast("encoder", new XrootdEncoder());
                    pipeline.addLast("decoder", new XrootdDecoder());
                    if (_log.isDebugEnabled()) {
                        pipeline.addLast("logger", new LoggingHandler(NettyXrootdServer.class));
                    }
                    pipeline.addLast("handshake", new XrootdHandshakeHandler(XrootdProtocol.LOAD_BALANCER));
                    pipeline.addLast("executor", new ExecutionHandler(_requestExecutor));
                    pipeline.addLast("session-2", new SessionHandler(session));
                    for (ChannelHandlerFactory factory: _channelHandlerFactories) {
                        pipeline.addLast("plugin:" + factory.getName(), factory.createHandler());
                    }
                    pipeline.addLast("redirector", new XrootdRedirectHandler(_door, _rootPath, _uploadPath));
                    return pipeline;
                }
            });

        bootstrap.bind(new InetSocketAddress(_address, _port));
    }

    private static class SessionHandler implements ChannelUpstreamHandler
    {
        private final String session;

        private SessionHandler(String session)
        {
            this.session = session;
        }

        @Override
        public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception
        {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.sendUpstream(e);
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }
    }
}
