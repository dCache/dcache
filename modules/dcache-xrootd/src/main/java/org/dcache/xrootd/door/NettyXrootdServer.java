package org.dcache.xrootd.door;

import static org.dcache.xrootd.plugins.tls.SSLHandlerFactory.SERVER_TLS;

import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.FsPath;
import dmg.cells.nucleus.CDC;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.util.TimebasedCounter;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerAdapter;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.haproxy.HAProxyMessageDecoder;
import io.netty.handler.logging.LoggingHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.dcache.util.CDCThreadFactory;
import org.dcache.util.NDC;
import org.dcache.xrootd.OutboundExceptionHandler;
import org.dcache.xrootd.core.XrootdAuthenticationHandler;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.core.XrootdSessionHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.tls.SSLHandlerFactory;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.security.SigningPolicy;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.util.ServerProtocolFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

/**
 * Netty based xrootd redirector. Could possibly be replaced by pure spring configuration once we
 * move to Netty 3.1.
 */
public class NettyXrootdServer implements CellIdentityAware {

    private static final Logger _log =
          LoggerFactory.getLogger(NettyXrootdServer.class);

    private static final BaseEncoding SESSION_ENCODING = BaseEncoding.base64().omitPadding();

    private static final TimebasedCounter sessionCounter = new TimebasedCounter();

    private int _port;
    private int _backlog;
    private ExecutorService _requestExecutor;
    private XrootdDoor _door;
    private ConnectionTracker _connectionTracker;
    private List<ChannelHandlerFactory> _channelHandlerFactories;
    private List<ChannelHandlerFactory> _accessLogHandlerFactories;
    private List<ChannelHandlerFactory> _sslHandlerFactories;
    private FsPath _rootPath;
    private InetAddress _address;
    private String sessionPrefix;
    private EventLoopGroup _acceptGroup;
    private EventLoopGroup _socketGroup;
    private EventLoopGroup _proxyGroup;
    private Map<String, String> _queryConfig;
    private Map<String, String> _appIoQueues;
    private CellAddressCore _myAddress;
    private SigningPolicy _signingPolicy;
    private ServerProtocolFlags _serverProtocolFlags;

    private boolean _expectProxyProtocol;

    public int getPort() {
        return _port;
    }

    @Required
    public void setPort(int port) {
        _port = port;
    }

    @Required
    public void setSigningPolicy(SigningPolicy config) {
        _signingPolicy = config;
    }

    @Required
    public void setServerProtocolFlags(ServerProtocolFlags serverProtocolFlags) {
        _serverProtocolFlags = serverProtocolFlags;
    }

    public String getAddress() {
        return (_address == null) ? null : _address.toString();
    }

    public void setAddress(String address) throws UnknownHostException {
        _address = (address == null) ? null : InetAddress.getByName(address);
    }

    public int getBacklog() {
        return _backlog;
    }

    @Required
    public void setBacklog(int backlog) {
        _backlog = backlog;
    }

    @Required
    public void setRequestExecutor(ExecutorService executor) {
        _requestExecutor = executor;
    }

    @Required
    public void setConnectionTracker(ConnectionTracker connectionTracker) {
        _connectionTracker = connectionTracker;
    }

    @Required
    public void setDoor(XrootdDoor door) {
        _door = door;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        _myAddress = address;
    }

    @Required
    public void setChannelHandlerFactories(
          List<ChannelHandlerFactory> channelHandlerFactories) {
        _channelHandlerFactories = channelHandlerFactories;
    }

    @Required
    public void setAccessLogHandlerFactories(
          List<ChannelHandlerFactory> channelHandlerFactories) {
        _accessLogHandlerFactories = channelHandlerFactories;
    }

    @Required
    public void setSSLHandlerFactories(List<ChannelHandlerFactory> channelHandlerFactories) {
        _sslHandlerFactories = channelHandlerFactories;
    }

    /**
     * Sets the root path of the name space exported by this xrootd door.
     */
    @Required
    public void setRootPath(String s) {
        _rootPath = FsPath.create(s);
    }

    public String getRootPath() {
        return Objects.toString(_rootPath, null);
    }

    public Map<String, String> getQueryConfig() {
        return _queryConfig;
    }

    @Required
    public void setQueryConfig(Map<String, String> queryConfig) {
        _queryConfig = queryConfig;
    }

    @Required
    public void setAppIoQueues(Map<String, String> appIoQueues) {
        _appIoQueues = appIoQueues;
    }

    public void setExpectedProxyProtocol(boolean allowProxyProtocol) {
        this._expectProxyProtocol = allowProxyProtocol;
    }

    public boolean getExpectProxyProtocol() {
        return _expectProxyProtocol;
    }

    public void start() {
        sessionPrefix =
              "door:" + _myAddress.getCellName() + "@" + _myAddress.getCellDomainName() + ":";

        _acceptGroup = new NioEventLoopGroup(0, new CDCThreadFactory(
              new ThreadFactoryBuilder().setNameFormat("xrootd-listen-%d").build()));
        _socketGroup = new NioEventLoopGroup(0, new CDCThreadFactory(
              new ThreadFactoryBuilder().setNameFormat("xrootd-net-%d").build()));

        if (_door.isProxied()) {
            _proxyGroup = new NioEventLoopGroup(0, new CDCThreadFactory(
                  new ThreadFactoryBuilder().setNameFormat("xrootd-proxy-client-%d")
                        .build()));
            _door.setProxyGroups(_acceptGroup, _socketGroup, _proxyGroup);
        }

        ServerBootstrap bootstrap = new ServerBootstrap()
              .group(_acceptGroup, _socketGroup)
              .channel(NioServerSocketChannel.class)
              .childOption(ChannelOption.TCP_NODELAY, true)
              .childOption(ChannelOption.SO_KEEPALIVE, true)
              .childHandler(new ChannelInitializer<Channel>() {
                  @Override
                  protected void initChannel(Channel ch) throws Exception {
                      String session = sessionPrefix + SESSION_ENCODING.encode(
                            Longs.toByteArray(sessionCounter.next()));

                      ChannelPipeline pipeline = ch.pipeline();
                      pipeline.addLast("outerrors", new OutboundExceptionHandler());
                      pipeline.addLast("session", new SessionHandler(session));
                      if (_expectProxyProtocol) {
                          pipeline.addLast("haproxy", new HAProxyMessageDecoder());
                      }
                      pipeline.addLast("tracker", _connectionTracker);
                      pipeline.addLast("handshake",
                            new XrootdHandshakeHandler(XrootdProtocol.LOAD_BALANCER));
                      pipeline.addLast("encoder", new XrootdEncoder());
                      pipeline.addLast("decoder", new XrootdDecoder());
                      if (_log.isDebugEnabled()) {
                          pipeline.addLast("logger", new LoggingHandler(NettyXrootdServer.class));
                      }

                      /*
                       *  This needs to precede the other plugins in order for
                       *  the logging to be captured on the arriving requests.
                       */
                      Optional<ChannelHandlerFactory> accessLogHandlerFactory =
                            _accessLogHandlerFactories.stream().findFirst();
                      if (accessLogHandlerFactory.isPresent()) {
                          ChannelHandlerFactory factory = accessLogHandlerFactory.get();
                          pipeline.addLast("plugin:" + factory.getName(),
                                factory.createHandler());
                      }

                      /*
                       *  The TLSSessionInfo needs to be shared between
                       *  the xrootd session handler and the redirect handler.
                       *
                       *  The door only needs one for incoming requests (server).
                       */
                      SSLHandlerFactory sslHandlerFactory
                            = SSLHandlerFactory.getHandlerFactory(SERVER_TLS,
                            _sslHandlerFactories);
                      TLSSessionInfo tlsSessionInfo = new TLSSessionInfo(_serverProtocolFlags);
                      tlsSessionInfo.setServerSslHandlerFactory(sslHandlerFactory);

                      XrootdSessionHandler sessionHandler = new XrootdSessionHandler();
                      /*
                       *  Support security level/signed hash verification or for TLS.
                       */
                      sessionHandler.setTlsSessionInfo(tlsSessionInfo);
                      sessionHandler.setSigningPolicy(_signingPolicy);
                      pipeline.addLast(XrootdSessionHandler.SESSION_HANDLER, sessionHandler);

                      for (ChannelHandlerFactory factory : _channelHandlerFactories) {
                          ChannelHandler handler = factory.createHandler();
                          if (handler instanceof XrootdAuthenticationHandler) {
                              XrootdAuthenticationHandler authn = (XrootdAuthenticationHandler) handler;
                              _log.debug("adding {} to {}.", authn, sessionHandler);
                              /*
                               * Add this handler to the session handler, not the pipeline.
                               * The session handler will select the requested handler from
                               * its internal map and add it to the pipeline just at the time
                               * of the authentication request.
                               */
                              authn.setSessionHandler(sessionHandler);
                              sessionHandler.add(authn);
                          } else {
                              pipeline.addLast("plugin:" + factory.getName(), handler);
                          }
                      }

                      XrootdRedirectHandler handler = new XrootdRedirectHandler(_door, _rootPath,
                            _requestExecutor, _queryConfig, _appIoQueues);
                      handler.setSigningPolicy(_signingPolicy);
                      handler.setTlsSessionInfo(tlsSessionInfo);
                      pipeline.addLast("redirector", handler);
                  }
              });

        bootstrap.bind(new InetSocketAddress(_address, _port));
    }

    public void stop() {
        _acceptGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
        _socketGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);

        if (_proxyGroup != null) {
            _proxyGroup.shutdownGracefully(1, 3, TimeUnit.SECONDS);
        }

        try {
            _acceptGroup.terminationFuture().sync();
            _socketGroup.terminationFuture().sync();
            if (_proxyGroup != null) {
                _proxyGroup.terminationFuture().sync();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static class SessionHandler extends ChannelHandlerAdapter implements
          ChannelInboundHandler {

        private final String session;

        private SessionHandler(String session) {
            this.session = session;
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelRegistered();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelUnregistered();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelActive();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelInactive();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelRead(msg);
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelReadComplete();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireUserEventTriggered(evt);
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireChannelWritabilityChanged();
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            CDC.setSession(session);
            NDC.push(session);
            try {
                ctx.fireExceptionCaught(cause);
            } finally {
                NDC.pop();
                CDC.setSession(null);
            }
        }
    }
}
