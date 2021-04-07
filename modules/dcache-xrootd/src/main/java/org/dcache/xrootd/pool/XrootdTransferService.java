/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2013-2015 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.xrootd.pool;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellPath;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import dmg.util.command.Option;
import io.netty.channel.Channel;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleStateHandler;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;
import org.dcache.pool.movers.NettyMover;
import org.dcache.pool.movers.NettyTransferService;
import org.dcache.util.CDCThreadFactory;
import org.dcache.util.NetworkUtils;
import org.dcache.vehicles.XrootdDoorAdressInfoMessage;
import org.dcache.vehicles.XrootdProtocolInfo;
import org.dcache.xrootd.core.XrootdDecoder;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.plugins.ChannelHandlerFactory;
import org.dcache.xrootd.plugins.tls.SSLHandlerFactory;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.security.SigningPolicy;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.stream.ChunkedResponseWriteHandler;
import org.dcache.xrootd.util.ServerProtocolFlags;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import static org.dcache.xrootd.plugins.tls.SSLHandlerFactory.CLIENT_TLS;
import static org.dcache.xrootd.plugins.tls.SSLHandlerFactory.SERVER_TLS;

/**
 * xrootd transfer service.
 *
 * The transfer service uses a Netty server. The Netty server is started dynamically
 * as soon as any xrootd movers have been executed. The server shuts down once the
 * last xrootd movers terminates.
 *
 * Xrootd movers are registered with the Netty server using a UUID. The UUID is
 * relayed to the door which includes it in the xrootd redirect sent to the client.
 * The redirected client will include the UUID when connecting to the pool and
 * serves as an one-time authorization token and as a means of binding the client
 * request to the correct mover.
 *
 * A transfer is considered to have succeeded if at least one file was opened and
 * all opened files were closed again.
 *
 * Open issues:
 *
 * * Write calls blocked on space allocation may starve read
 *   calls. This is because both are served by the same thread
 *   pool. This should be changed such that separate thread pools are
 *   used (may fix later).
 *
 * * Write messages are currently processed as one big message. If the
 *   client chooses to upload a huge file as a single write message,
 *   then the pool will run out of memory. We can fix this by breaking
 *   a write message into many small blocks. The old mover suffers
 *   from the same problem (may fix later).
 *
 * * At least for vector read, the behaviour when reading beyond the
 *   end of the file is wrong.
 *
 * Additions:  third-party client support for dCache as destination.
 *   Responsible for the management of the loop thread group used by
 *   third-party embedded clients.
 */
public class XrootdTransferService extends NettyTransferService<XrootdProtocolInfo>
                implements CellCommandListener
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(XrootdTransferService.class);

    @Command(name = "xrootd set server response timeout",
                    hint = "time in seconds a server has to reply "
                                    + "to the third-party client",
                    description = "Sets the timeout on the third-party client.  "
                                    + "The default mirrors the aggressive "
                                    + "behavior of the SLAC xrootd "
                                    + "server; see the property ")
    class TimeoutCommand implements Callable<String> {
            @Argument(usage = "Timeout.")
            Long timeout = 2L;

            @Option(name = "unit",
                    usage = "Time unit for the timeout.")
            TimeUnit unit;

            @Override
            public String call() throws Exception {
                tpcServerResponseTimeout = timeout;
                if (unit != null) {
                    tpcServerResponseTimeoutUnit = unit;
                }
                return "Timeout now set to " + getTpcServerResponseTimeoutInSeconds()
                                + " seconds; this affects only future transfers, "
                                + "not those currently running.";
            }
    }

    private int                         maxFrameSize;
    private List<ChannelHandlerFactory> plugins;
    private List<ChannelHandlerFactory> accessLogPlugins;
    private List<ChannelHandlerFactory> tpcClientPlugins;
    private List<ChannelHandlerFactory> sslHandlerFactories;
    private Map<String, String>         queryConfig;
    private NioEventLoopGroup           thirdPartyClientGroup;
    private ScheduledExecutorService    thirdPartyShutdownExecutor;

    private SigningPolicy               signingPolicy;
    private ServerProtocolFlags         serverProtocolFlags;
    private long                        tpcServerResponseTimeout;
    private TimeUnit                    tpcServerResponseTimeoutUnit;
    private Map<String, Timer>          reconnectTimers;
    private long                        readReconnectTimeout;
    private TimeUnit                    readReconnectTimeoutUnit;

    public XrootdTransferService()
    {
        super("xrootd");
    }

    public long getTpcServerResponseTimeoutInSeconds() {
        return tpcServerResponseTimeoutUnit.toSeconds(tpcServerResponseTimeout);
    }

    public NioEventLoopGroup getThirdPartyClientGroup()
    {
        return thirdPartyClientGroup;
    }

    public ScheduledExecutorService getThirdPartyShutdownExecutor()
    {
        return thirdPartyShutdownExecutor;
    }

    @Override
    public synchronized void start() {
        super.start();
        ThreadFactory factory = new ThreadFactoryBuilder()
                        .setNameFormat("xrootd-tpc-client-%d")
                        .build();
        thirdPartyClientGroup = new NioEventLoopGroup(0, new CDCThreadFactory(factory));
        reconnectTimers = new HashMap<>();
    }

    @Required
    public void setAccessLogPlugins(List<ChannelHandlerFactory> plugins)
    {
        this.accessLogPlugins = plugins;
    }

    /**
     * Stop the timer, presumably because the client has reconnected.
     *
     * @param uuid  of the mover (channel)
     */
    public synchronized void cancelReconnectTimerForMover(UUID uuid)
    {
        Timer timer = reconnectTimers.remove(uuid.toString());
        if (timer != null) {
            LOGGER.debug("timer for {} cancelled.", uuid);
            timer.cancel();
        }
    }

    /**
     *  Because IO stall during a read may trigger the xrootd client
     *  to attempt, after a timeout, to reconnect by opening another socket,
     *  we would like not to reject it on the basis of a missing mover.  Thus in the
     *  case that the file descriptor maps to a READ mover channel, we leave the
     *  mover in the map held by the transfer service and we start a timer.
     *  If the client fails to reconnect before expiration, the channel is released.
     *
     *  @param descriptor referencing the mover (channel)
     */
    public synchronized void scheduleReconnectTimerForMover(FileDescriptor descriptor)
    {
        NettyMoverChannel channel = descriptor.getChannel();
        UUID key = channel.getMoverUuid();
        /*
         * Make sure no timer exists associated with this mover.
         * This might happen if both channel inactive and exception caught
         * calls trigger this method in rapid succession.
         */
        cancelReconnectTimerForMover(key);
        Timer timer = new Timer();
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                LOGGER.debug("timer for {} expired, releasing channel.", key);
                channel.releaseAll();
                removeReadReconnectTimer(key);
            }
        };
        reconnectTimers.put(key.toString(), timer);
        timer.schedule(task, readReconnectTimeoutUnit.toMillis(readReconnectTimeout));
    }

    @Required
    public void setReadReconnectTimeout(long readReconnectTimeout)
    {
        this.readReconnectTimeout = readReconnectTimeout;
    }

    @Required
    public void setReadReconnectTimeoutUnit(TimeUnit readReconnectTimeoutUnit)
    {
        this.readReconnectTimeoutUnit = readReconnectTimeoutUnit;
    }

    @Required
    public void setPlugins(List<ChannelHandlerFactory> plugins)
    {
        this.plugins = plugins;
    }

    public List<ChannelHandlerFactory> getPlugins()
    {
        return plugins;
    }

    @Required
    public void setSigningPolicy(SigningPolicy signingPolicy)
    {
        this.signingPolicy = signingPolicy;
    }

    @Required
    public void setServerProtocolFlags(ServerProtocolFlags serverProtocolFlags)
    {
        this.serverProtocolFlags = serverProtocolFlags;
    }

    @Required
    public void setSslHandlerFactories(List<ChannelHandlerFactory> sslHandlerFactories)
    {
        this.sslHandlerFactories = sslHandlerFactories;
    }

    @Resource
    public void setTpcServerResponseTimeout(long timeout)
    {
        this.tpcServerResponseTimeout = timeout;
    }

    @Resource
    public void setTpcServerResponseTimeoutUnit(TimeUnit unit)
    {
        this.tpcServerResponseTimeoutUnit = unit;
    }

    @Required
    public void setThirdPartyShutdownExecutor(
                    ScheduledExecutorService thirdPartyShutdownExecutor)
    {
        this.thirdPartyShutdownExecutor = thirdPartyShutdownExecutor;
    }

    @Required
    public void setTpcClientPlugins(List<ChannelHandlerFactory> tpcClientPlugins)
    {
        this.tpcClientPlugins = tpcClientPlugins;
    }

    public List<ChannelHandlerFactory> getTpcClientPlugins()
    {
        return tpcClientPlugins;
    }

    @Required
    public void setMaxFrameSize(int maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
    }

    public int getMaxFrameSize()
    {
        return maxFrameSize;
    }

    public Map<String, String> getQueryConfig()
    {
        return queryConfig;
    }

    @Required
    public void setQueryConfig(Map<String, String> queryConfig)
    {
        this.queryConfig = queryConfig;
    }

    @Override
    protected UUID createUuid(XrootdProtocolInfo protocolInfo)
    {
        return protocolInfo.getUUID();
    }

    /**
     * Sends our address to the door. Copied from the old xrootd mover.
     */
    @Override
    protected void sendAddressToDoor(NettyMover<XrootdProtocolInfo> mover, int port)
            throws SocketException, CacheException
    {
        XrootdProtocolInfo protocolInfo = mover.getProtocolInfo();
        InetAddress localIP = NetworkUtils.getLocalAddress(protocolInfo.getSocketAddress().getAddress());
        CellPath cellpath = protocolInfo.getXrootdDoorCellPath();
        XrootdDoorAdressInfoMessage doorMsg =
                new XrootdDoorAdressInfoMessage(protocolInfo.getXrootdFileHandle(), new InetSocketAddress(localIP, port));
        doorStub.notify(cellpath, doorMsg);
        LOGGER.debug("sending redirect {} to Xrootd-door {}", localIP, cellpath);
    }

    @Override
    protected void initChannel(Channel ch) throws Exception
    {
        super.initChannel(ch);

        ChannelPipeline pipeline = ch.pipeline();

        pipeline.addLast("handshake",
                         new XrootdHandshakeHandler(XrootdProtocol.DATA_SERVER));
        pipeline.addLast("encoder", new XrootdEncoder());
        pipeline.addLast("decoder", new XrootdDecoder());
        if (LOGGER.isDebugEnabled()) {
            pipeline.addLast("logger", new LoggingHandler());
        }

        /*
         *  This needs to precede the other plugins in order for
         *  the logging to be captured on the arriving requests.
         */
        Optional<ChannelHandlerFactory> accessLogHandlerFactory =
                        accessLogPlugins.stream().findFirst();
        if (accessLogHandlerFactory.isPresent()) {
            ChannelHandlerFactory factory = accessLogHandlerFactory.get();
            pipeline.addLast("plugin:" + factory.getName(),
                             factory.createHandler());
        }

        for (ChannelHandlerFactory plugin: plugins) {
            pipeline.addLast("plugin:" + plugin.getName(),
                             plugin.createHandler());
        }
        pipeline.addLast("timeout", new IdleStateHandler(0,
                                                         0,
                                                         clientIdleTimeout,
                                                         clientIdleTimeoutUnit));
        pipeline.addLast("chunkedWriter", new ChunkedResponseWriteHandler());

        /*
         *  The TLSSessionInfo needs to be shared between
         *  the authentication handler and the redirect handler.
         *
         *  The TLSSessionInfo on the pool should carry inbound and outbound
         *  handlers (server and client) in case of a third-party request.
         */
        TLSSessionInfo tlsSessionInfo = new TLSSessionInfo(serverProtocolFlags);
        SSLHandlerFactory factory
                        = SSLHandlerFactory.getHandlerFactory(SERVER_TLS,
                                                              sslHandlerFactories);
        tlsSessionInfo.setServerSslHandlerFactory(factory);
        factory = SSLHandlerFactory.getHandlerFactory(CLIENT_TLS, sslHandlerFactories);
        tlsSessionInfo.setClientSslHandlerFactory(factory);

        XrootdPoolRequestHandler handler
                        = new XrootdPoolRequestHandler(this,
                                                       maxFrameSize,
                                                       queryConfig);
        handler.setSigningPolicy(signingPolicy);
        handler.setTlsSessionInfo(tlsSessionInfo);
        pipeline.addLast("transfer", handler);
    }

    @Override
    protected void initialiseShutdown()
    {
        super.initialiseShutdown();
        shutdownGracefully(thirdPartyClientGroup);
    }

    private synchronized void removeReadReconnectTimer(UUID key)
    {
        reconnectTimers.remove(key.toString());
    }
}
