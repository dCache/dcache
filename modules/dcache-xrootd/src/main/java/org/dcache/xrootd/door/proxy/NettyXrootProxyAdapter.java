/*
COPYRIGHT STATUS:
Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
software are sponsored by the U.S. Department of Energy under Contract No.
DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
non-exclusive, royalty-free license to publish or reproduce these documents
and software for U.S. Government purposes.  All documents and software
available from this server are protected under the U.S. and Foreign
Copyright Laws, and FNAL reserves all rights.

Distribution of the software available from this server is free of
charge subject to the user following the terms of the Fermitools
Software Legal Information.

Redistribution and/or modification of the software shall be accompanied
by the Fermitools Software Legal Information  (including the copyright
notice).

The user is asked to feed back problems, benefits, and/or suggestions
about the software to the Fermilab Software Providers.

Neither the name of Fermilab, the  URA, nor the names of the contributors
may be used to endorse or promote products derived from this software
without specific prior written permission.

DISCLAIMER OF LIABILITY (BSD):

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.

Liabilities of the Government:

This software is provided by URA, independent from its Prime Contract
with the U.S. Department of Energy. URA is acting independently from
the Government and in its own private capacity and is not acting on
behalf of the U.S. Government, nor as its contractor nor its agent.
Correspondingly, it is understood and agreed that the U.S. Government
has no connection to this software and in no manner whatsoever shall
be liable for nor assume any responsibility or obligation for any claim,
cost, or damages arising out of or resulting from the use of the software
available from this server.

Export Control:

All documents and software available from this server are subject to U.S.
export control laws.  Anyone downloading information from this server is
obligated to secure any necessary Government licenses before exporting
documents or software obtained from this server.
 */
package org.dcache.xrootd.door.proxy;

import static org.dcache.xrootd.protocol.XrootdProtocol.DATA_SERVER;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.util.NettyPortRange;
import org.dcache.util.NetworkUtils;
import org.dcache.xrootd.OutboundExceptionHandler;
import org.dcache.xrootd.core.XrootdEncoder;
import org.dcache.xrootd.core.XrootdHandshakeHandler;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves as a pool façade/transfer tunnel.
 * <p/>
 * Listens for client connect, and relays all client requests directly to the pool and pool
 * responses back to the client.  The raw bytes are simply passed through without interpretation
 * (for efficiency), relying on the pool request handler to establish validity.  Only the handshake,
 * protocol and login request messages are deserialized in order to handle TLS on the two pipelines
 * separately.
 */
public class NettyXrootProxyAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(NettyXrootProxyAdapter.class);

    /**
     * Used to flush the incoming request or response to the other channel.
     */
    static void writeAndFlushToChannel(Channel channel, Object object) {
        LOGGER.debug("Outbound write on {}: {}.", channel, object);
        channel.writeAndFlush(object, channel.newPromise())
              .addListener(new ChannelFutureListener() {
                  @Override
                  public void operationComplete(ChannelFuture future) {
                      if (!future.isSuccess()) {
                          LOGGER.error("Outbound write on {}: {}", future.channel(),
                                future.cause());
                          future.channel().pipeline().fireExceptionCaught(future.cause());
                      }
                  }
              });
    }

    private final EventLoopGroup acceptGroup;
    private final EventLoopGroup socketGroup;
    private final EventLoopGroup clientGroup;
    private final NettyPortRange portRange;
    private final InetSocketAddress poolAddress;
    private final TLSSessionInfo tlsSessionInfo;
    private final ScheduledExecutorService executorService;
    private final String proxyId;

    private final int responseTimeoutInSeconds;

    private Channel proxyChannel;

    public NettyXrootProxyAdapter(EventLoopGroup acceptGroup, EventLoopGroup socketGroup,
          EventLoopGroup clientGroup, NettyPortRange portRange, InetSocketAddress poolAddress,
          TLSSessionInfo tlsSessionInfo, int responseTimeoutInSeconds,
          ScheduledExecutorService executorService) {
        this.acceptGroup = acceptGroup;
        this.socketGroup = socketGroup;
        this.clientGroup = clientGroup;
        this.portRange = portRange;
        this.poolAddress = poolAddress;
        this.tlsSessionInfo = tlsSessionInfo;
        this.responseTimeoutInSeconds = responseTimeoutInSeconds;
        this.executorService = executorService;
        proxyId = UUID.randomUUID().toString();
    }

    public String getProxyId() {
        return proxyId;
    }

    public int getResponseTimeoutInSeconds() {
        return responseTimeoutInSeconds;
    }

    public InetSocketAddress start(InetAddress clientAddress) throws IOException {
        ServerBootstrap bootstrap = new ServerBootstrap()
              .group(acceptGroup, socketGroup)
              .channel(NioServerSocketChannel.class)
              .childOption(ChannelOption.TCP_NODELAY, false)
              .childOption(ChannelOption.SO_KEEPALIVE, false)
              .childHandler(new ChannelInitializer<>() {
                  @Override
                  protected void initChannel(Channel ch) throws Exception {
                      NettyXrootProxyAdapter.this.initChannel(ch);
                  }
              });

        /*
         *  On dual stack deployments like BNL's, it is possible
         *  that only the IPv4 address is exposed to the outside.
         *  In this case, we need to make sure the proxy is reachable.
         */
        InetAddress proxyAddress = NetworkUtils.getLocalAddress(clientAddress);
        proxyChannel = portRange.bind(bootstrap, proxyAddress);
        InetSocketAddress redirectEndpoint = (InetSocketAddress) proxyChannel.localAddress();

        LOGGER.info("Proxy {} started, listening on {}.", proxyId, redirectEndpoint);
        return redirectEndpoint;
    }

    public void shutdown() throws InterruptedException {
        proxyChannel.close().sync();
    }

    private void initChannel(Channel ch) throws Exception {
        ProxyRequestHandler requestHandler = new ProxyRequestHandler(this, clientGroup,
              poolAddress, tlsSessionInfo, executorService);
        ProxyErrorHandler errorHandler = new ProxyErrorHandler(proxyId);
        errorHandler.setRequestHandler(requestHandler);
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast("outerrors", new OutboundExceptionHandler());
        pipeline.addLast("handshake", new XrootdHandshakeHandler(DATA_SERVER));
        pipeline.addLast("sender", new XrootdEncoder());
        pipeline.addLast("decoder", new ProxyRequestDecoder(proxyId, requestHandler));
        pipeline.addLast("receiver", requestHandler);
        pipeline.addLast("errorHandler", errorHandler);
    }


}
