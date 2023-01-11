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

import static org.dcache.xrootd.door.proxy.NettyXrootProxyAdapter.writeAndFlushToChannel;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ArgInvalid;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_FSError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_IOError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ServerError;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_login;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_protocol;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.ScheduledExecutorService;
import org.dcache.xrootd.OutboundExceptionHandler;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.messages.ErrorResponse;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.LoginResponse;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.protocol.messages.ProtocolResponse;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.tpc.protocol.messages.InboundLoginResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundProtocolResponse;
import org.dcache.xrootd.util.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves as a pool façade.  Handles all client requests except for protocol and login by passing
 * them through without deserialization to the pool pipeline. Replies are similarly relayed when the
 * pool pipeline handler calls the reply methods.
 * <p/>
 * For the protocol and login requests, it checks for TLS activation on the connection. This has to
 * be handled independently as TLS requires the host certificate information sent back to the client
 * to be correct.  The SSL handler should decrypt the message before it is passed to the pool
 * pipeline.
 * <p/>
 * Thus we have an unfortunate but seemingly unavoidable performance hit in proxying with TLS
 * (encrypt-decrypt from client to proxy; encrypt-decrypt from proxy to pool, and vice versa).
 */
public class ProxyRequestHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyRequestHandler.class);

    private final NettyXrootProxyAdapter adapter;
    private final String proxyId;
    private final ProxyResponseHandler responseHandler;
    private final ProxyErrorHandler errorHandler;
    private final TLSSessionInfo tlsSessionInfo;

    private Channel proxyChannel;
    private ProtocolRequest protocolRequest;
    private LoginRequest loginRequest;

    ProxyRequestHandler(NettyXrootProxyAdapter proxyAdapter, EventLoopGroup clientGroup,
          InetSocketAddress poolAddress, TLSSessionInfo tlsSessionInfo,
          ScheduledExecutorService executorService) throws Exception {
        adapter = proxyAdapter;
        proxyId = proxyAdapter.getProxyId();
        this.tlsSessionInfo = tlsSessionInfo;
        ProxyResponseDecoder decoder = new ProxyResponseDecoder(proxyId);
        responseHandler = new ProxyResponseHandler(proxyAdapter, decoder, tlsSessionInfo,
              executorService);
        errorHandler = new ProxyErrorHandler(proxyId);
        Bootstrap clientBootstrap = new Bootstrap();
        clientBootstrap.group(clientGroup)
              .channel(NioSocketChannel.class)
              .option(ChannelOption.TCP_NODELAY, true)
              .option(ChannelOption.SO_KEEPALIVE, true)
              .handler(new ChannelInitializer<>() {
                  @Override
                  protected void initChannel(Channel ch) {
                      ChannelPipeline pipeline = ch.pipeline();
                      pipeline.addLast("outerrors", new OutboundExceptionHandler());
                      pipeline.addLast("sender", new ProxyOutboundEncoder());
                      pipeline.addLast("decoder", decoder);
                      pipeline.addLast("receiver", responseHandler);
                      pipeline.addLast("errorHandler", errorHandler);
                  }
              });

        Channel poolChannel = clientBootstrap.connect(poolAddress).sync().channel();

        LOGGER.info("Proxy {}, connected to {}, pool channel {}.", proxyId, poolAddress,
              poolChannel);
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        proxyChannel = ctx.channel();
        responseHandler.setRequestHandler(this);
        errorHandler.setRequestHandler(this);
        LOGGER.debug("ProxyRequestHandler {} channel active {}.", proxyId, ctx.channel());
        super.channelActive(ctx);
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOGGER.debug("ProxyRequestHandler {} received {} on channel {}.", proxyId, msg,
              ctx.channel());
        if (msg instanceof ProtocolRequest) {
            doOnProtocolRequest(ctx, (ProtocolRequest) msg);
        } else if (msg instanceof LoginRequest) {
            doOnLoginRequest(ctx, (LoginRequest) msg);
        } else {
            responseHandler.sendRaw(msg);
        }
    }

    void notifyException(Throwable t) {
        String error = t.getMessage();
        int errno;

        if (t instanceof XrootdException) {
            errno = ((XrootdException) t).getError();
        } else if (t instanceof UnknownHostException) {
            error = "Invalid address: " + error;
            errno = kXR_FSError;
        } else if (t instanceof IOException) {
            errno = kXR_IOError;
        } else if (t instanceof ParseException) {
            errno = kXR_ArgInvalid;
        } else {
            errno = kXR_ServerError;
        }

        /*
         *  Even though this error may be downstream from the protocol request,
         *  the xroot code needs the request merely to get the stream id and the session,
         *  which should be the same for this handler instance.
         */
        writeAndFlushToChannel(proxyChannel,
              new ErrorResponse<>(proxyChannel.pipeline().firstContext(), protocolRequest, errno,
                    error));
    }

    void replyToLoginRequest(InboundLoginResponse fromPool) {
        String sec = fromPool.getInfo("unix") == null ? "" : "&P=unix";
        LoginResponse response = new LoginResponse(loginRequest, fromPool.getSessionId(), sec);
        writeAndFlushToChannel(proxyChannel, response);
        LOGGER.debug("ProxyRequestHandler {}, replyToLoginRequest: wrote {} to channel {}.",
              proxyId, response, proxyChannel);
    }

    void replyToProtocolRequest(InboundProtocolResponse fromPool) {
        ProtocolResponse response = new ProtocolResponse(protocolRequest, fromPool.getFlags(),
              fromPool.getSigningPolicy());
        writeAndFlushToChannel(proxyChannel, response);
        LOGGER.debug("ProxyRequestHandler {}, replyToProtocolRequest: wrote {} to channel {}.",
              proxyId, response, proxyChannel);
    }

    void replyWithRaw(Object msg) {
        writeAndFlushToChannel(proxyChannel, msg);
        LOGGER.debug("ProxyRequestHandler {}, replyWithRaw: wrote {} to channel {}.", proxyId, msg,
              proxyChannel);
    }

    void shutdown() throws InterruptedException {
        responseHandler.shutDown();
        adapter.shutdown();
        LOGGER.debug("ProxyRequestHandler {}, shutdown.", proxyId);
    }

    private void doOnLoginRequest(ChannelHandlerContext ctx, LoginRequest msg)
          throws XrootdException {
        loginRequest = msg;

        if (tlsSessionInfo.serverUsesTls()) {
            boolean startedTLS = tlsSessionInfo.serverTransitionedToTLS(kXR_login, ctx);
            LOGGER.debug(
                  "ProxyRequestHandler {}, kXR_login, server has now transitioned to tls? {}.",
                  proxyId, startedTLS);
        }

        LOGGER.debug("ProxyRequestHandler {}, calling responseHandler.sendLoginRequest {}.",
              proxyId, msg);
        responseHandler.sendLoginRequest(msg);
    }

    private void doOnProtocolRequest(ChannelHandlerContext ctx, ProtocolRequest msg)
          throws XrootdException {
        protocolRequest = msg;

        if (tlsSessionInfo == null) {
            throw new XrootdException(kXR_ServerError,
                  "incomplete information on protocol request");
        } else {
            LOGGER.debug("doOnProtocolRequest, version {}, expect {}, option {}.",
                  new Object[]{msg.getVersion(), msg.getExpect(), msg.getOption()});
            tlsSessionInfo.setLocalTlsActivation(msg.getVersion(), msg.getOption(),
                  msg.getExpect());
            if (tlsSessionInfo.serverUsesTls()) {
                boolean isStarted = tlsSessionInfo.serverTransitionedToTLS(kXR_protocol, ctx);
                LOGGER.debug(
                      "ProxyRequestHandler {}, kXR_protocol, server has now transitioned to tls? {}.",
                      proxyId, isStarted);
            }
        }

        LOGGER.debug("ProxyRequestHandler {}, calling responseHandler.doHandshake {}.",
              proxyId, msg);

        /*
         * The response handler will forward the protocol request when
         * the handshake response arrives.
         */
        responseHandler.doHandshake(msg);
    }
}
