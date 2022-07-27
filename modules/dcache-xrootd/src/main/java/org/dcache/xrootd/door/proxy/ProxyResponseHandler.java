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
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_InvalidRequest;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_REQFENCE;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_handshake;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_login;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_ok;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_protocol;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.channel.ChannelInboundHandlerAdapter;
import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.core.XrootdSessionIdentifier;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.ProtocolRequest;
import org.dcache.xrootd.security.TLSSessionInfo;
import org.dcache.xrootd.security.TLSSessionInfo.ClientTls;
import org.dcache.xrootd.tpc.protocol.messages.InboundHandshakeResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundLoginResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundProtocolResponse;
import org.dcache.xrootd.tpc.protocol.messages.OutboundHandshakeRequest;
import org.dcache.xrootd.tpc.protocol.messages.OutboundLoginRequest;
import org.dcache.xrootd.tpc.protocol.messages.OutboundProtocolRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serves as client to the pool.  Passes on all requests without deserialization except for
 * handshake, protocol and login. Replies are similarly relayed back to the proxy.
 * <p/>
 * For the protocol and login requests, it checks for TLS activation on the connection. This has to
 * be handled independently as TLS requires the host certificate information sent back to the client
 * to be correct.  The SSL handler should (re)encrypt the message before it is sent to the pool.
 * <p/>
 * Thus we have an unfortunate but seemingly unavoidable performance hit in proxying with TLS
 * (encrypt-decrypt from client to proxy; encrypt-decrypt from proxy to pool, and vice versa).
 */
public class ProxyResponseHandler extends ChannelInboundHandlerAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyResponseHandler.class);

    private final String proxyId;
    private final TLSSessionInfo tlsSessionInfo;
    private final int responseTimeout;
    private final ScheduledExecutorService executorService;
    private final ProxyResponseDecoder decoder;

    private ProxyRequestHandler requestHandler;
    private Channel poolChannel;
    private Future timerTask;
    private ProtocolRequest protocolRequest;

    ProxyResponseHandler(NettyXrootProxyAdapter proxyAdapter, ProxyResponseDecoder decoder,
          TLSSessionInfo tlsSessionInfo, ScheduledExecutorService executorService) {
        this.decoder = decoder;
        proxyId = proxyAdapter.getProxyId();
        responseTimeout = proxyAdapter.getResponseTimeoutInSeconds();
        this.tlsSessionInfo = new TLSSessionInfo(tlsSessionInfo);
        this.executorService = executorService;
    }

    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("ProxyResponseHandler {} channel active {}.", proxyId, ctx.channel());
        poolChannel = ctx.channel();
        super.channelActive(ctx);
    }

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        LOGGER.debug("ProxyResponseHandler {} received {} on channel {}.", proxyId, msg,
              ctx.channel());
        stopTimer();
        if (msg instanceof InboundHandshakeResponse) {
            LOGGER.debug("ProxyResponseHandler {}, got handshake response on channel {}.",
                  proxyId, ctx.channel());
            sendProtocolRequest(protocolRequest);
        } else if (msg instanceof InboundProtocolResponse) {
            doOnProtocolResponse(ctx, (InboundProtocolResponse) msg);
        } else if (msg instanceof InboundLoginResponse) {
            doOnLoginResponse(ctx, (InboundLoginResponse) msg);
        } else {
            requestHandler.replyWithRaw(msg);
        }
    }

    void doHandshake(ProtocolRequest request) {
        protocolRequest = request;
        LOGGER.debug("ProxyResponseHandler {}, sending handshake request on channel {}.",
              proxyId, poolChannel);
        decoder.setExpectedResponse(kXR_handshake);
        writeAndFlushToChannel(poolChannel, new OutboundHandshakeRequest());
    }

    void sendLoginRequest(LoginRequest request) {
        try {
            boolean isStarted = tlsSessionInfo.clientTransitionedToTLS(kXR_login,
                  poolChannel.pipeline().firstContext());
            LOGGER.debug("ProxyResponseHandler {}, kXR_login, transitioning client to TLS? {}.",
                  proxyId, isStarted);
        } catch (XrootdException e) {
            poolChannel.pipeline().fireExceptionCaught(e);
            return;
        }

        int streamId = request.getStreamId();
        int pid = request.getPID();
        String uname = request.getUserName();
        String token = request.getToken();
        decoder.setExpectedResponse(kXR_login);
        writeAndFlushToChannel(poolChannel, new OutboundLoginRequest(streamId, pid, uname, token));
        LOGGER.debug(
              "ProxyResponseHandler {}, sent login request on channel {}, stream {}, pid {}, uname {}.",
              proxyId, poolChannel, streamId, pid, uname);
        startTimer();
    }

    void sendProtocolRequest(ProtocolRequest request) {
        int streamId = request.getStreamId();
        int version = request.getVersion();
        int options = request.getOption();
        boolean requiresTls = ClientTls.getMode(version, options) == ClientTls.REQUIRES;
        tlsSessionInfo.createClientSession(requiresTls);
        LOGGER.debug("Proxy {} sendProtocolRequest, client requires TLS ? {}.", proxyId,
              requiresTls);
        int[] flags = tlsSessionInfo.getClientFlags();
        decoder.setExpectedResponse(kXR_protocol);
        writeAndFlushToChannel(poolChannel,
              new OutboundProtocolRequest(request.getStreamId(), flags[0], flags[1], flags[2]));
        LOGGER.debug("Proxy {} sent protocol request on channel {}, stream {}, flags {}.", proxyId,
              poolChannel, streamId, Arrays.asList(flags));
        startTimer();
    }

    void sendRaw(Object msg) {
        decoder.setExpectedResponse(kXR_REQFENCE);
        writeAndFlushToChannel(poolChannel, msg);
        LOGGER.debug("ProxyResponseHandler {} sent {} to channel {}.", proxyId, msg,
              poolChannel);
        startTimer();
    }

    void setRequestHandler(ProxyRequestHandler requestHandler) {
        this.requestHandler = requestHandler;
    }

    void shutDown() throws InterruptedException {
        poolChannel.disconnect();
        poolChannel.close().sync();
        LOGGER.debug("ProxyRequestHandler {}, disconnected and closed channel.", proxyId,
              poolChannel);
    }

    private void doOnLoginResponse(ChannelHandlerContext ctx, InboundLoginResponse response)
          throws XrootdException {
        int status = response.getStatus();
        ChannelId id = ctx.channel().id();
        int streamId = response.getStreamId();
        XrootdSessionIdentifier sessionId = response.getSessionId();
        LOGGER.debug(
              "Proxy {}, login response on {}, channel {}, stream {}, received; sessionId {}, "
                    + "status {}.", proxyId, id, streamId, sessionId, status);

        if (status == kXR_ok) {
            LOGGER.debug(
                  "Proxy {}, login channel {}, stream {}, sessionId {},  notifying request handler.",
                  proxyId, id, streamId, sessionId);
        } else {
            String error = String.format("Login to %s failed: status %d.",
                  ctx.channel().remoteAddress(), status);
            throw new XrootdException(kXR_InvalidRequest, error);
        }

        requestHandler.replyToLoginRequest(response);
    }

    private void doOnProtocolResponse(ChannelHandlerContext ctx, InboundProtocolResponse response)
          throws XrootdException {
        int status = response.getStatus();
        ChannelId id = ctx.channel().id();
        int streamId = response.getStreamId();
        tlsSessionInfo.setSourceServerFlags(response.getFlags());
        LOGGER.debug(
              "Proxy {} protocol response on channel {}, stream {}, received, signing policy {}; "
                    + "tls {}; status {}.", proxyId, id, streamId, response.getSigningPolicy(),
              tlsSessionInfo.getClientTls(), status);
        if (status == kXR_ok) {
            LOGGER.debug(
                  "Proxy {} protocol request, channel {}, stream {}, notifying request handler.",
                  proxyId, id, streamId);
        } else {
            String error = String.format(
                  "Proxy {} protocol request failed with status %d.", proxyId, status);
            throw new XrootdException(kXR_InvalidRequest, error);
        }

        requestHandler.replyToProtocolRequest(response);
    }

    private synchronized void startTimer() {
        stopTimer();

        LOGGER.debug("ProxyResponseHandler {} started timer.", proxyId);
        timerTask = executorService.schedule(() ->
        {
            if (poolChannel.isActive()) {
                poolChannel.pipeline()
                      .fireExceptionCaught(new TimeoutException("No response from server after "
                            + responseTimeout + " seconds."));
            }
        }, responseTimeout, TimeUnit.SECONDS);
    }

    private synchronized void stopTimer() {
        if (timerTask != null) {
            timerTask.cancel(true);
            timerTask = null;
            LOGGER.debug("ProxyResponseHandler {} stopped timer.", proxyId);
        }
    }
}
