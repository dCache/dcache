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

import static org.dcache.xrootd.protocol.XrootdProtocol.SERVER_RESPONSE_LEN;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_handshake;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_login;
import static org.dcache.xrootd.protocol.XrootdProtocol.kXR_protocol;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelId;
import io.netty.handler.codec.ByteToMessageDecoder;
import java.util.List;
import org.dcache.xrootd.core.XrootdException;
import org.dcache.xrootd.protocol.XrootdProtocol;
import org.dcache.xrootd.tpc.protocol.messages.InboundHandshakeResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundLoginResponse;
import org.dcache.xrootd.tpc.protocol.messages.InboundProtocolResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Adapted from XrootdClientDecoder.   Deserializes the frame only for handshake, protocol and login
 * responses; else, reinserts the frame directly into the pipeline.
 * <p>
 * The special handling of protocol and login is in order to handle TLS correctly.
 */
public class ProxyResponseDecoder extends ByteToMessageDecoder {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyResponseDecoder.class);

    private String proxyId;

    private Integer expectedResponse = kXR_handshake;

    ProxyResponseDecoder(String proxyId) {
        this.proxyId = proxyId;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        LOGGER.debug("ProxyResponseDecoder {} channel active {}.", proxyId,
              ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        ChannelId id = ctx.channel().id();
        int readable = in.readableBytes();

        if (readable < SERVER_RESPONSE_LEN) {
            return;
        }

        int pos = in.readerIndex();
        int headerFrameLength = in.getInt(pos + 4);

        if (headerFrameLength < 0) {
            LOGGER.error(
                  "Decoder {}, channel {}: received illegal frame length in xrootd header: {}."
                        + " Closing channel.", proxyId, id, headerFrameLength);
            ctx.channel().close();
            return;
        }

        int length = SERVER_RESPONSE_LEN + headerFrameLength;

        if (readable < length) {
            return;
        }

        ByteBuf frame = in.readSlice(length);

        LOGGER.debug("ProxyResponseDecoder {}, received response for {}.", proxyId,
              XrootdProtocol.getClientRequest(expectedResponse));

        try {
            switch (expectedResponse) {
                case kXR_handshake:
                    out.add(new InboundHandshakeResponse(frame));
                    break;
                case kXR_protocol:
                    out.add(new InboundProtocolResponse(frame));
                    break;
                case kXR_login:
                    out.add(new InboundLoginResponse(frame));
                    break;
                default:
                    frame.retain();
                    out.add(frame);
                    break;
            }
        } catch (XrootdException e) {
            LOGGER.error("Decoder {}, channel {}: error for request type {}: {}. Closing channel.",
                  proxyId, id, expectedResponse, e.getMessage());
        }
    }

    public void setExpectedResponse(int requestId) {
        this.expectedResponse = requestId;
    }
}
