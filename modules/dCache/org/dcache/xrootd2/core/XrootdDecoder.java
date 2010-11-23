package org.dcache.xrootd2.core;

import org.jboss.netty.handler.codec.frame.FrameDecoder;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.buffer.ChannelBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.xrootd2.protocol.messages.*;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

/**
 * A FrameDecoder decoding xrootd frames into AbstractRequestMessage
 * objects.
 *
 * TODO: Implement zero-copy handling of write requests by splitting
 * the request into fragments.
 */
public class XrootdDecoder extends FrameDecoder
{
    private final static Logger _logger =
        LoggerFactory.getLogger(XrootdDecoder.class);

    private boolean gotHandshake = false;

    @Override
    protected Object decode(ChannelHandlerContext ctx, Channel channel,
                            ChannelBuffer buffer)
    {
        int readable = buffer.readableBytes();

        /* The first 20 bytes form a handshake.
         */
        if (!gotHandshake) {
            if (readable < CLIENT_HANDSHAKE_LEN) {
                return null;
            }
            gotHandshake = true;

            return new HandshakeRequest(buffer.readSlice(CLIENT_HANDSHAKE_LEN));
        }

        /* All other requests have a common framing format with a
         * fixed length header.
         */
        if (readable < CLIENT_REQUEST_LEN) {
            return null;
        }

        int pos = buffer.readerIndex();
        int headerFrameLength = buffer.getInt(pos + 20);

        if (headerFrameLength < 0) {
            _logger.error("Received illegal frame length in xrootd header: {}."
                          + " Closing channel.", headerFrameLength);
            channel.close();
            return null;
        }

        int length = CLIENT_REQUEST_LEN + headerFrameLength;

        if (readable < length) {
            return null;
        }

        ChannelBuffer frame = buffer.readBytes(length);
        int requestID = frame.getUnsignedShort(2);

        switch (requestID) {
        case kXR_login:
            return new LoginRequest(frame);
        case kXR_open:
            return new OpenRequest(frame);
        case kXR_stat:
            return new StatRequest(frame);
        case kXR_statx:
            return new StatxRequest(frame);
        case kXR_read:
            return new ReadRequest(frame);
        case kXR_readv:
            return new ReadVRequest(frame);
        case kXR_write:
            return new WriteRequest(frame);
        case kXR_sync:
            return new SyncRequest(frame);
        case kXR_close:
            return new CloseRequest(frame);
        case kXR_protocol:
            return new ProtocolRequest(frame);
        case kXR_rm:
            return new RmRequest(frame);
        case kXR_rmdir:
            return new RmDirRequest(frame);
        case kXR_mkdir:
            return new MkDirRequest(frame);
        case kXR_mv:
            return new MvRequest(frame);
        case kXR_dirlist:
            return new DirListRequest(frame);
        case kXR_auth:
            return new AuthenticationRequest(frame);
        default:
            return new UnknownRequest(frame);
        }
    }
}
