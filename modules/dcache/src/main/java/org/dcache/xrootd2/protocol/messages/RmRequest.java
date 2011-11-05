package org.dcache.xrootd2.protocol.messages;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import org.jboss.netty.buffer.ChannelBuffer;

public class RmRequest extends AbstractRequestMessage
{
    private final String path;
    private final String opaque;

    public RmRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != kXR_rm) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_rm message");
        }

        int dlen = buffer.getInt(20);
        int end = 24 + dlen;
        int pos = buffer.indexOf(24, end, (byte)0x3f);
        if (pos > -1) {
            path = buffer.toString(24, pos - 24, XROOTD_CHARSET);
            opaque = buffer.toString(pos + 1, end - (pos + 1), XROOTD_CHARSET);
        } else {
            path = buffer.toString(24, end - 24, XROOTD_CHARSET);
            opaque = null;
        }
    }

    public String getPath()
    {
        return path;
    }

    public String getOpaque()
    {
        return opaque;
    }
}
