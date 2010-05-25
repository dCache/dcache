package org.dcache.xrootd2.protocol.messages;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import org.jboss.netty.buffer.ChannelBuffer;

public class RmDirRequest extends AbstractRequestMessage {

    private final String path;
    private final String opaque;

    public RmDirRequest(ChannelBuffer buffer) {
        super(buffer);

        if (getRequestID() != kXR_rmdir) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_rmdir message");
        }

        int dlen = buffer.getInt(20);
        int end = 24 + dlen;
        int pos = buffer.indexOf(24, end, (byte)0x3f);
        if (pos > -1) {
            path = buffer.toString(24, pos - 24, "ASCII");
            opaque = buffer.toString(pos + 1, end - (pos + 1), "ASCII");
        } else {
            path = buffer.toString(24, end - 24, "ASCII");
            opaque = null;
        }
    }

    public String getPath()
    {
        return path;
    }

    public String getOpaque() {
        return opaque;
    }
}
