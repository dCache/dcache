package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class StatRequest extends AbstractRequestMessage
{
    private final short opts;
    private final String path;

    public StatRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_stat)
            throw new IllegalArgumentException("doesn't seem to be a kXR_stat message");

        opts = buffer.getUnsignedByte(4);

        // look for '?' character, indicating beginning of optional
        // opaque information (see xrootd-protocol spec.)
        int dlen = buffer.getInt(20);
        int end = 24 + dlen;
        int pos = buffer.indexOf(24, end, (byte)0x3f);
        if (pos > -1) {
            path = buffer.toString(24, pos - 24, "ASCII");
        } else {
            path = buffer.toString(24, end - 24, "ASCII");
        }
    }

    public String getPath()
    {
        return path;
    }

    public boolean isVfsSet()
    {
        return (opts & XrootdProtocol.kXR_vfs) == XrootdProtocol.kXR_vfs;
    }

}
