package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class StatxRequest extends AbstractRequestMessage
{
    private final String[] paths;

    public StatxRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_statx)
            throw new IllegalArgumentException("doesn't seem to be a kXR_statx message");

        int dlen = buffer.getInt(20);
        paths = buffer.toString(24, dlen, "ASCII").split("\n");
    }

    public String[] getPaths()
    {
        return paths;
    }
}
