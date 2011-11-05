package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class SyncRequest extends AbstractRequestMessage
{
    private final int fhandle;

    public SyncRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_sync)
            throw new IllegalArgumentException("doesn't seem to be a kXR_sync message");

        fhandle = buffer.getInt(4);
    }

    public int getFileHandle()
    {
        return fhandle;
    }
}
