package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public abstract class AbstractRequestMessage
{
    protected final int streamId;
    protected final int requestId;

    public AbstractRequestMessage()
    {
        streamId = 0;
        requestId = 0;
    }

    public AbstractRequestMessage(ChannelBuffer buffer)
    {
        streamId = buffer.getUnsignedShort(0);
        requestId = buffer.getUnsignedShort(2);
    }

    public int getStreamID()
    {
        return streamId;
    }

    public int getRequestID()
    {
        return requestId;
    }
}
