package org.dcache.xrootd2.protocol.messages;

import java.nio.charset.Charset;

import org.jboss.netty.buffer.ChannelBuffer;

public abstract class AbstractRequestMessage
{
    protected final int streamId;
    protected final int requestId;

    protected final static Charset XROOTD_CHARSET = Charset.forName("ASCII");

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
