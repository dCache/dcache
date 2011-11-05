package org.dcache.xrootd2.protocol.messages;

import org.jboss.netty.buffer.ChannelBuffer;

public class UnknownRequest extends AbstractRequestMessage
{
    public UnknownRequest(ChannelBuffer buffer)
    {
        super(buffer);
    }

    @Override
    public String toString()
    {
        return String.format("unknown[%d,%d]", streamId, requestId);
    }
}
