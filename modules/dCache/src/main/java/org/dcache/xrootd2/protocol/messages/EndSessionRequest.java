package org.dcache.xrootd2.protocol.messages;

import org.jboss.netty.buffer.ChannelBuffer;

public class EndSessionRequest extends AbstractRequestMessage
{
    public EndSessionRequest(ChannelBuffer buffer)
    {
        super(buffer);
    }
}
