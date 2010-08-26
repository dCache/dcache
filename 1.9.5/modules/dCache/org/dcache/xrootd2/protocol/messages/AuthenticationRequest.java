package org.dcache.xrootd2.protocol.messages;

import org.jboss.netty.buffer.ChannelBuffer;

public class AuthenticationRequest extends AbstractRequestMessage
{
    public AuthenticationRequest(ChannelBuffer buffer)
    {
        super(buffer);
    }
}
