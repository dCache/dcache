package org.dcache.xrootd2.protocol.messages;

import org.jboss.netty.buffer.ChannelBuffer;

public class HandshakeRequest extends AbstractRequestMessage
{
    private final byte[] handshake;

    public HandshakeRequest(ChannelBuffer buffer)
    {
        super(buffer);
        handshake = new byte[20];
        buffer.getBytes(0, handshake);
    }

    public byte[] getHandshake()
    {
        return handshake;
    }
}
