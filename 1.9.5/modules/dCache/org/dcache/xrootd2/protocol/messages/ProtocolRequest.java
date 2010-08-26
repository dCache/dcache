package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class ProtocolRequest extends AbstractRequestMessage
{
    public ProtocolRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_protocol) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_protocol message");
        }
    }

}
