package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;

public class ProtocolRequest extends AbstractRequestMessage {

    public ProtocolRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_protocol) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_protocol message");
        }
    }

}
