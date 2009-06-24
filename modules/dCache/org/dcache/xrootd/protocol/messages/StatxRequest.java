package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;

public class StatxRequest extends AbstractRequestMessage {

    public StatxRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_statx)
            throw new IllegalArgumentException("doesn't seem to be a kXR_statx message");
    }

    public String[] getPaths() {

        readFromHeader(false);

        return new String(data).split("\n");
    }
}
