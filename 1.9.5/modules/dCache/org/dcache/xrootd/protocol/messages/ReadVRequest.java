package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;

public class ReadVRequest extends GenericReadRequestMessage {

    public ReadVRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_readv) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_readv message");
        }
    }

    public int NumberOfReads() {
        return getSizeOfList();
    }

    public EmbeddedReadRequest[] getReadRequestList() {
        return super.getReadRequestList();
    }

}
