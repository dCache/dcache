package org.dcache.xrootd.protocol.messages;

public class UnknownRequest extends AbstractRequestMessage {
    public UnknownRequest(int[] h, byte[] d) {
        super(h, d);
    }
}
