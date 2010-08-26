package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class OKResponse extends AbstractResponseMessage {

    public OKResponse(int sId) {
        super(sId, XrootdProtocol.kXR_ok, 0);
    }

}
