package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class LoginResponse extends AbstractResponseMessage {

    public LoginResponse(int sId, Object ssId, Object sec) {
        super(sId, XrootdProtocol.kXR_ok, 0);

        //		.. put sessionId and security info
    }
}
