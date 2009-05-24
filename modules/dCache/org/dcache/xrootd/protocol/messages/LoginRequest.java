package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class LoginRequest extends AbstractRequestMessage {

    public LoginRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_login)
            throw new IllegalArgumentException("doesn't seem to be a kXR_login message");
    }

    public String getUserName() {

        StringBuffer sb = new StringBuffer();

        for (int i = 0; i < 8;i++) {
            if (getUnsignedChar(8+i) == 0)
                break;

            sb.append((char)getUnsignedChar(8+i));
        }
        return sb.toString();

    }

    public boolean supportsAsyn() {
        return (getUnsignedChar(18) & 0x80) == 0x80 ? true : false;
    }

    public int getClientProtocolVersion() {
        return getUnsignedChar(18) & 0x3f;
    }

    public boolean isAdmin() {
        return getUnsignedChar(19) == XrootdProtocol.kXR_useradmin ? true : false;
    }

    public int getPID() {
        return getSignedInt(4);
    }

    public byte[] getToken() {
        return data;
    }

}
