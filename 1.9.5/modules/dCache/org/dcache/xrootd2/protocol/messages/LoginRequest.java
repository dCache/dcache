package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class LoginRequest extends AbstractRequestMessage
{
    private final String username;
    private final short role;
    private final short capver;
    private final int pid;
    private final String token;

    public LoginRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_login)
            throw new IllegalArgumentException("doesn't seem to be a kXR_login message");

        int pos =
            buffer.indexOf(8, 16, (byte)0); // User name is padded with '\0'
        if (pos > -1) {
            username = buffer.toString(8, pos - 8, "ASCII");
        } else {
            username = buffer.toString(8, 8, "ASCII");
        }

        pid = buffer.getInt(4);
        capver = buffer.getUnsignedByte(18);
        role = buffer.getUnsignedByte(19);

        int tlen = buffer.getInt(20);
        token = buffer.toString(24, tlen, "ASCII");
    }

    public String getUserName()
    {
        return username;
    }

    public boolean supportsAsyn()
    {
        return (capver & 0x80) == 0x80 ? true : false;
    }

    public int getClientProtocolVersion()
    {
        return capver & 0x3f;
    }

    public boolean isAdmin()
    {
        return role == XrootdProtocol.kXR_useradmin ? true : false;
    }

    public int getPID()
    {
        return pid;
    }

    public String getToken()
    {
        return token;
    }

}
