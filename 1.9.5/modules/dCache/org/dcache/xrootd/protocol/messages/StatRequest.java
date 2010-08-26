package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class StatRequest extends AbstractRequestMessage {

    public StatRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_stat)
            throw new IllegalArgumentException("doesn't seem to be a kXR_stat message");
    }

    public String getPath() {

        readFromHeader(false);

        StringBuffer sb = new StringBuffer("");

        for (int i = 0; i < data.length; i++)   {

            //			look for '?' character, indicating beginning of optional opaque information (see xrootd-protocol spec.)
            if (data[i] == 0x3f)
                break;

            sb.append((char) getUnsignedChar(i));
        }

        return sb.toString();
    }

    public boolean isVfsSet() {

        readFromHeader(true);

        return (getUnsignedChar(4) & XrootdProtocol.kXR_vfs) == XrootdProtocol.kXR_vfs;
    }

}
