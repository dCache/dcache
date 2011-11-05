package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class StatxRequest extends AbstractRequestMessage
{
    private final String[] paths;
    private final String[] opaques;

    public StatxRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_statx)
            throw new IllegalArgumentException("doesn't seem to be a kXR_statx message");

        int dlen = buffer.getInt(20);
        paths = buffer.toString(24, dlen, XROOTD_CHARSET).split("\n");
        opaques = new String[paths.length];

        for (int i = 0; i < paths.length; i++) {
            String path = paths[i];
            int pos = path.indexOf('?');
            if (pos > -1) {
                paths[i] = path.substring(0, pos);
                opaques[i] = path.substring(pos + 1);
            }
        }
    }

    public String[] getPaths()
    {
        return paths;
    }

    public String[] getOpaques()
    {
        return opaques;
    }
}
