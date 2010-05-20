package org.dcache.xrootd2.protocol.messages;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

import java.util.Map;

import org.dcache.xrootd2.util.ParseException;
import org.jboss.netty.buffer.ChannelBuffer;

public class RmRequest extends AuthorizableRequestMessage
{
    private final String path;
    private final String opaque;

    public RmRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != kXR_rm) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_rm message");
        }

        int dlen = buffer.getInt(20);
        int end = 24 + dlen;
        int pos = buffer.indexOf(24, end, (byte)0x3f);
        if (pos > -1) {
            path = buffer.toString(24, pos - 24, "ASCII");
            opaque = buffer.toString(pos + 1, end - (pos + 1), "ASCII");
        } else {
            path = buffer.toString(24, end - 24, "ASCII");
            opaque = null;
        }
    }

    public String getPath()
    {
        return path;
    }

    @Override
    public String getOpaque()
    {
        return opaque;
    }

    @Override
    public Map<String,String> getOpaqueMap() throws ParseException
    {
       return getOpaqueMap(opaque);
    }
}
