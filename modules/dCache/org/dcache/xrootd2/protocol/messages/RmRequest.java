package org.dcache.xrootd2.protocol.messages;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.dcache.xrootd2.util.ParseException;
import org.jboss.netty.buffer.ChannelBuffer;
import static org.dcache.xrootd2.protocol.XrootdProtocol.*;

public class RmRequest extends AbstractRequestMessage
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

    public String getOpaque()
    {
        return opaque;
    }

    public Map<String,String> getOpaqueMap() throws ParseException
    {
        if (opaque == null) {
            return Collections.emptyMap();
        } else {
            Map<String,String> map = new HashMap<String,String>();
            int tokenStart;
            int tokenEnd = 0;

            while ((tokenStart = opaque.indexOf('&', tokenEnd)) != -1) {
                tokenEnd = opaque.indexOf('&',++tokenStart);

                if (tokenEnd == -1) {
                    tokenEnd = opaque.length();
                }

                int delimiter = opaque.indexOf("=",tokenStart);
                if (delimiter == -1 || delimiter >= tokenEnd) {
                    throw new ParseException("Opaque information is missing "
                          + "value for variable " +
                          opaque.substring(tokenStart, tokenEnd));
                }

                map.put(opaque.substring(tokenStart, delimiter),
                        opaque.substring(delimiter + 1, tokenEnd));
            }

            return map;
        }
    }
}
