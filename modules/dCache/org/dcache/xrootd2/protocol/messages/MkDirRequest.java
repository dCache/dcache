package org.dcache.xrootd2.protocol.messages;

import java.util.Map;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;
import org.dcache.xrootd2.util.ParseException;
import org.jboss.netty.buffer.ChannelBuffer;

/**
 * FIXME the mode field is currently unsupported, because the owner of the file
 * can not be determined. Supporting the mode is dependant on implementation of
 * authenticated (GSI) xrootd
 */
public class MkDirRequest extends AuthorizableRequestMessage
{
    private final short options;
    private final int mode;
    private final String path;
    private final String opaque;

    public MkDirRequest(ChannelBuffer buffer) {
        super(buffer);

        if (getRequestID() != kXR_mkdir) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_mkdir message");
        }

        options = buffer.getByte(4);
        mode = buffer.getUnsignedShort(18);

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

    @Override
    public String getOpaque() {
        return opaque;
    }

    @Override
    public Map<String, String> getOpaqueMap() throws ParseException {
        return getOpaqueMap(opaque);
    }

    public String getPath() {
        return path;
    }

    public short getOptions() {
        return options;
    }

    public boolean shouldMkPath() {
        return (getOptions() & kXR_mkpath) == kXR_mkpath;
    }

    public int getMode() {
        return mode;
    }
}
