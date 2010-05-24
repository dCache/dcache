package org.dcache.xrootd2.protocol.messages;

import static org.dcache.xrootd2.protocol.XrootdProtocol.kXR_mv;

import org.jboss.netty.buffer.ChannelBuffer;

public class MvRequest extends AbstractRequestMessage
{
    private final String sourcePath;
    private final String targetPath;
    private final String opaque;

    public MvRequest(ChannelBuffer buffer) {
        super(buffer);

        if (getRequestID() != kXR_mv) {
            throw new IllegalArgumentException("doesn't seem to be a kXR_mv message");
        }

        int dlen = buffer.getInt(20);
        int end = 24 + dlen;

        int psep = buffer.indexOf(24, end, (byte)0x20);
        int osep = buffer.indexOf(psep, end, (byte)0x3f);

        if (psep == -1) {
            throw new IllegalArgumentException("kXR_mv needs two paths!");
        }

        if (osep > -1) {
            sourcePath = buffer.toString(24, psep - 24, "ASCII");
            targetPath = buffer.toString(psep+1, osep - (psep + 1), "ASCII");
            opaque = buffer.toString(osep + 1, end - (osep + 1), "ASCII");
        } else {
            sourcePath = buffer.toString(24, psep - 24, "ASCII");
            targetPath = buffer.toString(psep+1, end - (psep + 1), "ASCII");
            opaque = null;
        }
    }

    public String getOpaque() {
        return opaque;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public String getTargetPath() {
        return targetPath;
    }
}
