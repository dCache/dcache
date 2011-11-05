package org.dcache.xrootd2.protocol.messages;

import java.util.zip.Adler32;
import java.util.Arrays;

import static org.dcache.xrootd2.protocol.XrootdProtocol.*;
import org.jboss.netty.buffer.ChannelBuffer;

public class PrepareRequest extends AbstractRequestMessage
{
    private final int _options;
    private final int _priority;
    private final String[] _plist;

    public PrepareRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != kXR_prepare)
            throw new IllegalArgumentException("doesn't seem to be a kXR_prepare message");

        _options = buffer.getUnsignedShort(4);
        _priority = buffer.getUnsignedShort(5);

        int plen = buffer.getInt(20);
        int end = 24 + plen;

        _plist = buffer.toString(24, end - 24, "ASCII").split("\n");
    }

    public int getOptions()
    {
        return _options;
    }

    public int getPriority()
    {
        return _priority;
    }

    public String[] getPathList()
    {
        return _plist;
    }

    public boolean isCancel() {
        return (getOptions() & kXR_cancel) == kXR_cancel;
    }

    public boolean isNotify() {
        return (getOptions() & kXR_notify) == kXR_notify;
    }

    public boolean isNoErrors() {
        return (getOptions() & kXR_noerrs) == kXR_noerrs;
    }

    public boolean isStage() {
        return (getOptions() & kXR_stage) == kXR_stage;
    }

    public boolean isWriteMode() {
        return (getOptions() & kXR_wmode) == kXR_wmode;
    }

    public boolean isColocate() {
        return (getOptions() & kXR_coloc) == kXR_coloc;
    }

    public boolean isFresh() {
        return (getOptions() & kXR_fresh) == kXR_fresh;
    }

    public String toString()
    {
        return String.format("prepare[%d,%d,%s]", _options, _priority,
                             Arrays.toString(_plist));
    }
}