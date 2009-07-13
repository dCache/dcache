package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class WriteRequest extends AbstractRequestMessage
{
    private final int fhandle;
    private final long offset;
    private final byte[] data;

    public WriteRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_write)
            throw new IllegalArgumentException("doesn't seem to be a kXR_write message");

        fhandle = buffer.getInt(4);
        offset = buffer.getLong(8);

        int dlen = buffer.getInt(20);
        data = new byte[dlen];
        buffer.getBytes(24, data);
    }

    public int getFileHandle()
    {
        return fhandle;
    }

    public long getWriteOffset()
    {
        return offset;
    }

    public int getDataLength()
    {
        return data.length;
    }

    public byte[] getData()
    {
        return data;
    }
}
