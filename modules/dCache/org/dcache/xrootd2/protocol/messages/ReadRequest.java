package org.dcache.xrootd2.protocol.messages;

import org.dcache.xrootd2.protocol.XrootdProtocol;
import org.jboss.netty.buffer.ChannelBuffer;

public class ReadRequest extends GenericReadRequestMessage
{
    private final int fhandle;
    private final long offset;
    private final int rlen;

    public ReadRequest(ChannelBuffer buffer)
    {
        super(buffer);

        if (getRequestID() != XrootdProtocol.kXR_read)
            throw new IllegalArgumentException("doesn't seem to be a kXR_read message");

        fhandle = buffer.getInt(4);
        offset = buffer.getLong(8);
        rlen = buffer.getInt(16);
    }

    public int getFileHandle()
    {
        return fhandle;
    }

    public long getReadOffset()
    {
        return offset;
    }

    public int bytesToRead()
    {
        return rlen;
    }

    public int NumberOfPreReads()
    {
        return getSizeOfList();
    }

    public EmbeddedReadRequest[] getPreReadRequestList()
    {
        return getReadRequestList();
    }

    @Override
    public String toString()
    {
        return String.format("read[handle=%d,offset=%d,length=%d]",
                             fhandle, offset, rlen);
    }
}
