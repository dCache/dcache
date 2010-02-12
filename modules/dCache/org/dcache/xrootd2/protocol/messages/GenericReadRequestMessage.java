package org.dcache.xrootd2.protocol.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jboss.netty.buffer.ChannelBuffer;

public abstract class GenericReadRequestMessage extends AbstractRequestMessage
{
    public static class EmbeddedReadRequest
    {
        private final int fh;
        private final int len;
        private final long offs;

        EmbeddedReadRequest(int fh, int len, long offs)
        {
            this.fh = fh;
            this.len = len;
            this.offs = offs;
        }

        public int getFileHandle()
        {
            return fh;
        }

        public int BytesToRead()
        {
            return len;
        }

        public long getOffset()
        {
            return offs;
        }

        public String toString()
        {
            return String.format("(%d,%d,%d)", fh, len, offs);
        }
    }

    private final static Logger _log =
        LoggerFactory.getLogger(GenericReadRequestMessage.class);

    private final int pathid;
    private final EmbeddedReadRequest[] readList;

    public GenericReadRequestMessage(ChannelBuffer buffer)
    {
        super(buffer);

        int alen = buffer.getInt(20);

        if (alen <= 8) {
            pathid = -1;
            readList = new EmbeddedReadRequest[0];
        } else {
            int prefix = 0;
            if (alen % 16 == 0) {
                pathid = -1;
            } else if (alen % 16 != 8) {
                pathid = -1;
                _log.warn("invalid readv request: data doesn't start with 8 byte prefix (pathid)");
            } else {
                pathid = buffer.getUnsignedByte(24);
                prefix = 8;
            }

            int numberOfListEntries = (alen - prefix) / 16;

            readList = new EmbeddedReadRequest[numberOfListEntries];

            for (int i = 0; i < numberOfListEntries; i++) {
                int j = 24 + prefix + i * 16;
                readList[i] = new EmbeddedReadRequest(buffer.getInt(j),
                                                      buffer.getInt(j + 4),
                                                      buffer.getLong(j + 8));
            }
        }
    }

    public int getPathID()
    {
        return pathid;
    }

    protected int getSizeOfList()
    {
        return readList.length;
    }

    protected EmbeddedReadRequest[] getReadRequestList()
    {
        return readList;
    }
}
