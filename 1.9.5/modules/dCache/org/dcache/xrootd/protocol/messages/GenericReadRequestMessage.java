package org.dcache.xrootd.protocol.messages;

import org.apache.log4j.Logger;

public abstract class GenericReadRequestMessage extends AbstractRequestMessage {

    public class EmbeddedReadRequest {

        private int fh;
        private int len;
        private long offs;

        EmbeddedReadRequest(int fh, int len, long offs) {
            this.fh = fh;
            this.len = len;
            this.offs = offs;
        }

        public int getFileHandle() {

            return fh;
        }

        public int BytesToRead() {

            return len;
        }

        public long getOffset() {

            return offs;
        }

    }

    private final static Logger _log =
        Logger.getLogger(GenericReadRequestMessage.class);

    private EmbeddedReadRequest[] readList;

    public GenericReadRequestMessage(int[] h, byte[] d) {
        super(h, d);
    }

    public int getPathID() {
        if (data.length > 0) {
            return data[0];
        } else
            return -1;
    }

    protected int getSizeOfList() {
        //		return (data.length -8) / 16;
        return (data.length) / 16;

    }

    protected EmbeddedReadRequest[] getReadRequestList() {
        if (readList == null) {

            int numberOfListEntries = getSizeOfList();

            int prefix = 0;
            if (data.length % 16 != 0) {
                if (data.length % 16 != 8) {
                    _log.warn("invalid readv request: data doesn't start with 8 byte prefix (pathid)");
                } else {
                    prefix = 8;
                }
            }

            readList = new EmbeddedReadRequest[numberOfListEntries];

            readFromHeader(false);
            for (int i = 0; i < numberOfListEntries; i++) {
                int j = i * 16 + prefix;
                readList[i] = new EmbeddedReadRequest(getSignedInt(j), getSignedInt(j+4), getSignedLong(j+8));
            }

            return readList;

        } else return readList;
    }

}
