package org.dcache.xrootd.protocol.messages;
import org.dcache.xrootd.protocol.XrootdProtocol;

public class ReadRequest extends GenericReadRequestMessage {


    public ReadRequest(int[] h, byte[] d) {
        super(h, d);

        if (getRequestID() != XrootdProtocol.kXR_read)
            throw new IllegalArgumentException("doesn't seem to be a kXR_read message");
    }




    //	public String getFileHandle() {
    //
    //		readFromHeader(true);
    //
    //		StringBuffer sb = new StringBuffer();
    //
    //		for (int i = 4; i < 8; i++)
    //			sb.append((char) getUnsignedChar(i));
    //
    //		return sb.toString();
    //	}

    public int getFileHandle() {

        readFromHeader(true);

        return getSignedInt(4);

    }

    public long getReadOffset() {

        readFromHeader(true);

        return getSignedLong(8);
    }

    public int bytesToRead() {

        readFromHeader(true);

        return getSignedInt(16);

    }

    public int NumberOfPreReads() {

        return getSizeOfList();
    }

    public EmbeddedReadRequest[] getPreReadRequestList() {
        return getReadRequestList();
    }

}
