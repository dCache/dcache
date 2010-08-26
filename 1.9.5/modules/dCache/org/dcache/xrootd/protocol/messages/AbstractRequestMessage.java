package org.dcache.xrootd.protocol.messages;

import org.dcache.xrootd.protocol.XrootdProtocol;


public abstract class AbstractRequestMessage {

    protected int[] header;
    //	protected int[] data;
    protected byte[] data;

    protected boolean readFromHeader = true;

    public AbstractRequestMessage(int[] h, byte[] d) {

        if (h.length != XrootdProtocol.CLIENT_REQUEST_LEN)
            throw new IllegalArgumentException("expected header length doesn't match received header length");

        header = h;

        readFromHeader(true);

        if (getSignedInt(20) > 0) {
            if (getSignedInt(20) != d.length)
                throw new IllegalArgumentException("expected data length doesn't match received data length");

            data = d;
        } else

            data = new byte[0];
    }

    public int getStreamID() {
        readFromHeader(true);
        return getUnsignedShort(0);
    }

    public int getRequestID() {
        readFromHeader(true);
        return getUnsignedShort(2);
    }

    public int getDataLength() {
        return data.length;
    }

    protected int getUnsignedChar(int i) {
        return 	isReadFromHeader() ? header[i] : data[i] & 0xff;
    }

    protected int getUnsignedShort(int i) {
        return  isReadFromHeader() ? header[i] << 8 | header[i+1] : (data[i] & 0xff) << 8 | (data[i+1] & 0xff);
    }

    protected int getSignedInt(int i) {
        return 	isReadFromHeader() ? header[i] 	<< 24 | header[i+1] << 16 |	header[i+2] << 8  |	header[i+3]
            : (data[i] & 0xff)	<< 24 | (data[i+1] & 0xff)  << 16 |	(data[i+2] & 0xff) << 8  | (data[i+3] & 0xff);
    }

    protected long getSignedLong(int i) {
        return isReadFromHeader() ?	(long) header[i] 	<< 56 | (long) header[i+1] << 48 | (long) header[i+2] << 40 | (long) header[i+3] << 32 | (long) header[i+4] << 24 |	(long) header[i+5] << 16 |	(long) header[i+6] << 8  | header[i+7]
            :  (data[i] & 0xffL) << 56  | (data[i+1] & 0xffL)  << 48 | (data[i+2] & 0xffL) << 40 | (data[i+3] & 0xffL)  << 32 |(data[i+4] & 0xffL) << 24 |  (data[i+5] & 0xffL) << 16 |  (data[i+6] & 0xffL) << 8  |  (data[i+7] & 0xffL);
    }


    protected void readFromHeader(boolean readFromHeader) {
        this.readFromHeader = readFromHeader;
    }

    protected boolean isReadFromHeader() {
        return this.readFromHeader;
    }


    public byte[] getData() {

        //		byte[] field = new byte[data.length];
        //
        //		for (int i = 0; i < field.length; i++)
        //			field[i] = (byte) data[i];
        //
        //		return field;
        return data;
    }
}
