package org.dcache.xrootd.protocol.messages;
import java.util.Arrays;

public abstract class AbstractResponseMessage {

    private byte[] header = new byte[8];
    private byte[] data = null;
    private int dataLength;

    private byte[] current = header;
    private int pos = 0;


    public AbstractResponseMessage(int sId, int stat, int length) {
        putUnsignedShort(sId);
        putUnsignedShort(stat);
        putSignedInt(length);

        dataLength = length;

        data = new byte[length];
        writeToData(true);
        resetCurrent();

    }

    public AbstractResponseMessage(int sId, int stat) {
        putUnsignedShort(sId);
        putUnsignedShort(stat);
    }

    protected void writeToData(boolean b) {
        current = b ? data : header;
    }

    protected boolean isDataWritten() {
        return current == data ? true : false;
    }

    protected void resetCurrent() {
        resetCurrent(0);
    }

    protected void resetCurrent(int p) {
        pos = p;
    }

    protected void clearCurrent() {
        Arrays.fill(current, (byte) 0);
        resetCurrent();
    }

    protected void resizeData(int newSize) {

        if (newSize <= data.length)
            throw new IllegalArgumentException("new size of data array must be bigger than the old size");

        byte[] tmp = data;
        data = new byte[newSize];

        System.arraycopy(tmp,0,data,0,tmp.length);

        if (current == tmp)
            current = data;
    }

    //	set whole data part (payload) without any regards to current position
    protected void setData(byte[] field, int dataLength) {
        data = field;
        writeToData(false);
        resetCurrent(4);
        putSignedInt(dataLength);
        this.dataLength = dataLength;

    }

    public int getDataLength() {
        return dataLength;
    }

    //	add data beginning at current postition
    protected void put (byte[] field) {
        System.arraycopy(field,0,data,pos,field.length);
    }

    protected void putUnsignedChar(int c) {
        current[pos++] = (byte) c;
    }

    protected void putUnsignedShort(int s) {
        current[pos++] = (byte) (s >> 8);
        current[pos++] = (byte) s;
    }

    protected void putSignedInt(int i) {
        current[pos++] = (byte) (i >> 24);
        current[pos++] = (byte) (i >> 16);
        current[pos++] = (byte) (i >> 8);
        current[pos++] = (byte) i;
    }

    protected void putSignedLong(long l) {
        current[pos++] = (byte) (l >> 56);
        current[pos++] = (byte) (l >> 48);
        current[pos++] = (byte) (l >> 40);
        current[pos++] = (byte) (l >> 32);
        current[pos++] = (byte) (l >> 24);
        current[pos++] = (byte) (l >> 16);
        current[pos++] = (byte) (l >> 8);
        current[pos++] = (byte) l;
    }

    protected void putLittleEndian(long i) {
        current[pos++] = (byte) i;
        current[pos++] = (byte) (i >> 8);
        current[pos++] = (byte) (i >> 16);
        current[pos++] = (byte) (i >> 24);
    }

    /**
     * Put all characters of a String as unsigned kXR_chars
     * @param s the String representing the char sequence to put
     */
    protected void putCharSequence(String s) {
        byte[] ss = s.getBytes();
        //		for (int i=0;i<ss.length;i++)
        //			putUnsignedChar(ss[i]);
        put(ss);
    }

    public byte[] getHeader() {
        return header;
    }

    public byte[] getData() {
        return data;
    }

    public int getStatus() {
        return header[2] << 8 |	header[3];
    }

    public int getStreamID() {
        return header[0]  << 8 | header[1];
    }
}
