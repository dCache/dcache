// $Id: DCapDataOutputStream.java,v 1.2 2003-05-27 14:47:34 cvs Exp $

package org.dcache.pool.movers;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class DCapDataOutputStream extends DataOutputStream
{
    public DCapDataOutputStream(OutputStream out){
        super(out);
    }
    public void writeCmdData(byte [] data, int offset, int size) throws IOException {
        writeInt(4);
        writeInt(DCapConstants.IOCMD_DATA);
        writeInt(size);
        write(data, offset, size);
        writeInt(-1);
        flush();
    }
    public void writeCmdSeek(long offset, int whence) throws IOException {
        writeInt(16);
        writeInt(DCapConstants.IOCMD_SEEK);
        writeLong(offset);
        writeInt(whence);
        flush();
    }
    public void writeCmdLocate() throws IOException {
        writeInt(4);
        writeInt(DCapConstants.IOCMD_LOCATE);
        flush();
    }
    public void writeCmdWrite() throws IOException {
        writeInt(4);
        writeInt(DCapConstants.IOCMD_WRITE);
        flush();
    }
    public void writeCmdRead(long size) throws IOException {
        writeInt(12);
        writeInt(DCapConstants.IOCMD_READ);
        writeLong(size);
        flush();
    }
    public void writeCmdSeekAndRead(long offset,
                                     int  whence,
                                     long size) throws IOException {
        writeInt(24);
        writeInt(DCapConstants.IOCMD_SEEK_AND_READ);
        writeLong(offset);
        writeInt(whence);
        writeLong(size);
        flush();
    }
    public void writeCmdClose() throws IOException {
        writeInt(4);
        writeInt(DCapConstants.IOCMD_CLOSE);
        flush();
    }
    public void writeACK(int command) throws IOException {
        writeInt(12);
        writeInt(DCapConstants.IOCMD_ACK);
        writeInt(command);
        writeInt(0);
        flush();
    }
    public void writeACK(int command, int returnCode, String message)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(message);
        dos.flush();
        dos.close();
        byte [] msgBytes = baos.toByteArray();
        int len = 4 + 4 + 4 + msgBytes.length;
        //         len = ((len-1) / 8 + 1) * 8;
        writeInt(len);
        writeInt(DCapConstants.IOCMD_ACK);
        writeInt(command);
        writeInt(returnCode);
        write(msgBytes,0,msgBytes.length);
        flush();
    }
    public void writeACK(long location, long size) throws IOException {
        writeInt(4+4+4+8+8);
        writeInt(DCapConstants.IOCMD_ACK);
        writeInt(DCapConstants.IOCMD_LOCATE);
        writeInt(0);
        writeLong(location);
        writeLong(size);
        flush();
    }
    public void writeACK(long location) throws IOException {
        writeInt(4+4+4+8);
        writeInt(DCapConstants.IOCMD_ACK);
        writeInt(DCapConstants.IOCMD_SEEK);
        writeInt(0);
        writeLong(location);
        flush();
    }
    public void writeFIN(int command) throws IOException {
        writeInt(12);
        writeInt(DCapConstants.IOCMD_FIN);
        writeInt(command);
        writeInt(0);
        flush();
    }
    public void writeFIN(int command, int returnCode, String message)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        dos.writeUTF(message);
        dos.flush();
        dos.close();
        byte [] msgBytes = baos.toByteArray();
        int len = 4 + 4 + 4 + msgBytes.length;
        //         len = ((len-1) / 8 + 1) * 8;
        writeInt(len);
        writeInt(DCapConstants.IOCMD_FIN);
        writeInt(command);
        writeInt(returnCode);
        write(msgBytes,0,msgBytes.length);
        flush();
    }
    public void writeDATA_HEADER() throws IOException {
        writeInt(4);
        writeInt(DCapConstants.IOCMD_DATA);
        flush();
    }
    public void writeDATA_TRAILER() throws IOException {
        writeInt(-1);
        flush();
    }
    public void writeDATA_BLOCK(byte [] data, int offset, int size)
        throws IOException{

        writeInt(size);
        write(data, offset, size);
        flush();
    }
}

