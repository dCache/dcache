// $Id: DCapOutputByteBuffer.java,v 1.2 2007-05-24 13:51:05 tigran Exp $

package org.dcache.pool.movers ;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;


public class DCapOutputByteBuffer  {

    private final ByteBuffer _buffer;

    public DCapOutputByteBuffer( ByteBuffer buffer ){
        _buffer = buffer ;
    }
    public DCapOutputByteBuffer(int size ){
        _buffer = ByteBuffer.allocate(size) ;
    }
    public ByteBuffer buffer(){ return _buffer ; }
    public void writeACK(int command)
    {
        _buffer.clear();
        _buffer.putInt(12).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(command).
            putInt(0);
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeACK( long location )
    {
        _buffer.clear();
        _buffer.putInt(4+4+4+8).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(DCapConstants.IOCMD_SEEK).
            putInt(0).
            putLong(location) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeACK( long location , long size )
    {
        _buffer.clear();
        _buffer.putInt(4+4+4+8+8).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(DCapConstants.IOCMD_LOCATE).
            putInt(0).
            putLong(location).
            putLong(size);
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeACK( int command , int returnCode , String message)
        throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        DataOutputStream dos = new DataOutputStream(baos) ;
        dos.writeUTF(message) ;
        dos.flush() ;
        dos.close() ;
        byte [] msgBytes = baos.toByteArray() ;
        int len = 4 + 4 + 4 + msgBytes.length ;
        //         len = ( (len-1) / 8 + 1 ) * 8 ;
        _buffer.clear();
        _buffer.putInt(len).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(command).
            putInt(returnCode).
            put(msgBytes,0,msgBytes.length) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeFIN(int command)
    {
        _buffer.clear();
        _buffer.putInt(12).
            putInt(DCapConstants.IOCMD_FIN).
            putInt(command).
            putInt(0);
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeFIN( int command , int returnCode , String message)
        throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream() ;
        DataOutputStream dos = new DataOutputStream(baos) ;
        dos.writeUTF(message) ;
        dos.flush() ;
        dos.close() ;
        byte [] msgBytes = baos.toByteArray() ;
        int len = 4 + 4 + 4 + msgBytes.length ;
        //         len = ( (len-1) / 8 + 1 ) * 8 ;
        _buffer.clear();
        _buffer.putInt(len).
            putInt(DCapConstants.IOCMD_FIN).
            putInt(command).
            putInt(returnCode).
            put(msgBytes,0,msgBytes.length) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeDATA_HEADER()
    {
        _buffer.clear();
        _buffer.putInt(4).
            putInt(DCapConstants.IOCMD_DATA) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeDATA_TRAILER()
    {
        _buffer.clear();
        _buffer.putInt(-1) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeDATA_BLOCK( byte [] data , int offset , int size )
    {

        _buffer.clear();
        _buffer.putInt( size ).
            put( data , offset , size ) ;
        _buffer.limit(_buffer.position()).position(0);
    }
    public void writeEND_OF_BLOCK(){
        _buffer.clear();
        _buffer.putInt( 0 ).
            putInt( -1 ) ;
        _buffer.limit(_buffer.position()).position(0);
    }
}
