package org.dcache.pool.movers ;

import java.nio.ByteBuffer;

import static java.nio.charset.StandardCharsets.UTF_8;

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
        _buffer.flip();
    }
    public void writeACK( long location )
    {
        _buffer.clear();
        _buffer.putInt(4+4+4+8).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(DCapConstants.IOCMD_SEEK).
            putInt(0).
            putLong(location) ;
        _buffer.flip();
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
        _buffer.flip();
    }
    public void writeACK( int command , int returnCode , String message) {
        byte [] msgBytes = message.getBytes(UTF_8);
        int len = 4 + 4 + 4 + 2 + msgBytes.length ;
        //         len = ( (len-1) / 8 + 1 ) * 8 ;
        _buffer.clear();
        _buffer.putInt(len).
            putInt(DCapConstants.IOCMD_ACK).
            putInt(command).
            putInt(returnCode).
            putShort((short)msgBytes.length).
            put(msgBytes,0,msgBytes.length) ;
        _buffer.flip();
    }
    public void writeFIN(int command)
    {
        _buffer.clear();
        _buffer.putInt(12).
            putInt(DCapConstants.IOCMD_FIN).
            putInt(command).
            putInt(0);
        _buffer.flip();
    }
    public void writeFIN( int command , int returnCode , String message) {
        byte [] msgBytes = message.getBytes(UTF_8);
        int len = 4 + 4 + 4 + 2 + msgBytes.length ;

        _buffer.clear();
        _buffer.putInt(len).
            putInt(DCapConstants.IOCMD_FIN).
            putInt(command).
            putInt(returnCode).
            putShort((short)msgBytes.length).
            put(msgBytes,0,msgBytes.length) ;
        _buffer.flip();
    }
    public void writeDATA_HEADER()
    {
        _buffer.clear();
        _buffer.putInt(4).
            putInt(DCapConstants.IOCMD_DATA) ;
        _buffer.flip();
    }
    public void writeDATA_TRAILER()
    {
        _buffer.clear();
        _buffer.putInt(-1) ;
        _buffer.flip();
    }
    public void writeDATA_BLOCK( byte [] data , int offset , int size )
    {

        _buffer.clear();
        _buffer.putInt( size ).
            put( data , offset , size ) ;
        _buffer.flip();
    }
    public void writeEND_OF_BLOCK(){
        _buffer.clear();
        _buffer.putInt( 0 ).
            putInt( -1 ) ;
        _buffer.flip();
    }
}
