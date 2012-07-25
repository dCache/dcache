// $Id: MoverIoBuffer.java,v 1.1 2003-05-29 17:10:21 cvs Exp $

package org.dcache.pool.movers;

class MoverIoBuffer
{
    private int _sendBufferSize;
    private int _recvBufferSize;
    private int _ioBufferSize;

    MoverIoBuffer(MoverIoBuffer buffer){

        _sendBufferSize = buffer._sendBufferSize;
        _recvBufferSize = buffer._recvBufferSize;
        _ioBufferSize   = buffer._ioBufferSize;
    }
    MoverIoBuffer(int sendBufferSize ,
                  int recvBufferSize ,
                  int ioBufferSize    ){

        _sendBufferSize = sendBufferSize;
        _recvBufferSize = recvBufferSize;
        _ioBufferSize   = ioBufferSize;
    }
    void setBufferSize(int sendBufferSize ,
                       int recvBufferSize ,
                       int ioBufferSize    ){

        _sendBufferSize = sendBufferSize;
        _recvBufferSize = recvBufferSize;
        _ioBufferSize   = ioBufferSize;
    }
    int getSendBufferSize(){ return _sendBufferSize; }
    int getRecvBufferSize(){ return _recvBufferSize; }
    int getIoBufferSize(){   return _ioBufferSize; }

    void setSendBufferSize(int sendBufferSize){
        _sendBufferSize = sendBufferSize;
    }
    void setRecvBufferSize(int recvBufferSize){
        _recvBufferSize = recvBufferSize;
    }
    void setIoBufferSize(int ioBufferSize){
        _ioBufferSize = ioBufferSize;
    }
    public String toString(){
        return "send="+_sendBufferSize+";recv="+_recvBufferSize+";io="+_ioBufferSize;
    }
}
