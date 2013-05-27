// $Id: VspConnection.java,v 1.4 2001-06-05 21:46:27 cvs Exp $
//
package diskCacheV111.clients.vsp ;

import java.io.IOException;

public interface VspConnection {

    public void sync() throws IOException ;

    public void write( byte [] data , int offset , int size )
           throws IOException ;
    public long read( byte [] data , int offset , int size )
           throws IOException ;
    public long read( long size  , VspDataTransferrable consumer )
           throws IOException ;
    public long seek_and_read( byte [] data , int offset ,
                               long file_offset , int file_whence , int size )
           throws IOException ;

    public void close() throws IOException ;
    public void setIoFinishable( VspIoFinishable callBack ) ;
    public void query() throws IOException ;

    public void seek( long position , int whence )
           throws IOException ;

    public long getPosition() ;
    public long getLength() ;
    public long  getBytesRead() ;
    public void setSynchronous( boolean sync ) ;
}
