/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
*/

// $Id: GFtpProtocol_1.java,v 1.22 2007-08-30 21:10:58 abaranov Exp $


package diskCacheV111.movers ;
import  diskCacheV111.vehicles.* ;
import  diskCacheV111.util.* ;
import  diskCacheV111.doors.*;
import  diskCacheV111.repository.SpaceMonitor;
import  dmg.cells.nucleus.* ;
import  java.io.* ;
import  java.util.* ;
import  java.net.* ;
import  java.security.MessageDigest ;
import diskCacheV111.util.Checksum ;
import  java.security.NoSuchAlgorithmException ;
import diskCacheV111.util.ChecksumFactory;




public class GFtpProtocol_1 implements MoverProtocol, ChecksumMover, DataBlocksRecipient{

    //
    // <init>( CellAdapter cell ) ;
    //
    private CellAdapter  _cell = null ;
    private long _bytesTransferred = 0;
    private long _lastTransferred = System.currentTimeMillis() ;
    private long _transferStarted  = 0 ;
    private long _transferTime     = 0 ;
    private int _bufferSize = 0;
    private long    _spaceUsed       = 0 ;
    private long    _spaceAllocated  = 0 ;
    private static final int INC_SPACE  =  (50*1024*1024) ;
    private int     _allocationSpace = INC_SPACE ;
    private String  _status          = "None" ;
    private boolean _wasChanged      = false ;
    private Ranges _offsetRanges = null;


    EBlockReceiver _eBlockReceiver = null;

    // checksum related variables
    // copied from DCapProtocol_3_nio

    private  MessageDigest _transferMessageDigest = null ;
    private  Checksum  _transferChecksum      = null ;

    public GFtpProtocol_1( CellAdapter cell, int bufferSize ) {
        this(cell);
        _bufferSize = bufferSize;
    }

    public GFtpProtocol_1( CellAdapter cell ) {
        _cell = cell ;
        _cell.say( "GFtpProtocol_1 created" ) ;
        _offsetRanges = new Ranges();
    }

    public void setAttribute(String name, Object attribute) {
    }

    public Object getAttribute(String name) {
        return null;
    }

    private void say( String str ) {
        _cell.say( "(GFtp_1) "+str ) ;
    }

    private void esay( String str ) {
        _cell.esay( "(GFtp_1) "+str ) ;
    }

    private void esay( Throwable t) {
        _cell.esay( t ) ;
    }

    public String toString() {
        return "SU="+_spaceUsed+";SA="+_spaceAllocated+";S="+_status ;
    }

    private RandomAccessFile raDiskFile;

    public void runIO(
                  RandomAccessFile diskFile ,
                  ProtocolInfo protocol ,
                  StorageInfo  storage ,
                  PnfsId       pnfsId  ,
                  SpaceMonitor spaceMonitor ,
                  int          access )

    throws Exception {
        raDiskFile = diskFile;
        _lastTransferred = System.currentTimeMillis() ;
        if( ( access & MoverProtocol.WRITE ) != 0 ) {
            _wasChanged = true ;
            runRemoteToDisk(  protocol , storage , pnfsId.toString() , spaceMonitor );
        } else {
            runDiskToRemote(  protocol , storage , pnfsId.toString()  ) ;
        }
    }

    public void runRemoteToDisk(
        ProtocolInfo protocol ,
        StorageInfo  storage ,
        String       pnfsId,
        SpaceMonitor spaceMonitor )

    throws Exception {

        if( ! ( protocol instanceof GFtpProtocolInfo ) ) {
            throw new CacheException(44,
                       "runRemoteToDisk: received wrong type of protocol info");
        }

        GFtpProtocolInfo ftp = (GFtpProtocolInfo)protocol ;
        String mode = ftp.getMode();

        say("runRemoteToDisk: mode = " + mode);

        if ( mode.equalsIgnoreCase("S") ) {
            recvStream( ftp, storage, pnfsId, spaceMonitor);
        }
        else if ( mode.equalsIgnoreCase("E") ) {
            if (!recvEBlock( ftp, storage, pnfsId, spaceMonitor)) { 
                throw new CacheException("runRemoteToDisk: recvEBlock failed");
            }
        }
    }

    public void runDiskToRemote(
        ProtocolInfo protocol ,
        StorageInfo  storage ,
        String       pnfsId    )

    throws Exception {

        if( ! ( protocol instanceof GFtpProtocolInfo ) ) {
            throw new CacheException(44,
                       "runDiskToRemote: received wrong type of protocol info");
        }
        GFtpProtocolInfo ftp = (GFtpProtocolInfo)protocol ;
        String mode = ftp.getMode();

        if ( mode.equalsIgnoreCase("S") ) {
            sendStream( ftp, storage, pnfsId);
        } else if ( mode.equalsIgnoreCase("E") ) {
            sendEBlock( ftp, storage, pnfsId);
        }
    }

    private void sendStream(
        GFtpProtocolInfo ftp,
        StorageInfo  storage,
        String       pnfsId    )

    throws Exception {

        int    port = ftp.getPort() ;
        String host = ftp.getHost() ;
        // this is the offset and size as specified for
        // ERET in partial retrieve mode
        long prm_offset = ftp.getOffset();
        long prm_size = ftp.getSize();
        long fileSize = storage.getFileSize();
        if(prm_offset < 0)
        {
            String err = "sendStream: invalid prm_offset: " + prm_offset;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        if(prm_size < 0)
        {
            String err = "sendStream: invalide prm_size: " + prm_size;
            esay(err);
            throw new IllegalArgumentException(err);
        }

        if(prm_offset+prm_size > fileSize)
        {
            String err = "sendStream: invalid prm_offset=" + prm_offset +
                         " plus prm_size " + prm_size +
                         " for file of size " + fileSize;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        say("sendStream: connecting to " + host + ":" + port);
        Socket       dataSocket = new Socket( host, port ) ;
        OutputStream ostream    = dataSocket.getOutputStream() ;
        if(ftp.getBufferSize()>0) {
            dataSocket.setReceiveBufferSize(ftp.getBufferSize());
            dataSocket.setSendBufferSize(ftp.getBufferSize());
        }
        say("sendStream: connected to " + host + ":" + port);
        int data_length = 128*1024;
        byte [] data            = new byte[data_length] ;
        say("sendStream: expected filesize = " + fileSize + " bytes");


        int nbytes ;
        _transferStarted  = System.currentTimeMillis() ;
        _bytesTransferred = 0 ;
        SysTimer sysTimer = new SysTimer() ;
        sysTimer.getDifference() ;
        try {
            raDiskFile.seek(prm_offset);
            while ((! Thread.currentThread().isInterrupted()) &&
                    (_bytesTransferred < prm_size))
            {
                if( Thread.currentThread().isInterrupted() )
                {
                    throw new
                    InterruptedException("sendStream: transfer interrupted");
                }
                if(data_length+_bytesTransferred <= prm_size)
                {
                    nbytes = raDiskFile.read(data);
                }
                else
                {
                    nbytes = raDiskFile.read(data, 0,
                                          (int)(prm_size - _bytesTransferred));
                }

                //  say("Transferring "+nbytes+" bytes");
                if( nbytes <= 0 )
                    break;
                ostream.write( data , 0, nbytes );
                _lastTransferred = System.currentTimeMillis() ;
                _bytesTransferred += nbytes;
            }
        }
        finally
        {
            try {
                dataSocket.close() ;
            }
            catch(Exception xe) {
            }
            ftp.setBytesTransferred( _bytesTransferred ) ;
            _transferTime = System.currentTimeMillis() - _transferStarted ;
            ftp.setTransferTime( _transferTime ) ;

            say("sendStream: transfered " + _bytesTransferred + " bytes in " +
                ( _transferTime/1000 ) + " seconds " ) ;
            if( _transferTime > 0 ) {
                double rate =
                    ((double)_bytesTransferred)/((double)_transferTime)/1024.*1000./1024.;
                say("sendStream: transfer rate: " + rate + " MBytes/sec");
            }
            say("sendStream: SysTimer: " + sysTimer.getDifference().toString()) ;
        }
    }

    private void put64(long x, byte [] buf, int offset)
    {
        for( int i = offset + 7; i > offset - 1; i-- )
        {
            buf[i] = (byte)x;
            x >>= 8;
        }
    }

    private void sendEBlock(
        GFtpProtocolInfo protocol ,
        StorageInfo  storage ,
        String       pnfsId    )

    throws Exception
    {

        int    port = protocol.getPort() ;
        String host = protocol.getHost() ;
        long    fileSize        = storage.getFileSize() ;
        // this is the offset and size as specified for
        // ERET in partial retrieve mode
        long prm_offset = protocol.getOffset();
        long prm_size =protocol.getSize();
        if(prm_offset < 0)
        {
            String err = "sendEBlock: invalid prm_offset: " + prm_offset;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        if(prm_size < 0)
        {
            String err = "sendEBlock: invalid prm_offset: " + prm_size;
            esay(err);
            throw new IllegalArgumentException(err);
        }

        if(prm_offset+prm_size > fileSize)
        {
            String err = "sendEBlock: invalid prm_offset=" + prm_offset +
                         " plus prm_size " + prm_size +
                         " for file of size " + fileSize;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        say("sendEBlock: received prm_offset " + prm_offset +
            " prm_size " + prm_size);
        say("sendEBlock: connecting to " + host + ":" + port);
        int parallelStart = protocol.getParallelStart();
        OutputStream[] ostreams = new OutputStream[parallelStart];
        Socket[] sockets = new Socket[parallelStart];
        int data_channel_num = 0;
        Exception lastException=null;
        for( int i = 0; i < parallelStart; i++ )
        {
            try {
                Socket       dataSocket = new Socket( host, port ) ;
                OutputStream ostream    = dataSocket.getOutputStream() ;
                if(_bufferSize>0)
                {
                    dataSocket.setReceiveBufferSize(_bufferSize);
                    dataSocket.setSendBufferSize(_bufferSize);
                }
                sockets[i] = dataSocket;
                ostreams[i] = ostream;
            }
            catch( Exception e ) {
                lastException =e;
                break;
            }
            data_channel_num++;
        }
        if(data_channel_num == 0) {
            String errmsg ="sendEBlock: could not open any data channels";
            esay(errmsg);
            if(lastException != null)
            {
                throw lastException;
            }
            else
            {
                throw new IOException(errmsg);
            }
        }
        say("sendEBlock: connected to " + host + ":" + port);

        int bufsize = 10*1024;
        byte [] data = new byte[bufsize + 17] ;

        say("sendEBlock: expected filesize = " + fileSize + " bytes");

        int nbytes ;
        _transferStarted  = System.currentTimeMillis() ;
        _bytesTransferred = 0 ;
        SysTimer sysTimer = new SysTimer() ;
        sysTimer.getDifference() ;
        try {
            int idc = 0;
            long offset = prm_offset;
            raDiskFile.seek(prm_offset);
            while( ! Thread.currentThread().isInterrupted() &&
                    (_bytesTransferred < prm_size) )
            {
                if( Thread.currentThread().isInterrupted() )
                {
                    throw new InterruptedException("sendEBlock: transfer interrupted");
                }
                if(bufsize+_bytesTransferred <= prm_size)
                {
                    nbytes = raDiskFile.read(data, 17, bufsize);
                }
                else
                {
                   nbytes = raDiskFile.read(data, 17, (int)(prm_size - _bytesTransferred));
                }
                if( nbytes <= 0 )
                {
                    break;
                }
                idc %= data_channel_num;
                OutputStream ostream = ostreams[idc++];
                data[0] = 0;
                put64((long)nbytes, data, 1);
                put64(offset, data, 9);
                ostream.write( data , 0, nbytes + 17);
                //say("sent eblock with offset = "+offset+", byte count = "+ nbytes);
                _bytesTransferred += nbytes;

                offset += nbytes;
               _lastTransferred = System.currentTimeMillis() ;
            }
            // Send EODC & EOD & CLOSE bits
            // if we do not send EOD,
            // then, because of globus cog kit bug the data channel will
            // be considered not to transfer any more data
            // by the cog kit client since this is the zero length block and
            // the client  will not try receive EOD on this channel

            data[0] = 64|8|4;
            put64((long)0, data, 1);
            put64((long)data_channel_num, data, 9);
            ostreams[0].write(data, 0, 17);
            say("sendEBlock: sent EODC(" + data_channel_num +
                ") + EOD on CHANNEL #0");

            for( idc = 1; idc < data_channel_num; idc++ )
            {
                //set EOD & CLOSE bits
                data[0] = 8|4;
                put64((long)0, data, 1);
                put64(offset, data, 9);
                ostreams[idc].write( data , 0, 17);
                say("sendEBlock: sent EOD on CHANNEL #" + idc);
            }
        }
        finally {
            for( int i = 0; i < data_channel_num; i++ )
            {
                try
                {
                    sockets[i].close() ;
                }
                catch(Exception xe)
                {
                    esay("sendEBlock: error closing data socket " +
                         sockets[i] + " : " + xe);
                }
            }
            protocol.setBytesTransferred( _bytesTransferred ) ;
            _transferTime = System.currentTimeMillis() -
                            _transferStarted ;
            protocol.setTransferTime( _transferTime ) ;

            say("sendEBlock: transferred " + _bytesTransferred + " bytes in " +
                 ( _transferTime/1000 ) + " seconds ");
            if( _transferTime > 0 ) {
                double rate =
                    ((double)_bytesTransferred)/((double)_transferTime)/1024.*1000./1024.;
                say("sendEBlock: transfer rate: " + rate + " MBytes/sec");
            }
            say("sendEBlock: SysTimer: " + sysTimer.getDifference().toString());
        }


    }

    public long getBytesTransferred() {
        /** @todo - synchronize with _lastTransferred */
        return
                ( _eBlockReceiver == null  )
                ? _bytesTransferred
                : _eBlockReceiver.getBytesTransferred()  ;
    }
    public long getLastTransferred() {
        /** @todo - synchronize with _bytesTransferred */
        return
                ( _eBlockReceiver == null  )
                ?_lastTransferred
                : _eBlockReceiver.getLastTransferred();
    }

    public long getTransferTime() {
        return _transferTime < 0 ?
               System.currentTimeMillis() - _transferStarted :
               _transferTime  ;
    }

    public boolean recvStream(
                              GFtpProtocolInfo ftp,
                              StorageInfo  storage,
                              String       pnfsId,
                              SpaceMonitor spaceMonitor)
    throws Exception {
        int    port = ftp.getPort() ;
        String host = ftp.getHost() ;
        Socket       dataSocket = new Socket( host, port ) ;
        InputStream  istream    = dataSocket.getInputStream() ;
        say("recvStream: connected to " + host + ":" + port);
        if(ftp.getBufferSize()>0) {
            dataSocket.setReceiveBufferSize(ftp.getBufferSize());
            dataSocket.setSendBufferSize(ftp.getBufferSize());
        }
        byte [] data = new byte[128*1024] ;
        int nbytes ;
        SysTimer sysTimer = new SysTimer() ;
        _transferStarted  = System.currentTimeMillis() ;
        _bytesTransferred = 0 ;
        sysTimer.getDifference() ;
        try {
            while( ! Thread.currentThread().isInterrupted() ) {
                if( Thread.currentThread().isInterrupted() ) {
                    throw new InterruptedException(
                                   "recvStream: transfer interrupted");
                }
                nbytes = istream.read( data );
                //say("Transferring "+nbytes+" bytes ("+_bytesTransferred+")");
                if( nbytes <= 0 )
                    break;
                while( ( _spaceUsed + nbytes ) > _spaceAllocated ) {
                    _status = "WaitingForSpace("+_allocationSpace+")" ;
                    spaceMonitor.allocateSpace( _allocationSpace ) ;
                    _spaceAllocated += _allocationSpace ;
                    _status = "" ;
                }
                _spaceUsed        += nbytes;

                receiveEBlock(data, 0, nbytes, _bytesTransferred);
                _bytesTransferred += nbytes;
                _lastTransferred = System.currentTimeMillis() ;
            }

        } finally {
            try {
                dataSocket.close() ;
            } catch(Exception xe) {
                // ignore
            }
            ftp.setBytesTransferred( _bytesTransferred ) ;
            _transferTime = System.currentTimeMillis() - _transferStarted ;
            ftp.setTransferTime( _transferTime ) ;
            say("recvStream: transferred " +
                 _bytesTransferred + " bytes in " +
                 ( _transferTime/1000 ) + " seconds.");
            if( _transferTime > 0 ) {
                double rate =
                    ((double)_bytesTransferred)/((double)_transferTime)/1024.*1000./1024.;
                say("recvStream: transfer rate: " + rate + " MBytes/sec");
            }
            say("recvStream: SysTimer: " + sysTimer.getDifference().toString());
        }
        long freeIt = _spaceAllocated - _spaceUsed ;
        if( freeIt > 0 ) {
            spaceMonitor.freeSpace( freeIt);
        }
        return true;
    }

    public boolean recvEBlock(
                              GFtpProtocolInfo ftp,
                              StorageInfo  storage,
                              String       pnfsId,
                              SpaceMonitor spaceMonitor )
    throws Exception {
        String ftpErrorMsg = "";
        say("recvEBlock: opening proxy mode connection to door");
        Socket dataSocket = null;
        try {
            dataSocket = new Socket(InetAddress.getByName(ftp.getHost()), ftp.getPort());
            /** todo - check it is not null */
            _eBlockReceiver =
                new EBlockReceiver(dataSocket,
                                   spaceMonitor,
                                   this);
            say("recvEBlock: proxy mode connection opened to " + ftp.getHost() + ":" + ftp.getPort());
            if(ftp.getBufferSize()>0) {
                dataSocket.setReceiveBufferSize(ftp.getBufferSize());
                dataSocket.setSendBufferSize(ftp.getBufferSize());
            }
        } 
        catch( IOException ex ) {
            esay("recvEBlock: failed to make proxy mode connection: " +
                 ex.getMessage());
        }
        try {
            _eBlockReceiver.start();
            while( _eBlockReceiver.isAlive() ) { 
                _eBlockReceiver.join();
            }
            say("recvEBlock: number of offset ranges = " + _offsetRanges.getRanges().size());
            if (!_offsetRanges.isContiguous()) { 
                ftpErrorMsg = "451 Transmission error: lost EOD blocks";
                esay("recvEBlock: offset ranges are not contiguous");
                _offsetRanges.clear();
                _offsetRanges = null;
                throw new CacheException(ftpErrorMsg) ;
            }
            _offsetRanges.clear();
            _offsetRanges=null;
        } 
        catch ( Exception e ) {
            ftpErrorMsg = "451 Transmission error: " + e;
            esay("recvEBlock: " + ftpErrorMsg);
            return false;
        }
        finally {
            try  {   
                dataSocket.close(); 
            } 
            catch(IOException ignored) {
                // ignore
            }
        }

        if( _eBlockReceiver.failed() ) {
            ftpErrorMsg = "451 Receiver failed";
            esay("recvEBlock: " + ftpErrorMsg);
            return false;
        }

        return true;
    }

    public boolean wasChanged() {
        return _wasChanged;
    }

    // the following methods were adapted from DCapProtocol_3_nio mover
    public Checksum getClientChecksum() {
        return null;
    }

    public Checksum getTransferChecksum() {
        try {
            if(_transferChecksum == null) {
                return null;
            }

            if(recalculateOnFile && _transferMessageDigest != null) {
                byte[] bytes = new byte[128*1024];
                raDiskFile.seek(0);
                while (true) {
                    int read = raDiskFile.read(bytes);
                     if (read <= 0) {
                         break;
                     }
                     _transferMessageDigest.update(bytes,0,read);
                }
            }

            return _transferChecksum;
        }
        catch (Exception e) {
            esay("getTransferChecksum: got exception: " + e.getMessage());
            return null;
        }
    }

    public void setDigest(Checksum checksum) {
        _transferChecksum = checksum;
        _transferMessageDigest =
            checksum != null ? checksum.getMessageDigest() : null;
    }
   
    public ChecksumFactory getChecksumFactory(ProtocolInfo protocol) {
        if(protocol instanceof GFtpProtocolInfo) {
           GFtpProtocolInfo ftpp = (GFtpProtocolInfo)protocol;
           try {
               return ChecksumFactory.getFactory(ftpp.getChecksumType());
           } catch (NoSuchAlgorithmException ex) {
               esay("getChecksumFactory: CRC algorithm '" +
                    ftpp.getChecksumType() + "' is not supported.");
           }
        }
        return null;
    }

    // if we receive extended mode blocks,
    // we still try to calculate the checksum
    // but if this fails
    // we can calculate the checksum
    // on the end file
    private long previousUpdateEndOffset = 0;
    private boolean recalculateOnFile=false;

    public synchronized void receiveEBlock(byte[] array,
                                           int offset,
                                           int length,
                                           long offsetOfArrayInFile)
    throws IOException {

        raDiskFile.seek(offsetOfArrayInFile);
        raDiskFile.write(array, offset, length);
        _offsetRanges.addRange(new Range(offsetOfArrayInFile,offsetOfArrayInFile+(long)length));

        if(_transferMessageDigest == null || recalculateOnFile) {
            return;
        }

        if(_transferMessageDigest != null &&
           previousUpdateEndOffset != offsetOfArrayInFile) {
            say("receiveEBlock: previousUpdateEndOffset=" +
                previousUpdateEndOffset +
                ", offsetOfArrayInFile=" + offsetOfArrayInFile +
                " . Resetting the digest for future checksum calculation " +
                "of the file");
           recalculateOnFile = true;
           _transferMessageDigest.reset();
           return;
        }

        if(array == null) {
            return;
        }
        previousUpdateEndOffset += length;
        if(_transferMessageDigest != null) {
            _transferMessageDigest.update(array, offset, length);
        }
    }
}
