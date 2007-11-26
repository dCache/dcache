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

// $Id: GFtpProtocol_1_nio.java,v 1.12 2007-08-30 21:10:58 abaranov Exp $
package diskCacheV111.movers;

import diskCacheV111.vehicles.*;
import diskCacheV111.util.*;
import diskCacheV111.doors.*;
import diskCacheV111.repository.SpaceMonitor;
import dmg.cells.nucleus.*;
import java.io.*;
import java.util.* ;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.ServerSocketChannel;
import java.security.MessageDigest;
import diskCacheV111.util.Checksum;
import  java.security.NoSuchAlgorithmException ;
import diskCacheV111.util.ChecksumFactory;

public class GFtpProtocol_1_nio implements MoverProtocol, ChecksumMover,
        NioDataBlocksRecipient {

    //
    // <init>( CellAdapter cell ) ;
    //
    private CellAdapter _cell = null;

    private long _bytesTransferred = 0;

    private long _lastTransferred = System.currentTimeMillis();

    private long _transferStarted = 0;

    private long _transferTime = 0;

    private int _bufferSize = 0;

    private long _spaceUsed = 0;

    private long _spaceAllocated = 0;

    private static final int INC_SPACE = (50 * 1024 * 1024);

    private int _allocationSpace = INC_SPACE;

    private String _status = "None";

    private boolean _wasChanged = false;

    private Ranges _offsetRanges = null;


    EBlockReceiverNio _eBlockReceiver = null;

    // checksum related variables
    // copied from DCapProtocol_3_nio

    private MessageDigest _transferMessageDigest = null;

    private Checksum _transferChecksum = null;

    public GFtpProtocol_1_nio(CellAdapter cell, int bufferSize) {
        this(cell);
        _bufferSize = bufferSize;
    }

    public GFtpProtocol_1_nio(CellAdapter cell) {
	_offsetRanges = new Ranges();
	_cell = cell;
        _cell.say("GFtpProtocol_1_nio created");
    }

    public void setAttribute(String name, Object attribute) {}

    public Object getAttribute(String name) {
        return null;
    }

    private void say(String str) {
        _cell.say("(GFtp_1_nio) " + str);
    }

    private void esay(String str) {
        _cell.esay("(GFtp_1_nio) " + str);
    }

    private void esay(Throwable t) {
        _cell.esay(t);
    }

    public String toString() {
        return "SU=" + _spaceUsed + ";SA=" + _spaceAllocated + ";S=" + _status;
    }

    FileChannel _fileChannel = null;

    public void runIO(RandomAccessFile diskFile, ProtocolInfo protocol,
            StorageInfo storage, PnfsId pnfsId, SpaceMonitor spaceMonitor,
            int access)

    throws Exception {
        _fileChannel = diskFile.getChannel();
        _lastTransferred = System.currentTimeMillis();
        if ((access & MoverProtocol.WRITE) != 0) {
            _wasChanged = true;
            runRemoteToDisk(protocol, storage, pnfsId.toString(), spaceMonitor);
        } else {
            runDiskToRemote(protocol, storage, pnfsId.toString());
        }

    }

    public void runRemoteToDisk(ProtocolInfo protocol, StorageInfo storage,
            String pnfsId, SpaceMonitor spaceMonitor)

    throws Exception {

        if (!(protocol instanceof GFtpProtocolInfo)) {
            throw new CacheException(44, "protocol info not GFtpProtocolInfo");

        }

        GFtpProtocolInfo ftp = (GFtpProtocolInfo) protocol;
        String mode = ftp.getMode();

        say("runRemoteToDisk called mode = " + mode);

        if (mode.equalsIgnoreCase("S")) {
            recvStream(ftp, storage, pnfsId, spaceMonitor);
        } 
	else if (mode.equalsIgnoreCase("E")) {
	    if (!recvEBlock(ftp, storage, pnfsId, spaceMonitor)) { 
		throw new CacheException("failed in recvEBlock");
	    }
        }
    }

    public void runDiskToRemote(ProtocolInfo protocol, StorageInfo storage,
            String pnfsId)

    throws Exception {

        if (!(protocol instanceof GFtpProtocolInfo)) {
            throw new CacheException(44, "protocol info not GFtpProtocolInfo");

        }
        GFtpProtocolInfo ftp = (GFtpProtocolInfo) protocol;
        String mode = ftp.getMode();

        if (mode.equalsIgnoreCase("S"))
            sendStream(ftp, storage, pnfsId);
        else if (mode.equalsIgnoreCase("E"))
            sendEBlock(ftp, storage, pnfsId);
    }

    private void sendStream(GFtpProtocolInfo ftp, StorageInfo storage,
            String pnfsId)

    throws Exception {

        int port = ftp.getPort();
        String host = ftp.getHost();
        // this is the offset and size as specified for
        // ERET in partial retrieve mode
        long prm_offset = ftp.getOffset();
        long prm_size = ftp.getSize();
        long fileSize = storage.getFileSize();
        if (prm_offset < 0) {
            String err = "prm_offset is " + prm_offset;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        if (prm_size < 0) {
            String err = "prm_offset is " + prm_size;
            esay(err);
            throw new IllegalArgumentException(err);
        }

        if (prm_offset + prm_size > fileSize) {
            String err = "invalid prm_offset=" + prm_offset + " and prm_size "
                    + prm_size + " for file of size " + fileSize;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        say("Connecting to " + host + "(" + port + ")");
        // NIO socket
        SocketChannel socketChannel = null;
        socketChannel = SocketChannel.open(new InetSocketAddress(InetAddress
                .getByName(host), port));
        socketChannel.configureBlocking(true);

        Socket dataSocket = socketChannel.socket();

        if (ftp.getBufferSize() > 0) {
            dataSocket.setReceiveBufferSize(ftp.getBufferSize());
            dataSocket.setSendBufferSize(ftp.getBufferSize());
        }

        say("Connected to " + host + "(" + port + ")");
        int data_length = 128 * 1024;
        say("Expected filesize is " + fileSize + " bytes");

        int nbytes;
        _transferStarted = System.currentTimeMillis();
        _bytesTransferred = 0;
        SysTimer sysTimer = new SysTimer();
        sysTimer.getDifference();
        ByteBuffer bigBuffer = null;
        try {

            bigBuffer = ByteBuffer.allocateDirect(data_length);

            _fileChannel.position(prm_offset);
            while ((!Thread.currentThread().isInterrupted())
                    && (_bytesTransferred < prm_size)) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Transfer interrupted");
                }

                bigBuffer.clear();
                bigBuffer.position(0);
                bigBuffer.limit(bigBuffer.capacity());

                nbytes = _fileChannel.read(bigBuffer);

                // say("Transferring "+nbytes+" bytes");
                if (nbytes <= 0) {
                    break;
                }

                bigBuffer.limit(nbytes);
                bigBuffer.rewind();
                socketChannel.write(bigBuffer);

                _lastTransferred = System.currentTimeMillis();
                _bytesTransferred += nbytes;
            }
        } finally {

            try {
                socketChannel.close();
            } catch (Exception xe) {
            }

            ftp.setBytesTransferred(_bytesTransferred);
            _transferTime = System.currentTimeMillis() - _transferStarted;
            ftp.setTransferTime(_transferTime);

            say("Transfer finished : " + _bytesTransferred + " bytes in "
                    + (_transferTime / 1000) + " seconds ");
            if (_transferTime > 0) {
                double rate = ((double) _bytesTransferred)
                        / ((double) _transferTime) / 1024. * 1000. / 1024.;
                say("TransferRate : " + rate + " MBytes/sec");
            }
            say("SysTimer : " + sysTimer.getDifference().toString());
        }
    }

    private void sendEBlock(GFtpProtocolInfo protocol, StorageInfo storage,
            String pnfsId)

    throws Exception {

        int port = protocol.getPort();
        String host = protocol.getHost();
        long fileSize = storage.getFileSize();
        // this is the offset and size as specified for
        // ERET in partial retrieve mode
        long prm_offset = protocol.getOffset();
        long prm_size = protocol.getSize();
        if (prm_offset < 0) {
            String err = "prm_offset is " + prm_offset;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        if (prm_size < 0) {
            String err = "prm_offset is " + prm_size;
            esay(err);
            throw new IllegalArgumentException(err);
        }

        if (prm_offset + prm_size > fileSize) {
            String err = "invalid prm_offset=" + prm_offset + " and prm_size "
                    + prm_size + " for file of size " + fileSize;
            esay(err);
            throw new IllegalArgumentException(err);
        }
        say("received prm_offset " + prm_offset + " prm_size " + prm_size);
        say("Connecting to " + host + "(" + port + ")");
        int parallelStart = protocol.getParallelStart();

        SocketChannel[] socketChannel = new SocketChannel[parallelStart];
        int data_channel_num = 0;
        Exception lastException = null;
        for (int i = 0; i < parallelStart; i++) {
            try {

                // NIO socket
                socketChannel[i] = SocketChannel.open(new InetSocketAddress(
                        InetAddress.getByName(host), port));
                socketChannel[i].configureBlocking(true);

                Socket dataSocket = socketChannel[i].socket();

                if (_bufferSize > 0) {
                    dataSocket.setReceiveBufferSize(_bufferSize);
                    dataSocket.setSendBufferSize(_bufferSize);
                }

            } catch (Exception e) {
                lastException = e;
                break;
            }
            data_channel_num++;
        }
        if (data_channel_num == 0) {
            esay("could not open even a single data channel");
            if (lastException != null) {
                throw lastException;
            } else {
                throw new IOException(
                        "could not open even a single data channel");
            }
        }
        say("Connected to " + host + "(" + port + ")");

        int bufsize = 10 * 1024;
        ByteBuffer bigBuffer = ByteBuffer.allocateDirect(bufsize + 17);

        say("Expected filesize is " + fileSize + " bytes");

        int nbytes;
        _transferStarted = System.currentTimeMillis();
        _bytesTransferred = 0;
        SysTimer sysTimer = new SysTimer();
        sysTimer.getDifference();
        try {
            int idc = 0;
            long offset = prm_offset;

            _fileChannel.position(prm_offset);

            while (!Thread.currentThread().isInterrupted()
                    && (_bytesTransferred < prm_size)) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Transfer interrupted");
                }

                bigBuffer.clear();
                bigBuffer.position(17);

                if (bufsize + _bytesTransferred <= prm_size) {
                    bigBuffer.limit(bufsize + 17 );

                } else {
                    bigBuffer.limit( (int)(prm_size - _bytesTransferred) + 17);
                }
                
                nbytes = _fileChannel.read(bigBuffer);
                
                if (nbytes <= 0) {
                    break;
                }
                idc %= data_channel_num;

                bigBuffer.position(0);
                bigBuffer.put((byte) 0);
                bigBuffer.putLong((long) nbytes);
                bigBuffer.putLong((long) offset);
                bigBuffer.rewind();
                socketChannel[idc++].write(bigBuffer);
                // say("sent eblock with offset = "+offset+", byte count = "+
                // nbytes);
                _bytesTransferred += nbytes;

                offset += nbytes;
                _lastTransferred = System.currentTimeMillis();
            }
            // Send EODC & EOD & CLOSE bits
            // if we do not send EOD,
            // then, because of globus cog kit bug the data channel will
            // be considered not to transfer any more data
            // by the cog kit client since this is the zero length block and
            // the client will not try receive EOD on this channel
            bigBuffer.clear();
            bigBuffer.position(0);
            bigBuffer.put((byte) (64 | 8 | 4));
            bigBuffer.putLong(0);
            bigBuffer.putLong(data_channel_num);

            bigBuffer.rewind();
            socketChannel[0].write(bigBuffer);

            say("EODC(" + data_channel_num + ") + EOD on CHANNEL #0 sent");

            for (idc = 1; idc < data_channel_num; idc++) {
                // set EOD & CLOSE bits

                bigBuffer.clear();
                bigBuffer.position(0);
                bigBuffer.put((byte) (8 | 4));
                bigBuffer.putLong(0);
                bigBuffer.putLong(offset);

                bigBuffer.rewind();
                socketChannel[idc].write(bigBuffer);

                say("EOD on CHANNEL #" + idc + " sent");
            }

        } finally {
            for (int i = 0; i < data_channel_num; i++) {
                try {
                    socketChannel[i].close();
                } catch (Exception xe) {
                    esay("error closing data socket " + socketChannel[i]
                            + " : " + xe);
                }
            }
            protocol.setBytesTransferred(_bytesTransferred);
            _transferTime = System.currentTimeMillis() - _transferStarted;
            protocol.setTransferTime(_transferTime);

            say("Transfer finished : " + _bytesTransferred + " bytes in "
                    + (_transferTime / 1000) + " seconds ");
            if (_transferTime > 0) {
                double rate = ((double) _bytesTransferred)
                        / ((double) _transferTime) / 1024. * 1000. / 1024.;
                say("TransferRate : " + rate + " MBytes/sec");
            }
            say("SysTimer : " + sysTimer.getDifference().toString());
        }

    }

    public long getBytesTransferred() {
        /** @todo - synchronize with _lastTransferred */
        return (_eBlockReceiver == null) ? _bytesTransferred : _eBlockReceiver
                .getBytesTransferred();
    }

    public long getLastTransferred() {
        /** @todo - synchronize with _bytesTransferred */
        return (_eBlockReceiver == null) ? _lastTransferred : _eBlockReceiver
                .getLastTransferred();
    }

    public long getTransferTime() {
        return _transferTime < 0 ? System.currentTimeMillis()
                - _transferStarted : _transferTime;
    }

    public boolean recvStream(GFtpProtocolInfo ftp, StorageInfo storage,
            String pnfsId, SpaceMonitor spaceMonitor) throws Exception {
        int port = ftp.getPort();
        String host = ftp.getHost();

        // NIO socket
        SocketChannel socketChannel = null;
        socketChannel = SocketChannel.open(new InetSocketAddress(InetAddress
                .getByName(host), port));
        socketChannel.configureBlocking(true);

        Socket dataSocket = socketChannel.socket();

        say("Connected to " + host + "(" + port + ")");

        if (ftp.getBufferSize() > 0) {
            dataSocket.setReceiveBufferSize(ftp.getBufferSize());
            dataSocket.setSendBufferSize(ftp.getBufferSize());
        }

        ByteBuffer bigBuffer = null;
        int nbytes;
        SysTimer sysTimer = new SysTimer();
        _transferStarted = System.currentTimeMillis();
        _bytesTransferred = 0;
        sysTimer.getDifference();
        try {

            bigBuffer = ByteBuffer.allocateDirect(128 * 1204);

            while (!Thread.currentThread().isInterrupted()) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException("Transfer interrupted");
                }
                bigBuffer.clear();
                bigBuffer.position(0);
                bigBuffer.limit(bigBuffer.capacity());
                nbytes = socketChannel.read(bigBuffer);

                // say("Transferring "+nbytes+" bytes ("+_bytesTransferred+")");
                if (nbytes <= 0) {
                    break;
                }

                while ((_spaceUsed + nbytes) > _spaceAllocated) {
                    _status = "WaitingForSpace(" + _allocationSpace + ")";
                    spaceMonitor.allocateSpace(_allocationSpace);
                    _spaceAllocated += _allocationSpace;
                    _status = "";
                }
                _spaceUsed += nbytes;

                bigBuffer.limit(nbytes);
                bigBuffer.rewind();

                receiveEBlock(bigBuffer, 0, nbytes, _bytesTransferred);
                _bytesTransferred += nbytes;
                _lastTransferred = System.currentTimeMillis();
            }

        } finally {
            try {
                socketChannel.close();
            } catch (Exception xe) {
            }

            ftp.setBytesTransferred(_bytesTransferred);
            _transferTime = System.currentTimeMillis() - _transferStarted;
            ftp.setTransferTime(_transferTime);
            say("Transfer finished : " + _bytesTransferred + " bytes in "
                    + (_transferTime / 1000) + " seconds ");
            if (_transferTime > 0) {
                double rate = ((double) _bytesTransferred)
                        / ((double) _transferTime) / 1024. * 1000. / 1024.;
                say("TransferRate : " + rate + " MBytes/sec");
            }
            say("SysTimer : " + sysTimer.getDifference().toString());
        }

        long freeIt = _spaceAllocated - _spaceUsed;
        if (freeIt > 0) {
            spaceMonitor.freeSpace(freeIt);
        }

        return true;
    }

    public boolean recvEBlock(GFtpProtocolInfo ftp, StorageInfo storage,
            String pnfsId, SpaceMonitor spaceMonitor) throws Exception {
        String ftpErrorMsg = null;
        // XXX For proxy mode connections
        say("Opening proxy mode connection to door");
        SocketChannel dataSocket = null;
        try {
            // Socket dataSocket = new
            // Socket(InetAddress.getByName(ftp.getHost()), ftp.getPort());
            dataSocket = SocketChannel.open( new InetSocketAddress(ftp.getHost(), ftp.getPort()));
            /** todo - check it is not null */
            _eBlockReceiver = new EBlockReceiverNio(dataSocket, spaceMonitor,
                    this);
            say("Proxy mode connection opened " + ftp.getHost() + " "
                    + ftp.getPort());
            if (ftp.getBufferSize() > 0) {
                dataSocket.socket().setReceiveBufferSize(ftp.getBufferSize());

            }
        } catch (IOException ex) {
            // XXX Need some code here to let say that things are bad!
            esay("Proxy mode connection failed " + ex.getMessage() + " host=" + ftp.getHost() + ":" + ftp.getPort() );
            esay(ex);
        }
        try {
            _eBlockReceiver.start();
            while (_eBlockReceiver.isAlive()) { 
                _eBlockReceiver.join();
	    }
	    say("Number of offset ranges = "+_offsetRanges.getRanges().size());
	    if (!_offsetRanges.isContiguous()) { 
		ftpErrorMsg = "451 Transmission error: " + "lost EB blocks";
		say(ftpErrorMsg);
		_offsetRanges.clear();
		_offsetRanges=null;
		throw new
		    CacheException(ftpErrorMsg) ;
	    }
	    _offsetRanges.clear();
	    _offsetRanges=null;
            // TLog.middle(rcvr.bytesReceived());
        } catch (Exception e) {
            ftpErrorMsg = "451 Transmission error: " + e;
            say(ftpErrorMsg);
            return false;
        }finally{
        	dataSocket.close();
        }
        if (_eBlockReceiver.failed()) {
            ftpErrorMsg = "451 Receiver failed";
            say(ftpErrorMsg);
            return false;
        }

        // if( ConfirmEOFs )
        // reply ("200 Data received. Confirm EOF.");

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
            if (_transferChecksum == null) {
                return null;
            }

            if (recalculateOnFile && _transferMessageDigest != null) {

                ByteBuffer buffer = ByteBuffer.allocateDirect(128 * 1024);
                _fileChannel.position(0);
                while (true) {
                    buffer.clear();
                    buffer.position(0);
                    int read = _fileChannel.read(buffer);
                    if (read <= 0) {
                        break;
                    }
                    _transferMessageDigest.update(buffer.array(), 0, read);
                }
            }

            return _transferChecksum;
        } catch (Exception e) {
            esay(e);
            return null;
        }
    }

    public void setDigest(Checksum checksum) {
        _transferChecksum = checksum;
        _transferMessageDigest = checksum != null ? checksum.getMessageDigest()
                : null;

    }

     public ChecksumFactory getChecksumFactory(ProtocolInfo protocol)
     {
        if( protocol instanceof GFtpProtocolInfo ){
           GFtpProtocolInfo ftpp = (GFtpProtocolInfo)protocol;
           try {
               return ChecksumFactory.getFactory(ftpp.getChecksumType());
           } catch ( NoSuchAlgorithmException ex){
               esay("CRC Algorithm is not supported: "+ftpp.getChecksumType());
           }
        }
        return null;
    }

    // if we receive extendedn mode blocks,
    // we still try to calculate the checksum
    // but if this fails
    // we can calculate the checksum
    // on the end file
    private long previousUpdateEndOffset = 0;

    private boolean recalculateOnFile = false;

    public synchronized void receiveEBlock(ByteBuffer buffer, int skipBytes,
            int length, long fileOffset) throws IOException {

        buffer.rewind();
        buffer.position(skipBytes);
        buffer.limit(length + skipBytes);        
        _fileChannel.write(buffer, fileOffset);
	_offsetRanges.addRange(new Range(fileOffset,fileOffset+(long)length));

        if (_transferMessageDigest == null || recalculateOnFile) {
            return;
        }

        if (_transferMessageDigest != null
                && previousUpdateEndOffset != fileOffset) {
            say("previousUpdateEndOffset="
                    + previousUpdateEndOffset
                    + " offsetOfArrayInFile="
                    + fileOffset
                    + " : resetting the digest for future checksum calculation of the file");
            recalculateOnFile = true;
            _transferMessageDigest.reset();
            return;

        }

        previousUpdateEndOffset += length;
        if (_transferMessageDigest != null) {
            _transferMessageDigest.update(buffer.array(), skipBytes, length);
        }
    }

}
