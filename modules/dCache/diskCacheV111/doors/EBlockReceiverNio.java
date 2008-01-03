// $Id: EBlockReceiverNio.java,v 1.4 2006-11-13 14:13:02 tigran Exp $

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

package diskCacheV111.doors;

import diskCacheV111.repository.SpaceMonitor;
import java.util.*;
import java.io.*;
import java.net.*;
import diskCacheV111.movers.NioDataBlocksRecipient;
import java.nio.channels.*;
import java.nio.ByteBuffer;

import org.apache.log4j.Logger;

public class EBlockReceiverNio extends Thread {

	private final static Logger _logSpaceAllocation = Logger.getLogger("logger.org.dcache.poolspacemonitor." + EBlockReceiverNio.class.getName());
	
    private class EBlockDataChannelNio extends Thread {
        private NioDataBlocksRecipient recipient;

        private SocketChannel _dataSocket;

        private EBlockReceiverNio _receiver;

        private SpaceMonitor _spaceMonitor;

        public long _chBytesReceived = 0;

        private long _chLastTransferred = System.currentTimeMillis();

        public boolean _failed = false;

        private long _spaceUsed = 0;

        private long _spaceAllocated = 0;

        private static final int INC_SPACE = (500 * 1024 * 1024);

        private int _allocationSpace = INC_SPACE;

        public EBlockDataChannelNio(EBlockReceiverNio rcvr,
                NioDataBlocksRecipient recipient, SocketChannel sock,
                SpaceMonitor spaceMonitor) {
        	super();
            this.recipient = recipient;
            _dataSocket = sock;
            _receiver = rcvr;
            _spaceMonitor = spaceMonitor;
        }

        public void run() {
            try {
                System.out.println("Running EBlockDataChannel");

                boolean eod = false;
                EDataBlockNio b = new EDataBlockNio();
                while (!eod) {
                    try {
                        long n = b.read(_dataSocket);
                        if( n < 0 ) {
                        	// protocol error?
                        	eod = true;
                        	break;
                        }
                        if (b.isDescriptorSet(EDataBlock.EOF_DESCRIPTOR)) {
                            _receiver.eodcReceived((int) b.getOffset());
                            continue;
                        }

                        long blockSize = b.getSize();

                        if (blockSize < 0) {
                            eod = true;
                            break;
                        }

                        while ((_spaceUsed + n) > _spaceAllocated) {
                        	_logSpaceAllocation.debug("ALLOC: <UNKNOWN> : " + _allocationSpace);
                            _spaceMonitor.allocateSpace(_allocationSpace);
                            _spaceAllocated += _allocationSpace;
                        }
                        _spaceUsed += n;

                        if (n > 0 && n == blockSize) {
                            try {
                                // byte array in java can be at most of the
                                // length
                                // Integer.MAX_VALUE (2^31 -1)
                                // so the length of the datablock we can receive
                                // with current implementation is at most 2^31-1
                                _receiver.writeBlock(b.getData(),
                                        b.getOffset(), (int) blockSize);
                            } catch (Exception e) {
                                System.out.println("L57");
                                _failed = true;
                                eod = true;
                                break;
                            }
                            _chBytesReceived += blockSize;
                            _chLastTransferred = System.currentTimeMillis();
                            // Update common counter for all channels
                            addBytesTransferred(blockSize, _chLastTransferred);
                        }

                        if (b.isDescriptorSet(EDataBlock.EOD_DESCRIPTOR)) {
                            eod = true;
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            } catch (OutOfMemoryError oome) {
                oome.printStackTrace();
            }finally{
            	
            }
            long freeIt = _spaceAllocated - _spaceUsed;
            if (freeIt > 0) {
            	_logSpaceAllocation.debug("FREE: <UNKNOWN> : " + _allocationSpace );
                _spaceMonitor.freeSpace(freeIt);
            }

        }
    }

    private NioDataBlocksRecipient recipient;

    private SpaceMonitor _spaceMonitor;

    private ServerSocketChannel LsnSocket;

    private boolean EODCReceived = false;

    private int NDC = 0;

    private long _bytesReceived = 0;

    private long _lastTransferred = System.currentTimeMillis();

    private Object _syncBytesRec = new Object();

    private boolean _failed = false;

    private boolean InAccept = false;

    // XXX Proxy mode exists because the order of operations in
    // ftp is wrong for getting the port the client needs when it
    // needs it. Therefore we give the client a port on the door
    // admin node and create a passthrough from there.
    // This poor-mans proxy only uses one channel
    private boolean ProxyMode;

    private SocketChannel proxySocket;

    public EBlockReceiverNio(ServerSocketChannel sock,
            SpaceMonitor spaceMonitor, NioDataBlocksRecipient recipient) {
        this.recipient = recipient;
        LsnSocket = sock;
        ProxyMode = false;
        _spaceMonitor = spaceMonitor;
    }

    public EBlockReceiverNio(SocketChannel sock, SpaceMonitor spaceMonitor,
            NioDataBlocksRecipient recipient) {
        this.recipient = recipient;
        proxySocket = sock;
        ProxyMode = true;
        _spaceMonitor = spaceMonitor;
    }

    public void runProxyMode() {
        int dccnt = 0;
        EBlockDataChannelNio[] channels = new EBlockDataChannelNio[100];
        System.out.println("Running eblock-proxy reciever");

        EBlockDataChannelNio chn = new EBlockDataChannelNio(this, recipient,
                proxySocket, _spaceMonitor);
        channels[dccnt++] = chn;
        chn.start();

        setBytesTransferred(0);

        try {
            for (int i = 0; i < dccnt; i++) {
                System.out.println("EBlockReceiver: waiting for chn #" + i);
                while (channels[i].isAlive())
                    try {
                        channels[i].join();
                    } catch (Exception e) {
                        System.out.println("L116");
                        break;
                    }

                if (channels[i]._failed) {
                    _failed = true;
                }
                // Now each channel update common counter
                // _bytesReceived += channels[i]._chBytesReceived;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("EBlockReceiver: DONE");
    }

    public void run() {
        if (ProxyMode) {
            runProxyMode();
        } else {
            runNormalMode();
        }
    }

    public void runNormalMode() {
        int dccnt = 0;
        EBlockDataChannelNio[] channels = new EBlockDataChannelNio[100];
        System.out.println("Running eblock reciever");

        try {
            LsnSocket.socket().setSoTimeout(5000);
        } catch (SocketException e) {
            _failed = true;
            return;
        }

        while (!(EODCReceived && dccnt >= NDC) && !Thread.interrupted()) {
            SocketChannel sock;
            try {
                System.out.println("Accepting...");
                sock = LsnSocket.accept();
                System.out.println("accepted");
            }
            // catch( InterruptedException e )
            // { continue; }
            catch (IOException e) {
                break;
            }
            EBlockDataChannelNio chn = new EBlockDataChannelNio(this,
                    recipient, sock, _spaceMonitor);
            channels[dccnt++] = chn;
            chn.start();
            System.out.println("EBlockReceiver: chn #" + dccnt + " open");

        }

        System.out.println("EBlockReceiver: all channels open");

        try {
            LsnSocket.close();
        } catch (Exception e) {
        }

        setBytesTransferred(0);

        for (int i = 0; i < dccnt; i++) {
            System.out.println("EBlockReceiver: waiting for chn #" + i);
            while (channels[i].isAlive())
                try {
                    channels[i].join();
                } catch (Exception e) {
                    break;
                }
            if (channels[i]._failed)
                _failed = true;

            // Now each channel update common counter
            // _bytesReceived += channels[i]._chBytesReceived;
        }
        System.out.println("EBlockReceiver: DONE");
    }

    public long getLastTransferred() {
        return _lastTransferred;
    }

    public long getBytesTransferred() {
        return _bytesReceived;
    }

    private void setBytesTransferred(long n) {
        synchronized (_syncBytesRec) {
            _bytesReceived = n;
            _lastTransferred = System.currentTimeMillis();
        }
    }

    private void setBytesTransferred(long n, long t) {
        synchronized (_syncBytesRec) {
            _bytesReceived = n;
            _lastTransferred = t;
        }
    }

    private void addBytesTransferred(long n, long t) {
        synchronized (_syncBytesRec) {
            _bytesReceived += n;
            _lastTransferred = t;
        }
    }

    public synchronized void writeBlock(ByteBuffer buf, long offset, int length)
            throws IOException {
        recipient.receiveEBlock(buf, 0, length, offset);
    }

    public void eodcReceived(int count) {
        System.out.println("EBlockReceiver: EODC(" + count + ") received");
        EODCReceived = true;
        NDC = count;
    }

    public boolean failed() {

        return _failed;
    }
}

// $Log: not supported by cvs2svn $
// Revision 1.3  2006/05/16 14:26:01  tigran
// fixed reciveEBlock
//
// Revision 1.2 2006/04/28 08:20:07 tigran
// added Nio EData block
//
// Revision 1.1 2006/04/27 14:07:20 tigran
// skeleton for nio mover
//
// Revision 1.11 2005/10/26 23:49:13 aik
// Fix transfer counters in EBlock-receive mode
//
// Revision 1.10 2005/05/24 20:34:59 timur
// fixed the two GIGABYTE limitation on gridftp write bug
//
// Revision 1.9 2005/03/30 18:24:22 timur
// GFtpMover implements ChecksumMover interface
//
// Revision 1.8 2003/07/08 16:35:45 cvs
// timur: adding the support for gridftp commands ERET and ESTO,
// FEAT will report the features correctly,
// srm copy will use native java gridftp client code to perform
// transfers directly to or from the pools.
//
// Revision 1.6 2003/07/03 23:38:34 cvs
// error message
//
// Revision 1.5 2002/11/20 17:39:51 cvs
// added fermilab license
//
