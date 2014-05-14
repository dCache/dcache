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

package org.dcache.ftp.proxy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;

import org.dcache.util.PortRange;

public class ActiveAdapter implements Runnable, ProxyAdapter
{
    private final static Logger _log =
        LoggerFactory.getLogger(ActiveAdapter.class);

    /* After the transfer is completed we only expect the key for the
     * server socket to be left.
     */
    private final static int EXPECTED_KEY_SET_SIZE_WHEN_DONE = 1;

    private ServerSocketChannel _ssc; // The ServerSocketChannel we will
                                        // listen on...
    private String _tgtHost; // The remote host to connect
    private int _tgtPort; // The remote port to connect
    private String _laddr; // Local IP address
    private int _lport; // Local port number
    private int _maxBlockSize = 32768; // Size of the buffers for transfers
    private int _expectedStreams = 1; // The number of streams expected
    private Selector _selector;
    private final LinkedList<SocketChannel> _pending = new LinkedList<>();
    private String _error;
    private Thread _t; // A thread driving the adapter
    private boolean _closeForced;
    private int _streamsCreated;

    /**
     * @param range Port range for server socket
     * @param host Host to connect to
     * @param port Port to connect to
     * @throws IOException
     */
    public ActiveAdapter(PortRange range, String host, int port)
        throws IOException
    {
        _tgtHost = host;
        _tgtPort = port;

        _ssc = ServerSocketChannel.open();
        _ssc.configureBlocking(false);
        range.bind(_ssc.socket());
        _laddr = InetAddress.getLocalHost().getHostAddress(); // Find the
                                                                // address as a
                                                                // string
        _lport = _ssc.socket().getLocalPort(); // Find the port
        _t = new Thread(this);
        // Create a new Selector for selecting
        _selector = Selector.open();
    }

    @Override
    public synchronized void close()
    {
        _closeForced = true;
        if (_selector != null) {
            _selector.wakeup();
        }
        if (!_t.isAlive()) {
            /* Take care of the case where the adapter was never started.
             */
            closeNow();
        }
    }

    private synchronized void closeNow()
    {
        if (_ssc != null) {
            try {
                say("Closing " + _ssc.socket());
                _ssc.close();
            } catch (IOException e) {
                esay("Failed to close server socket: " + e.getMessage());
            }
            _ssc = null;
        }

        if (_selector != null) {
            for (SelectionKey key: _selector.keys()) {
                if (key.isValid() && key.attachment() instanceof Tunnel) {
                    ((Tunnel) key.attachment()).close();
                }
            }

            try {
                _selector.close();
            } catch (IOException e) {
                esay("Failed to close selector: " + e.getMessage());
            }
            _selector = null;
        }
    }

    /**
     * Returns whether the transfer is still in progress.
     */
    private synchronized boolean isTransferInProgress()
        throws IOException
    {
        if (_closeForced) {
            return false;
        }

        if (_streamsCreated < _expectedStreams) {
            return true;
        }

        /* We call selectNow to make sure that cancelled keys have
         * been removed from the key set.
         */
        _selector.selectNow();

        return _selector.keys().size() > EXPECTED_KEY_SET_SIZE_WHEN_DONE;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getClientListenerPort()
     */
    @Override
    public int getClientListenerPort() {
        return _lport;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getError()
     */
    @Override
    public String getError() {
        return _error;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getPoolListenerPort()
     */
    @Override
    public int getPoolListenerPort() {
        // This adapter does not listen the second port,
        // it actively connects to the second party
        return _lport;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#setMaxBlockSize(int)
     */
    @Override
    public void setMaxBlockSize(int size) {
        _maxBlockSize = size;
    }

    protected void say(String s) {
        _log.info("ActiveAdapter: " + s);
    }

    protected void esay(String s) {
        _log.error("ActiveAdapter: " + s);
    }

    protected void esay(Throwable t) {
        _log.error(t.getMessage(), t);
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#hasError()
     */
    @Override
    public boolean hasError() {
        return _error != null;
    }

    @Override
    public void setDirClientToPool() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setDirPoolToClient() {
        // TODO Auto-generated method stub

    }

    @Override
    public void setModeE(boolean modeE) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#isAlive()
     */
    @Override
    public boolean isAlive() {
        return _t.isAlive();
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#join()
     */
    @Override
    public void join() throws InterruptedException {
        _t.join();
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#join(long)
     */
    @Override
    public void join(long millis) throws InterruptedException {
        _t.join(millis);
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#start()
     */
    @Override
    public void start() {
        _t.start();
    }

    public String getLocalHost() {
        return _laddr;
    }

    /**
     *
     */
    private class Tunnel {
        //
        private final SocketChannel _scs;
        private final SocketChannel _sct;
        // A pre-allocated buffer for data
        private final ByteBuffer _sbuffer = ByteBuffer.allocate(_maxBlockSize);
        private final ByteBuffer _tbuffer = ByteBuffer.allocate(_maxBlockSize);

        /*
         *
         */
        Tunnel(SocketChannel scs, SocketChannel sct) {
            _scs = scs;
            _sct = sct;
        }

        /*
         *
         */
        public void register(Selector selector) throws ClosedChannelException {
            //
            if (_sct.isConnectionPending()) {
                // Register the target channel with selector, listening for
                // OP_CONNECT events
                _sct.register(selector, SelectionKey.OP_CONNECT, this);
            } else if (_sct.isConnected()) {
                // Register the source channel with the selector, for reading
                _scs.register(selector, SelectionKey.OP_READ, this);
                // System.err.printf("Register %s%n", _scs);
                // Register the target channel with selector, listening for
                // OP_READ events
                _sct.register(selector, SelectionKey.OP_READ, this);
            }
            // System.err.printf("Register %s%n", _sct);
        }

        /*
         *
         */
        public void close()
        {
            if (_selector != null) {
                SelectionKey key;

                key = _scs.keyFor(_selector);
                if (key != null) {
                    key.cancel();
                }

                key = _sct.keyFor(_selector);
                if (key != null) {
                    key.cancel();
                }
            }

            try {
                say("Closing " + _scs.socket());
                _scs.close();
            } catch (IOException ie) {
                esay("Error closing channel " + _scs + ": " + ie);
            }

            try {
                say("Closing " + _sct.socket());
                _sct.close();
            } catch (IOException ie) {
                esay("Error closing channel " + _sct + ": " + ie);
            }
        }

        /*
         *
         */
        public ByteBuffer getBuffer(SocketChannel sc) {
            if (sc == _scs) {
                return _sbuffer;
            } else if (sc == _sct) {
                return _tbuffer;
            } else {
                return null;
            }
        }

        /*
         *
         */
        public SocketChannel getMate(SocketChannel sc) {
            if (sc == _scs) {
                return _sct;
            } else if (sc == _sct) {
                return _scs;
            } else {
                return null;
            }
        }

        /*
         *
         */
        private void processInput(SocketChannel scs) throws IOException
        {
            SocketChannel sct = getMate(scs);
            ByteBuffer b = getBuffer(scs);
            b.clear();

            int r = scs.read(b);
            if (r < 0) {
                say("EOF on channel " + scs + ", shutting down output of " + sct);
                sct.socket().shutdownOutput();
                if (scs.socket().isOutputShutdown()) {
                    close();
                }
            } else if (r > 0) {
                b.flip();
                processOutput(sct);
            } else {
                SelectionKey key = scs.keyFor(_selector);
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }
        }

        /*
         *
         */
        private void processOutput(SocketChannel sct) throws IOException
        {
            SocketChannel scs = getMate(sct);
            ByteBuffer b = getBuffer(scs);

            sct.write(b);
            if (b.hasRemaining()) {
                // Register the output channel for OP_WRITE
                SelectionKey key = sct.keyFor(_selector);
                key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                // System.err.printf("has remaining: set OP_WRITE%n");
            } else {
                // Register the input channel for OP_READ
                SelectionKey key = scs.keyFor(_selector);
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
                // System.err.printf("no remaining: set OP_READ%n");
            }
        }

        public String toString() {
            return _scs.socket().toString() + "<->" + _sct.socket().toString();
        }

    } // class Tunnel

    /*
     *
     */
    @Override
    public void run()
    {
        try {
            // Create a new Selector for selecting
            // _selector = Selector.open();

            // Register the ServerSocketChannel, so we can listen for incoming
            // connections
            _ssc.register(_selector, SelectionKey.OP_ACCEPT);
            say("Listening on port " + _ssc.socket().getLocalPort());

            // Now process the events
            while (isTransferInProgress()) {
                // Watch for either an incoming connection, or incoming data on
                // an existing connection
                int num = _selector.select(5000);
                // System.err.printf("select returned %d%n", num);
                // if (num == 0) continue; // Just in case...
                // Get the keys corresponding to the activity that has been
                // detected, and process them one by one
                Iterator<SelectionKey> selectedKeys = _selector.selectedKeys()
                        .iterator();

                while (selectedKeys.hasNext()) {
                    // Get a key representing one of bits of I/O activity
                    SelectionKey key = selectedKeys.next();

                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    try {
                        processSelectionKey(key);
                    } catch (IOException e) {
                        // Handle error with channel and unregister
                        key.cancel();
                        esay("key processing error");
                    }
                }
                // Process pending accepted sockets and add them to the selector
                processPending();
            }
        } catch (ClosedSelectorException e) {
            // Adapter was forcefully closed; not an error
        } catch (IOException ie) {
            esay(ie);
        } finally {
            closeNow();
        }
    }

    /*
     *
     */
    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssc.accept();
        say("New connection: " + sc.socket());
        addPending(sc);
    }

    /*
     *
     */
    private void finishConnection(SelectionKey key) throws IOException {
        SocketChannel sc = (SocketChannel) key.channel();
        Tunnel tnl = (Tunnel) key.attachment();

        boolean success = sc.finishConnect();

        if (success) {
            say("New connection: " + sc.socket());
            tnl.register(_selector);
        } else {
            // An error occurred; handle it
            esay("Connection error: " + sc.socket());
            tnl.close();
        }
    }

    /*
     *
     */
    public void processSelectionKey(SelectionKey key) throws IOException {
        // System.err.printf("key.readyOps()=%x%n", key.readyOps());
        if (key.isValid() && key.isAcceptable()) {
            // System.out.println("ACCEPT");

            // It's an incoming connection, accept it
            this.accept(key);

        }
        if (key.isValid() && key.isConnectable()) {
            // System.out.println("CONNECT");

            // Get channel with connection request
            this.finishConnection(key);

        }
        if (key.isValid() && key.isReadable()) {
            // Obtain the interest of the key
            // int readyOps = key.readyOps();
            // System.out.println("READ:"+readyOps);
            // Disable the interest for the operation that is ready.
            // This prevents the same event from being raised multiple times.
            // key.interestOps(key.interestOps() & ~readyOps);
            key.interestOps(key.interestOps() & ~SelectionKey.OP_READ);

            // It's incoming data on a connection, process it
            this.read(key);

        }
        if (key.isValid() && key.isWritable()) {
            // Obtain the interest of the key
            // int readyOps = key.readyOps();
            // System.out.println("WRITE:"+readyOps);
            // Disable the interest for the operation that is ready.
            // This prevents the same event from being raised multiple times.
            // key.interestOps(key.interestOps() & ~readyOps);
            key.interestOps(key.interestOps() & ~SelectionKey.OP_WRITE);

            // It's incoming data on a connection, process it
            this.write(key);
        }
    }

    /*
     *
     */
    private void read(SelectionKey key)
    {
        Tunnel tnl = null;
        try {
            // There is incoming data on a connection, process it
            tnl = (Tunnel) key.attachment();
            //
            tnl.processInput((SocketChannel) key.channel());
        } catch (IOException ie) {
            esay("Communication error");
            // On exception, remove this channel from the selector
            tnl.close();
        }
    }

    /*
     *
     */
    private void write(SelectionKey key)
    {
        Tunnel tnl = null;
        try {
            // There is outgoing data on a connection, process it
            tnl = (Tunnel) key.attachment();
            //
            tnl.processOutput((SocketChannel) key.channel());
        } catch (IOException ie) {
            esay("Communication error");
            // On exception, remove this channel from the selector
            tnl.close();
        }
    }

    /*
     *
     */
    void addPending(SocketChannel s) {
        //
        synchronized (_pending) {
            // System.err.printf("addPending: add: %s%n", s);
            _pending.add(s);
            _pending.notify();
        }
    }

    /*
     * Process any targets in the pending list
     */
    private void processPending() throws IOException {
        synchronized (_pending) {
            // System.err.printf("ProcessPending: pending.size=%d%n",
            // _pending.size());
            while (_pending.size() > 0) {
                SocketChannel scs = _pending.removeFirst();
                // Make it non-blocking, so we can use a selector on it.
                scs.configureBlocking(false);
                // System.err.printf("ProcessPending: got %s%n", scs);
                try {
                    _streamsCreated++;

                    // Prepare the socket channel for the target
                    SocketChannel sct = createSocketChannel(_tgtHost, _tgtPort);
                    Tunnel tnl = new Tunnel(scs, sct);
                    tnl.register(_selector);
                } catch (IOException ie) {
                    // Something went wrong..........
                    esay(ie);
                }
            }
        }
    }

    /*
     * Creates a non-blocking socket channel for the specified host name and
     * port and calls the connect() on the new channel before it is returned.
     */
    static public SocketChannel createSocketChannel(String host, int port) throws IOException {
        // Create a non-blocking socket channel
        SocketChannel sc = SocketChannel.open();
        sc.configureBlocking(false);

        // Send a connection request to the server; this method is non-blocking
        sc.connect(new InetSocketAddress(host, port));
        return sc;
    }

    @Override
    public String toString()
    {
        return "active -> " + _tgtHost + ":" + _tgtPort + "; " + _streamsCreated + " streams created";
    }
}
