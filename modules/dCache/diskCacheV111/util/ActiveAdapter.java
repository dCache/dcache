//$Id: ActiveAdapter.java,v 1.4 2007-10-10 09:35:11 behrmann Exp $

//$Log: not supported by cvs2svn $
//Revision 1.3  2007/05/29 21:23:25  podstvkv
//Adapter closing mechanism changed
//

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

package diskCacheV111.util;

import java.io.IOException;
import java.net.BindException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Random;

import dmg.cells.nucleus.CellAdapter;

/**
 * @author V. Podstavkov
 *
 */

public class ActiveAdapter implements Runnable, ProxyAdapter {

    private ServerSocketChannel _ssc; // The ServerSocketChannel we will
                                        // listen on...
    private String _tgtHost = null; // The remote host to connect
    private int _tgtPort = 0; // The remote port to connect
    private String _laddr = null; // Local IP address
    private int _lport; // Local port number
    private int _maxBlockSize = 32768; // Size of the buffers for transfers
    private int _maxStreams = 128; // The maximum number of concurrent streams
                                    // allowed
    private Selector _selector = null;
    private LinkedList<SocketChannel> _pending = new LinkedList<SocketChannel>();
    private String _error;
    private CellAdapter _door; // The cell used for error logging
    private Random _random = new Random(); // Random number generator used when
                                            // binding sockets
    private Thread _t = null; // A thread driving the adapter
    private boolean _closeRequested = false; // Request to close received

    public ActiveAdapter(CellAdapter door) throws IOException {
        this(door, (ServerSocketChannel) null, (String) null, 0);
    }

    /**
     *
     * @param door
     * @param lowPort
     * @param highPort
     * @throws IOException
     */
    public ActiveAdapter(CellAdapter door, int lowPort, int highPort)
            throws IOException {
        _door = door;
        if (lowPort > highPort) {
            throw new IllegalArgumentException("lowPort > highPort");
        }

        say("Port range=" + lowPort + "-" + highPort);
        if (lowPort > 0) {
            /*
             * We randomise the first socket to try to reduce the risk of
             * conflicts and to make the port less predictable.
             */
            int start = _random.nextInt(highPort - lowPort + 1) + lowPort;
            int i = start;
            do {
                try {
                    say("Trying Port " + i);
                    _ssc = ServerSocketChannel.open();
                    _ssc.configureBlocking(false); // Set it to non-blocking,
                                                    // so we can use select
                    _ssc.socket().bind(new InetSocketAddress(i));
                    break;
                } catch (BindException ee) {
                    say("Problems trying port " + i + " " + ee);
                    if (i == highPort) {
                        throw ee;
                    }
                }
                i = (i < highPort ? i + 1 : lowPort);
            } while (i != start);
        } else {
            _ssc = ServerSocketChannel.open();
            _ssc.configureBlocking(false); // Set it to non-blocking, so we can
                                            // use select
            _ssc.socket().bind(null);
        }
        _laddr = InetAddress.getLocalHost().getHostAddress(); // Find the
                                                                // address as a
                                                                // string
        _lport = _ssc.socket().getLocalPort(); // Find the port
        _t = new Thread(this);
        // Create a new Selector for selecting
        _selector = Selector.open();
    }

    /*
     *
     */
    public ActiveAdapter(CellAdapter door, ServerSocketChannel ssc)
            throws IOException {
        this(door, ssc, (String) null, 0);
    }

    /*
     *
     */
    public ActiveAdapter(CellAdapter door, ServerSocketChannel ssc,
            String host, int port) throws IOException {
        //
        _door = door;
        if (ssc == null) {
            _ssc = ServerSocketChannel.open(); // Instead of creating a
                                                // ServerSocket, create a new
                                                // ServerSocketChannel
            _ssc.configureBlocking(false); // Set it to non-blocking, so we can
                                            // use select
            _ssc.socket().bind(null); // Get the Socket connected to this
                                        // channel, and bind to some system
                                        // chosen port
        } else {
            _ssc = ssc;
        }
        _laddr = InetAddress.getLocalHost().getHostAddress(); // Find the
                                                                // address as a
                                                                // string
        _lport = _ssc.socket().getLocalPort(); // Find the port
        _tgtHost = host;
        _tgtPort = port;
        _t = new Thread(this);
        // Create a new Selector for selecting
        _selector = Selector.open();
    }

    // <!--

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#acceptOnClientListener()
     */
    public Socket acceptOnClientListener() throws IOException {
        return _ssc.accept().socket();
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#close()
     */
    public void close() {
        say("Close request received...");
        // say("# of keys = "+_selector.keys().size());
        _closeRequested = true;
        _selector.wakeup();
        return;
    }

    private void _close() {
        say("Closing listener socket");
        // say("Still have "+_selector.keys().size()+" keys");
        try {
            if (_ssc != null) {
                _ssc.close();
                _ssc = null;
            }
        } catch (IOException e) {
            esay("_clientListenerSock.close() failed with IOException, ignoring");
            // esay(e);
        }
    }

    /**
     * Check if transfer is still in progress
     *
     * @return
     */
    private boolean transferInProgress() {
        if (_closeRequested == true && _selector.keys().size() < 2) {
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getClientListenerPort()
     */
    public int getClientListenerPort() {
        return _lport;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getError()
     */
    public String getError() {
        return _error;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#getPoolListenerPort()
     */
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
    public void setMaxBlockSize(int size) {
        _maxBlockSize = size;
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#setMaxStreams(int)
     */
    public void setMaxStreams(int n) {
        _maxStreams = n;

    }

    protected void say(String s) {
        if (_door == null) {
            System.out.println("ActiveAdapter: " + s);
        } else {
            _door.say("ActiveAdapter: " + s);
        }
    }

    protected void esay(String s) {
        if (_door == null) {
            System.err.println("ActiveAdapter: " + s);
        } else {
            _door.esay("ActiveAdapter: " + s);
        }
    }

    protected void esay(Throwable t) {
        if (_door == null) {
            System.err.println("ActiveAdapter exception:");
            System.err.println(t);
        } else {
            _door.esay("ActiveAdapter exception:");
            _door.esay(t);
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#hasError()
     */
    public boolean hasError() {
        return _error != null;
    }

    public void setDirClientToPool() {
        // TODO Auto-generated method stub

    }

    public void setDirPoolToClient() {
        // TODO Auto-generated method stub

    }

    public void setModeE(boolean modeE) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#isAlive()
     */
    public boolean isAlive() {
        return _t.isAlive();
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#join()
     */
    public void join() throws InterruptedException {
        _t.join();
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#join(long)
     */
    public void join(long millis) throws InterruptedException {
        _t.join(millis);
    }

    /*
     * (non-Javadoc)
     *
     * @see diskCacheV111.util.ProxyAdapter#start()
     */
    public void start() {
        _t.start();
    }

    // -->

    /*
     *
     */
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
        public void cancel(Selector selector) {
            //
            SelectionKey key = _scs.keyFor(selector);
            if (key != null)
                key.cancel();
            try {
                _scs.close();
                say("Close " + _scs);
            } catch (IOException ie) {
                esay("Error closing channel " + _scs + ": " + ie);
            }

            key = _sct.keyFor(selector);
            if (key != null)
                key.cancel();
            try {
                _sct.close();
                say("Close " + _sct);
            } catch (IOException ie) {
                esay("Error closing channel " + _sct + ": " + ie);
            }
        }

        /*
         *
         */
        public ByteBuffer getBuffer(SocketChannel sc) {
            if (sc == _scs)
                return _sbuffer;
            else if (sc == _sct)
                return _tbuffer;
            else
                return null;
        }

        /*
         *
         */
        public SocketChannel getMate(SocketChannel sc) {
            if (sc == _scs)
                return _sct;
            else if (sc == _sct)
                return _scs;
            else
                return null;
        }

        /*
         *
         */
        private boolean processInput(SocketChannel scs) throws IOException {
            //
            SocketChannel sct = getMate(scs);
            boolean ok = true;
            ByteBuffer b = this.getBuffer(scs);
            b.clear();

            int r = scs.read(b);

            if (r < 0) {
                esay("Can't read from " + scs);
                return false;
            } else if (r > 0) {
                b.flip();

                // System.out.println("Read "+r+" from "+scs);
                ok = send(sct);

            } else {
                // System.err.printf("Read 0 %s%n", scs);
                SelectionKey key = scs.keyFor(_selector);
                key.interestOps(key.interestOps() | SelectionKey.OP_READ);
            }

            return ok;
        }

        /*
         *
         */
        private boolean send(SocketChannel sct) throws IOException {
            //
            SocketChannel scs = getMate(sct);
            ByteBuffer b = this.getBuffer(scs);

            int r = sct.write(b);
            // System.err.printf("Wrote %d to %s%n", r, sct);
            if (r < 0) {
                esay("Can't write to " + sct);
                return false;
            }
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
            return true;
        }

        /*
         *
         */
        private boolean processOutput(SocketChannel sct) throws IOException {
            //
            return send(sct);
        }

        public String toString() {
            return _scs.toString() + "<->" + _sct.toString();
        }

    } // class Tunnel

    /*
     *
     */
    public void run() {
        //
        try {
            // Create a new Selector for selecting
            // _selector = Selector.open();

            // Register the ServerSocketChannel, so we can listen for incoming
            // connections
            _ssc.register(_selector, SelectionKey.OP_ACCEPT);
            say("Listening on port " + _ssc.socket().getLocalPort());

            // Now process the events in the infinite loop
            while (transferInProgress()) {
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
            _close();
        } catch (IOException ie) {
            esay(ie);
        }
    }

    /*
     *
     */
    private void accept(SelectionKey key) throws IOException {
        ServerSocketChannel ssc = (ServerSocketChannel) key.channel();
        SocketChannel sc = ssc.accept();
        // System.out.println("Got connection from "+sc);
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
            say("New connection: " + sc);
            tnl.register(_selector);
        } else {
            // An error occurred; handle it
            esay("Connection error: " + sc);
            tnl.cancel(_selector);
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
    private void read(SelectionKey key) throws IOException {
        Tunnel tnl = null;
        try {
            // There is incoming data on a connection, process it
            tnl = (Tunnel) key.attachment();
            //
            boolean ok = tnl.processInput((SocketChannel) key.channel());
            //
            // If the connection is dead, then remove it from the selector and
            // close it
            if (!ok) {
                // System.err.printf("Connection %s is dead%n", tnl);
                tnl.cancel(_selector);
            }
        } catch (IOException ie) {
            esay("Communication error");
            // On exception, remove this channel from the selector
            tnl.cancel(_selector);
        }
    }

    /*
     *
     */
    private void write(SelectionKey key) throws IOException {
        Tunnel tnl = null;
        try {
            // There is outgoing data on a connection, process it
            tnl = (Tunnel) key.attachment();
            //
            boolean ok = tnl.processOutput((SocketChannel) key.channel());
            //
            // If the connection is dead, then remove it from the selector and
            // close it
            if (!ok) {
                // System.err.printf("Connection %s is dead%n", tnl);
                tnl.cancel(_selector);
            }
        } catch (IOException ie) {
            esay("Communication error");
            // On exception, remove this channel from the selector
            tnl.cancel(_selector);
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
     *
     */
    public void setDestination(String host, int port) {
        _tgtHost = host;
        _tgtPort = port;
        _selector.wakeup();
    }

    /*
     * Process any targets in the pending list
     */
    private void processPending() throws IOException {
        if (_tgtHost == null || _tgtPort == 0)
            return;
        synchronized (_pending) {
            // System.err.printf("ProcessPending: pending.size=%d%n",
            // _pending.size());
            while (_pending.size() > 0) {
                SocketChannel scs = _pending.removeFirst();
                // Make it non-blocking, so we can use a selector on it.
                scs.configureBlocking(false);
                // System.err.printf("ProcessPending: got %s%n", scs);
                try {
                    // Prepare the socket channel for the target
                    SocketChannel sct = createSocketChannel(_tgtHost, _tgtPort);
                    Tunnel tnl = new Tunnel(scs, sct);
                    tnl.register(_selector);
                } catch (IOException ie) {
                    // Something went wrong..........
                    ie.printStackTrace();
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

    /*
     *
     */
    static public void main(String args[]) throws Exception {

        String rsvrHost = args[0]; // Data receiver host to connect
        int rsvrPort = Integer.parseInt(args[1]); // Data receiver port to
                                                    // connect

        // Create the adapter
        ActiveAdapter aa = new ActiveAdapter((CellAdapter) null);

        System.out.printf("ActiveAdaper is listening on %s:%d%n", aa
                .getLocalHost(), aa.getClientListenerPort());

        // Start the adapter
        aa.start();

        // The receiver address can be set even after the adapter started
        // For example when the adapter is used to store the data the pool is
        // not known in advance
        //
        System.out.printf("Set destination: %s, %d%n", rsvrHost, rsvrPort);
        aa.setDestination(rsvrHost, rsvrPort);
    }
}
