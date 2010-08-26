package org.dcache.pool.p2p;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.net.BindException;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import java.util.HashMap;

import org.dcache.util.PortRange;

import org.apache.log4j.Logger;

/**
 * Helper class for the P2PClient. Listens for connections from source
 * pools and dispatches those to the appropriate companion.
 */
class Acceptor implements Runnable
{
    private final static Logger _log = Logger.getLogger(Acceptor.class);

    private final Map<Integer, Companion> _sessions =
        new HashMap<Integer, Companion>();
    private int _nextId = 100;
    private Thread _worker;

    private ServerSocketChannel _serverChannel;

    /**
     * The socket address the acceptor will listen to for incomming
     * connections.
     */
    private InetSocketAddress _address;

    /**
     * Port range as defined by the org.dcache.net.tcp.portrange
     * property.
     */
    private final PortRange _portRange;

    private String _error;

    Acceptor()
    {
        String range = System.getProperty("org.dcache.net.tcp.portrange");
        if (range == null || range.equals(""))
            _portRange = new PortRange(0);
        else
            _portRange = PortRange.valueOf(range);
    }

    /**
     * Register companion and returns a session ID for this companion.
     */
    synchronized int register(Companion companion)
        throws IOException
    {
        if (_sessions.isEmpty()) {
            start();
        }

        int id = _nextId++;
        _sessions.put(id, companion);
        return id;
    }

    /**
     * Unregisters a companion with the given session ID.
     */
    synchronized void unregister(int id)
    {
        _sessions.remove(id);
        if (_sessions.isEmpty()) {
            try {
                stop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Sets the address the Acceptor will listen on. If the port is
     * zero, a free port is selected within the range defined by
     * org.dcache.net.tcp.portrange, or a free port is selected by the
     * OS if the range is not defined.
     */
    synchronized void setAddress(InetSocketAddress address)
    {
        if (address.isUnresolved())
            throw new IllegalArgumentException("Address must be resolved");

        _address = address;
    }

    /**
     * Returns the address set with the setAddress method.
     */
    synchronized InetSocketAddress getAddress()
    {
        return _address;
    }

    /**
     * Returns the socket address on which the acceptor listens.
     */
    synchronized InetSocketAddress getSocketAddress()
    {
        if (_serverChannel == null)
            throw new IllegalStateException("Acceptor not running");

        return (InetSocketAddress)
            _serverChannel.socket().getLocalSocketAddress();
    }

    synchronized Companion getSession(int id)
    {
        return _sessions.get(id);
    }

    /**
     * Starts the acceptor.
     */
    synchronized private void start()
        throws IOException
    {
        if (_worker == null) {
            try {
                _serverChannel = ServerSocketChannel.open();
                try {
                    _portRange.bind(_serverChannel.socket(), _address);
                    _worker = new Thread(this, "Acceptor");
                    _worker.start();
                } finally {
                    /* In case something failed we shut everything down.
                     */
                    if (_worker == null) {
                        try {
                            _serverChannel.close();
                        } catch (IOException e) {
                            _log.warn("Failure closing socket: " + e);
                        } finally {
                            _serverChannel = null;
                        }
                    }
                }
            } catch (IOException e) {
                _error = e.getMessage();
                _log.error("Problem in opening server socket: " + e);
                throw e;
            }
        }
    }

    /**
     * Stops the acceptor. Blocks untils the worker thread has stopped.
     */
    synchronized private void stop()
        throws InterruptedException
    {
        if (_worker != null) {
            try {
                _serverChannel.close();
                _worker.join();
            } catch (IOException e) {
                _log.warn("Failure closing socket: " + e);
            } finally {
                _serverChannel = null;
                _worker = null;
            }
        }
    }

    public void run()
    {
        try {
            _error = null;
            while (true) {
                final Socket socket = _serverChannel.accept().socket();
                socket.setKeepAlive(true);

                new Thread("P2P Transfer") {
                    public void run()
                    {
                        try {
                            DataInputStream in =
                                new DataInputStream(socket.getInputStream());
                            DataOutputStream out =
                                new DataOutputStream(socket.getOutputStream());
                            Companion companion = getSession(in.readInt());
                            if (companion == null) {
                                _log.warn("Unsolicited connection from " +
                                          socket.getRemoteSocketAddress());
                            } else {
                                companion.transfer(in, out);
                            }
                        } catch (IllegalStateException e) {
                            _log.error("Transfer denied: " + e.getMessage());
                        } catch (IOException e) {
                            /* This happens if we fail to read the
                             * session ID. Not much we can do about
                             * this except log the failure.
                             */
                            _log.error("Failed to read from "
                                       + socket.getRemoteSocketAddress()
                                       + ": " + e.getMessage());
                        } finally {
                            try {
                                socket.close();
                            } catch (IOException e) {
                                // take it easy
                            }
                        }
                    }
                }.start();
            }
        } catch (AsynchronousCloseException e) {
            /* This is expected when the socket is closed.
             */
        } catch (IOException e) {
            _error = e.getMessage();
            _log.error("Problem in accepting connection : " + e);
        } catch (Exception e) {
            _error = e.getMessage();
            _log.fatal("Bug detected: " + e, e);
        }
    }

    synchronized public String toString()
    {
        assert _worker == null || _serverChannel != null;

        if (_error != null)
            return _error;
        if (_worker == null)
            return _address.toString();
        return _serverChannel.socket().getLocalSocketAddress().toString();
    }
}
