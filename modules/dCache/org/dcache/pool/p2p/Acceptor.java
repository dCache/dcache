package org.dcache.pool.p2p;

import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ServerSocketChannel;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final AtomicInteger _nextId = new AtomicInteger(100);
    private Thread _worker;

    private ServerSocketChannel _serverChannel;
    private int _port = 0;
    private String _error;

    Acceptor()
    {
    }

    /**
     * Register companion and returns a session ID for this companion.
     */
    synchronized int register(Companion companion)
    {
        if (_sessions.isEmpty()) {
            start();
        }

        int id = _nextId.getAndIncrement();
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
     * Sets the port on which the acceptor is to listen.
     */
    synchronized void setPort(int port)
    {
        if (_serverChannel != null)
            throw new IllegalStateException("Acceptor already running");
        _port = port;
    }

    /**
     * Returns the port on which the acceptor is configured to listen.
     * May return zero, in which a free port is automatically chosen.
     */
    synchronized int getPort()
    {
        return _port;
    }

    /**
     * Returns the socket address on which the acceptor listens.
     */
    synchronized InetSocketAddress getSocketAddress()
        throws UnknownHostException
    {
        if (_serverChannel == null)
            throw new IllegalStateException("Acceptor not running");

        return new InetSocketAddress(InetAddress.getLocalHost(),
                                     _serverChannel.socket().getLocalPort());
    }

    /**
     * Starts the acceptor.
     */
    synchronized private void start()
    {
        if (_worker == null) {
            try {
                _serverChannel = ServerSocketChannel.open();
                _serverChannel.socket().bind(new InetSocketAddress(_port));

                _worker = new Thread(this, "Acceptor");
                _worker.start();
            } catch (IOException e) {
                _error = e.getMessage();
                _log.error("Problem in opening server socket: " + e);
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
                            Companion companion = _sessions.get(in.readInt());
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
        if (_error != null)
            return "Error: " + _error;
        if (_serverChannel == null)
            return "Port: " + _port;
        return "Port: " + _serverChannel.socket().getLocalPort();
    }
}
