/*
 * $Id: ProtocolConnectionPool.java,v 1.5 2007-07-04 16:29:31 tigran Exp $
 */
package org.dcache.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.AsynchronousCloseException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import org.dcache.util.PortRange;

import static com.google.common.base.Preconditions.checkState;

public class ProtocolConnectionPool implements Runnable {

    private static final Logger _logSocketIO = LoggerFactory.getLogger("logger.dev.org.dcache.io.socket");
    private final Map<Object, SocketChannel> _acceptedSockets = new HashMap<>();
    private final ChallengeReader _challengeReader;
    private final int _receiveBufferSize;
    private final PortRange _portRange;

    private int _port;
    private ServerSocketChannel _serverChannel;
    private long _activity;
    private Thread _thread;

    /**
     * Represent a "promise" to listen for incoming connections until closed.
     * This object is not thread-safe.
     */
    public class Listen implements Closeable
    {
        private boolean _released;

        /**
         * Indicate that promise is no longer needed.
         */
        @Override
        public void close()
        {
            if (!_released) {
                release();
                _released = true;
            }
        }

        /**
         * Get TCP port number used by this connection pool.
         * @return port number
         */
        public int getPort()
        {
            return ProtocolConnectionPool.this.getPort();
        }

        /**
         * Get a {@link SocketChannel} identified by <code>challenge</code>. The
         * caller will block until client is connected and challenge exchange is done.
         *
         * @param challenge the identifier the client is required to present
         * @return {@link SocketChannel} connected to client
         * @throws InterruptedException if current thread was interrupted
         */
        public SocketChannel getSocket(Object challenge) throws InterruptedException
        {
            checkState(!_released);

            assert _activity > 0;
            assert _thread != null;
            assert _serverChannel != null;

            synchronized (_acceptedSockets) {
                while (!_acceptedSockets.containsKey(challenge)) {
                    _acceptedSockets.wait();
                }
                return _acceptedSockets.remove(challenge);
            }
        }
   }

    /**
     * Create a new ProtocolConnectionPool on specified TCP port. If <code>listenPort</code>
     * is zero, then random port is used unless <i>org.dcache.net.tcp.portrange</i>
     * property is set. The {@link ChallengeReader} is used to associate connections
     * with clients.
     *
     * @param port the port on which to listen; 0 use a default range
     * @param bufferSize the size of the receive buffer; 0 implies a default value.
     * @param reader the ChallengeReader to extract challenges.
     */
    ProtocolConnectionPool(int port, int bufferSize, ChallengeReader reader)
    {
        _challengeReader = reader;
        _receiveBufferSize = bufferSize;

        if (port != 0) {
            _portRange = new PortRange(port);
        } else {
            String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");

            if (dcachePorts != null) {
                _portRange = PortRange.valueOf(dcachePorts);
            } else {
                _portRange = new PortRange(0);
            }
        }
    }

    private ServerSocketChannel open() throws IOException
    {
        ServerSocketChannel channel = ServerSocketChannel.open();
        ServerSocket socket = channel.socket();

        if (_receiveBufferSize > 0) {
            socket.setReceiveBufferSize(_receiveBufferSize);
        }

        if (_port != 0) {
            try {
                socket.bind(new InetSocketAddress((InetAddress)null, _port));
            } catch (IOException e) {
                _logSocketIO.debug("Failed to bind to existing port: {}", e.toString());
            }
        }

        if (!socket.isBound()) {
            _port = _portRange.bind(socket);
        }

        _logSocketIO.debug("Socket BIND local = {}:{}", socket.getInetAddress(),
                _port);

        return channel;
    }

    private void close(ServerSocketChannel channel)
    {
        _logSocketIO.debug("Socket SHUTDOWN local = {}:{}",
                channel.socket().getInetAddress(),
                channel.socket().getLocalPort());
        try {
            channel.close();
        } catch (IOException e) {
            _logSocketIO.warn("Failed to close socket: {}", e.toString());
        }
    }

    private synchronized int getPort()
    {
        return _port;
    }

    /**
     * Acquire a "promise" to accept an incoming connection.  This may trigger
     * opening a TCP port for incoming connections and starting a thread that
     * will handle incoming connections.
     */
    public synchronized Listen acquire() throws IOException
    {
        if (_serverChannel == null) {
            _serverChannel = open();
        }

        if (_thread == null) {
            _thread = new Thread(this, "ProtocolConnectionPool");
            _thread.start();
        }
        _activity++;
        return new Listen();
    }

    private synchronized void release()
    {
        if (_activity == 1) {
            if (_thread != null) {
                _thread.interrupt();
                _thread = null;
            }

            if (_serverChannel != null) {
                close(_serverChannel);
                _serverChannel = null;
            }
        }

        if (_activity > 0) {
            _activity--;
        }
    }

    @Override
    public void run() {
        ServerSocketChannel serverChannel;
        synchronized (this) {
            serverChannel = _serverChannel;
        }

        try {
            while (true) {
                SocketChannel channel = serverChannel.accept();
                _logSocketIO.debug("Socket OPEN (ACCEPT) remote = {}:{} local = {}:{}",
                        channel.socket().getInetAddress(), channel.socket().getPort(),
                        channel.socket().getLocalAddress(), channel.socket().getLocalPort());

                Object challenge = _challengeReader.getChallenge(channel);
                if (challenge == null) {
                    // Unable to read challenge....skip connection
                    _logSocketIO.debug("Socket CLOSE (no challenge) remote = {}:{} local = {}:{}",
                            channel.socket().getInetAddress(), channel.socket().getPort(),
                            channel.socket().getLocalAddress(), channel.socket().getLocalPort());
                    try {
                        channel.close();
                    } catch (IOException e) {
                        _logSocketIO.info("Failed to close client socket: {}",
                                channel.socket());
                    }
                } else {
                    synchronized (_acceptedSockets) {
                        _acceptedSockets.put(challenge, channel);
                        _acceptedSockets.notifyAll();
                    }
                }
            }
        } catch (AsynchronousCloseException e) {
            // Ignore thread stopped by interrupting or by closing the channel
        } catch (IOException e) {
            _logSocketIO.error("Accept loop", e);
        }
    }
}