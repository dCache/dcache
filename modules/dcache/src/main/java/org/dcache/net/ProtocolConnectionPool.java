/*
 * $Id: ProtocolConnectionPool.java,v 1.5 2007-07-04 16:29:31 tigran Exp $
 */
package org.dcache.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;

import org.dcache.util.PortRange;

public class ProtocolConnectionPool extends Thread {

    private static final Logger _logSocketIO = LoggerFactory.getLogger("logger.dev.org.dcache.io.socket");
    private final ServerSocketChannel _serverSocketChannel;
    private final Map<Object, SocketChannel> _acceptedSockets = new HashMap<>();
    private final ChallengeReader _challengeReader;
    private boolean _stop;

    /**
     * Create a new ProtocolConnectionPool on specified TCP port. If <code>listenPort</code>
     * is zero, then random port is used unless <i>org.dcache.net.tcp.portrange</i>
     * property is set. The {@link ChallengeReader} is used to associate connections
     * with clients.
     *
     * @param listenPort
     * @param challengeReader
     * @throws IOException
     */
    ProtocolConnectionPool(int listenPort, int receiveBufferSize,
                           ChallengeReader challengeReader)
        throws IOException
    {
        super("ProtocolConnectionPool");
        _challengeReader = challengeReader;
        _serverSocketChannel = ServerSocketChannel.open();
        if (receiveBufferSize > 0) {
            _serverSocketChannel.socket().setReceiveBufferSize(receiveBufferSize);
        }

        PortRange portRange;
        if (listenPort != 0) {
            portRange = new PortRange(listenPort);
        } else {
            String dcachePorts = System.getProperty("org.dcache.net.tcp.portrange");

            if (dcachePorts != null) {
                portRange = PortRange.valueOf(dcachePorts);
            } else {
                portRange = new PortRange(0);
            }
        }

        portRange.bind(_serverSocketChannel.socket());
        if (_logSocketIO.isDebugEnabled()) {
            _logSocketIO.debug("Socket BIND local = " + _serverSocketChannel.socket().getInetAddress() + ":" + _serverSocketChannel.socket().getLocalPort());
        }

    }

    /**
     * Get a {@link SocketChannel} identified by <code>chllenge</code>. The
     * caller will block until client is connected and challenge exchange is done.
     *
     * @param challenge
     * @return {@link SocketChannel} connected to client
     * @throws InterruptedException if current thread was interrupted
     */
    public SocketChannel getSocket(Object challenge) throws InterruptedException {

        synchronized (_acceptedSockets) {

            while (_acceptedSockets.isEmpty() || !_acceptedSockets.containsKey(challenge)) {
                _acceptedSockets.wait();
            }
            return  _acceptedSockets.remove(challenge);
        }
    }

    /**
     * Get TCP port number used by this connection pool.
     * @return port number
     */
    public int getLocalPort() {
        return _serverSocketChannel.socket().getLocalPort();
    }

    @Override
    public void run() {

        try {
            while (!_stop) {
                SocketChannel newSocketChannel = _serverSocketChannel.accept();
                if (_logSocketIO.isDebugEnabled()) {
                    _logSocketIO.debug("Socket OPEN (ACCEPT) remote = " + newSocketChannel.socket().getInetAddress() + ":" + newSocketChannel.socket().getPort() +
                            " local = " + newSocketChannel.socket().getLocalAddress() + ":" + newSocketChannel.socket().getLocalPort());
                }
                Object challenge = _challengeReader.getChallenge(newSocketChannel);

                if (challenge == null) {
                    // Unable to read challenge....skip connection
                    if (_logSocketIO.isDebugEnabled()) {
                        _logSocketIO.debug("Socket CLOSE (no challenge) remote = " + newSocketChannel.socket().getInetAddress() + ":" + newSocketChannel.socket().getPort() +
                                " local = " + newSocketChannel.socket().getLocalAddress() + ":" + newSocketChannel.socket().getLocalPort());
                    }
                    newSocketChannel.close();
                    continue;
                }

                synchronized (_acceptedSockets) {
                    _acceptedSockets.put(challenge, newSocketChannel);
                    _acceptedSockets.notifyAll();
                    Thread.yield();
                }

            }
        } catch (ClosedByInterruptException e) {
            // Shutdown while waiting for client to connect.
        } catch (IOException e) {
            _logSocketIO.error("Accept loop", e);
            try {
                _logSocketIO.debug("Socket SHUTDOWN local = {}:{}",
                        _serverSocketChannel.socket().getInetAddress(),
                        _serverSocketChannel.socket().getLocalPort());
                _serverSocketChannel.close();
            } catch (IOException ignored) {
            }
        }
    }
}
/*
 * $Log: not supported by cvs2svn $
 * Revision 1.4  2007/05/24 13:51:12  tigran
 * merge of 1.7.1 and the head
 *
 * Revision 1.1.2.3.2.2  2007/03/01 14:02:40  tigran
 * fixed local port in debug message
 *
 * Revision 1.1.2.3.2.1  2007/02/16 22:20:35  tigran
 * paranoid network traceing:
 *
 * all binds, accepts, connects and closees in dcap and FTP (nio code only)
 * used logging category:
 *
 * private static Logger _logSocketIO = LoggerFactory.getLogger("logger.dev.org.dcache.io.socket");
 *
 * TODO: find and trace others as well
 * all calls surrounded with
 * if ( _logSocketIO.isDebugEnabled ){
 * ....
 * }
 * so no performance penalty if debug switched off
 * log example (passive dcap):
 *
 * 16 Feb 2007 22:51:33 logger.dev.org.dcache.io.socket org.dcache.net.ProtocolConnectionPool.<init>(ProtocolConnectionPool.java:66) Socket BIND local = /0.0.0.0:33115
 * 16 Feb 2007 22:51:33 logger.dev.org.dcache.io.socket org.dcache.net.ProtocolConnectionPool.run(ProtocolConnectionPool.java:118) Socket OPEN (ACCEPT) remote = /127.0.0.2:11930 local = /127.0.0.2:11930
 * 16 Feb 2007 22:51:33 logger.dev.org.dcache.io.socket diskCacheV111.movers.DCapProtocol_3_nio.runIO(DCapProtocol_3_nio.java:766) Socket CLOSE remote = /127.0.0.2:11930 local = /127.0.0.2:11930
 * 16 Feb 2007 22:51:57 logger.dev.org.dcache.io.socket org.dcache.net.ProtocolConnectionPool.run(ProtocolConnectionPool.java:118) Socket OPEN (ACCEPT) remote = /127.0.0.2:11934 local = /127.0.0.2:11934
 * 16 Feb 2007 22:51:57 logger.dev.org.dcache.io.socket diskCacheV111.movers.DCapProtocol_3_nio.runIO(DCapProtocol_3_nio.java:766) Socket CLOSE remote = /127.0.0.2:11934 local = /127.0.0.2:11934
 * 16 Feb 2007 22:52:06 logger.dev.org.dcache.io.socket org.dcache.net.ProtocolConnectionPool.run(ProtocolConnectionPool.java:144) Socket SHUTDOWN local = /0.0.0.0:33115
 *
 * Revision 1.1.2.3  2006/10/04 09:57:06  tigran
 * fixed first/last port range index
 *
 * Revision 1.1.2.2  2006/08/22 13:43:34  tigran
 * added port range for passive DCAP
 * rmoved System.out
 *
 * Revision 1.2  2006/07/21 12:07:53  tigran
 * added port range support for multple pools on one host
 * to enable port range follofing java properies have to be defined:
 *
 * i) org.dcache.dcap.port=0
 * ii) org.dcache.net.tcp.portrange=<first>:<last>
 *
 * Revision 1.1  2006/07/18 09:06:04  tigran
 * added protocol connection pool
 *
 */
