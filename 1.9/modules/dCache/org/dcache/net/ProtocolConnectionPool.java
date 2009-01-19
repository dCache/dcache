/*
 * $Id: ProtocolConnectionPool.java,v 1.5 2007-07-04 16:29:31 tigran Exp $
 */
package org.dcache.net;

import java.net.* ;
import java.io.* ;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;


public class ProtocolConnectionPool extends Thread {

	private static Logger _logSocketIO = Logger.getLogger("logger.dev.org.dcache.io.socket");
	private ServerSocketChannel _serverSocketChannel = null;
	private final Map<Object, SocketChannel> _acceptedSockets = new HashMap<Object, SocketChannel>();
	ChallengeReader _challengeReader = null;
	private boolean _stop = false;

	ProtocolConnectionPool(int listenPort, ChallengeReader challengeReader) throws IOException {
		super("ProtocolConnectionPool");
		_challengeReader = challengeReader;
        _serverSocketChannel = ServerSocketChannel.open();


        /*
         * if org.dcache.net.tcp.portrange [first:last] defined
         * try to bind to first empty. If fist > last,
         * then only last port (lowest) is used.
         */
        SocketAddress socketAddress = null;
        if ( listenPort != 0 ) {
             socketAddress =  new InetSocketAddress( listenPort ) ;
            _serverSocketChannel.socket().bind(socketAddress);
        	if( _logSocketIO.isDebugEnabled() ) {
        		_logSocketIO.debug("Socket BIND local = " + _serverSocketChannel.socket().getInetAddress() + ":" + _serverSocketChannel.socket().getLocalPort() );
        	}
        }else{
            String portRange = System.getProperty("org.dcache.net.tcp.portrange");
            int firstPort = 0;
            int lastPort = 0;
            if( portRange != null ) {

                String[] range = portRange.split(":");
                try {
                    firstPort = Integer.parseInt(range[0]);
                    lastPort = Integer.parseInt(range[1]);
                }catch(NumberFormatException nfe) {
                    firstPort = lastPort = 0;
                }

                if( firstPort > lastPort ) {
                    firstPort = lastPort;
                }

            }

            for( int currentPort = firstPort; currentPort <= lastPort; currentPort++) {
                socketAddress =  new InetSocketAddress( currentPort ) ;
                try {
                    _serverSocketChannel.socket().bind(socketAddress);
                   	if( _logSocketIO.isDebugEnabled() ) {
                		_logSocketIO.debug("Socket BIND local = " + _serverSocketChannel.socket().getInetAddress() + ":" + _serverSocketChannel.socket().getLocalPort() );
                	}
                }catch(IOException e) {
                    // port is busy
                    continue;
                }
                break;
            }

            if( !_serverSocketChannel.socket().isBound() ) {
            	_logSocketIO.error("Can't bind Socket");
                throw new IOException("can't bind");
            }

        }
	}


	public SocketChannel getSocket(Object challenge) {

        SocketChannel mySocket = null;

		try {
            // System.out.println("waiting fo :  " + challenge);
			synchronized(_acceptedSockets){

				while( _acceptedSockets.isEmpty() || !_acceptedSockets.containsKey(challenge) ) {
               //     System.out.println("accepted: " + _acceptedSockets.size() );
               //     System.out.println("contains: " + _acceptedSockets.containsKey(challenge) );
					_acceptedSockets.wait();
				}
				mySocket = _acceptedSockets.remove(challenge);
			}

		}catch(Exception e) {

		}
		return mySocket;
	}

    public int getLocalPort() {
        return _serverSocketChannel.socket().getLocalPort();
    }

	public void run() {

		while(!_stop) {

			try {

                SocketChannel newSocketChannel = _serverSocketChannel.accept();
             	if( _logSocketIO.isDebugEnabled() ) {
            		_logSocketIO.debug("Socket OPEN (ACCEPT) remote = " + newSocketChannel.socket().getInetAddress() + ":" + newSocketChannel.socket().getPort() +
            					" local = " +newSocketChannel.socket().getLocalAddress() + ":" + newSocketChannel.socket().getLocalPort() );
            	}
				Object challenge = _challengeReader.getChallenge(newSocketChannel);

				if(challenge == null) {
					// Unable to read challenge....skip connection
	             	if( _logSocketIO.isDebugEnabled() ) {
	            		_logSocketIO.debug("Socket CLOSE (no challenge) remote = " + newSocketChannel.socket().getInetAddress() + ":" + newSocketChannel.socket().getPort() +
	            					" local = " +newSocketChannel.socket().getLocalAddress() + ":" + newSocketChannel.socket().getLocalPort());
	            	}
					newSocketChannel.close();
					continue;
				}
                // System.out.println("recived:  " + challenge);
				synchronized(_acceptedSockets){
					_acceptedSockets.put(challenge, newSocketChannel);
					_acceptedSockets.notifyAll();
					Thread.yield();
				}

			}catch(Exception e) {
				_logSocketIO.error("Accept loop", e);
				_stop = true;
				try {
		         	if( _logSocketIO.isDebugEnabled() ) {
		        		_logSocketIO.debug("Socket SHUTDOWN local = " + _serverSocketChannel.socket().getInetAddress() + ":" + _serverSocketChannel.socket().getLocalPort() );
		        	}
					_serverSocketChannel.close();
				}catch( Exception ignored) {}
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
 * private static Logger _logSocketIO = Logger.getLogger("logger.dev.org.dcache.io.socket");
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
