/*
 * $Id: ProtocolConnectionPool.java,v 1.1.2.3 2006-10-04 09:57:06 tigran Exp $
 */
package org.dcache.net;

import java.net.* ;
import java.io.* ;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Map;


public class ProtocolConnectionPool extends Thread {
	
	
	private ServerSocketChannel _serverSocketChannel = null;
	private Map _acceptedSockets = new HashMap();
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
                }catch(IOException e) {
                    // port is busy
                    continue;
                }
                break;
            }
            
            if( !_serverSocketChannel.socket().isBound() ) {
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
				mySocket = (SocketChannel)_acceptedSockets.remove(challenge);				
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
				Object challenge = _challengeReader.getChallenge(newSocketChannel);
				
				if(challenge == null) {
					// unabe to read chalange....skip connection
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
				e.printStackTrace();
				_stop = true;
				try {
					_serverSocketChannel.close();
				}catch( Exception ignored) {}
			}
		}
	}
}
/*
 * $Log: not supported by cvs2svn $
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
