/*
 * $Id: ProtocolConnectionPoolFactory.java,v 1.1 2006-07-18 09:06:04 tigran Exp $
 */
package org.dcache.net;

import java.io.IOException;

public class ProtocolConnectionPoolFactory {

	
	private static Object _initLock = new Object();
	private static ProtocolConnectionPool _protocolConnectionPool = null;
	
	private int _port;
	ChallengeReader _challengeReader = null;
	
	public ProtocolConnectionPoolFactory(int port, ChallengeReader challengeReader) {
		_port = port;
		_challengeReader = challengeReader;
	}
	
	public ProtocolConnectionPool getConnectionPool() throws IOException {
		synchronized(_initLock){
			if( _protocolConnectionPool == null ) {
				_protocolConnectionPool = new ProtocolConnectionPool(_port, _challengeReader);
				_protocolConnectionPool.start();
			}
		}
		
		return _protocolConnectionPool;
	}
	
	
}
/*
 * $Log: not supported by cvs2svn $
 */