/*
 * $Id: ProtocolConnectionPoolFactory.java,v 1.1 2006-07-18 09:06:04 tigran Exp $
 */
package org.dcache.net;

import java.io.IOException;

public class ProtocolConnectionPoolFactory {


	private static final Object _initLock = new Object();
	private static ProtocolConnectionPool _protocolConnectionPool;

	private int _port;
	ChallengeReader _challengeReader;

	public ProtocolConnectionPoolFactory(int port, ChallengeReader challengeReader) {
		_port = port;
		_challengeReader = challengeReader;
	}

	public ProtocolConnectionPool getConnectionPool(int receiveBufferSize)
            throws IOException
        {
		synchronized(_initLock){
			if( _protocolConnectionPool == null ) {
                                _protocolConnectionPool = new ProtocolConnectionPool(_port, receiveBufferSize, _challengeReader);
				_protocolConnectionPool.start();
			}
		}

		return _protocolConnectionPool;
	}


}
/*
 * $Log: not supported by cvs2svn $
 */
