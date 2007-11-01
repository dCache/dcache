package org.dcache.xrootd.core.connection;

import org.dcache.xrootd.core.stream.GenericStreamListener;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

public class GenericConnectionListener implements
		PhysicalConnectionListener {

	public void handshakeRequest() {}

	public boolean loginRequest(LoginRequest login) {return true;}

	public boolean authRequest(AuthentiticationRequest auth) {return true;}
	
	public StreamListener newStreamForked(int streamID) {
		return new GenericStreamListener();
	}

	public void closeConnection() {}

}
