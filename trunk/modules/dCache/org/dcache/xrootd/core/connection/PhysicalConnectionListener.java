package org.dcache.xrootd.core.connection;

import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

public interface PhysicalConnectionListener {
	
	public void handshakeRequest();
	public boolean loginRequest(LoginRequest login);
	public boolean authRequest(AuthentiticationRequest auth);
	public StreamListener newStreamForked(int streamID);
	public void closeConnection();
	
}
