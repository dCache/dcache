package org.dcache.xrootd.core.connection;

import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;

public interface PhysicalConnectionListener {

    public void handshakeRequest();
    public AbstractResponseMessage loginRequest(LoginRequest login);
    public AbstractResponseMessage authRequest(AuthentiticationRequest auth);
    public StreamListener newStreamForked(int streamID);
    public void closeConnection();

}
