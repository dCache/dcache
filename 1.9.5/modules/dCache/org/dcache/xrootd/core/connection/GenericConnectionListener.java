package org.dcache.xrootd.core.connection;

import org.dcache.xrootd.core.stream.GenericStreamListener;
import org.dcache.xrootd.core.stream.StreamListener;
import org.dcache.xrootd.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd.protocol.messages.AuthentiticationRequest;
import org.dcache.xrootd.protocol.messages.LoginRequest;
import org.dcache.xrootd.protocol.messages.OKResponse;

public class GenericConnectionListener implements
                                           PhysicalConnectionListener {

    public void handshakeRequest() {}

    public AbstractResponseMessage loginRequest(LoginRequest login)
    {
        return new OKResponse(login.getStreamID());
    }

    public AbstractResponseMessage authRequest(AuthentiticationRequest auth) {
        return new OKResponse(auth.getStreamID());
    }

    public StreamListener newStreamForked(int streamID) {
        return new GenericStreamListener();
    }

    public void closeConnection() {}

}
