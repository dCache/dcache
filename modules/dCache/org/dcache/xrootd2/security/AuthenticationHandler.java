package org.dcache.xrootd2.security;

import org.dcache.xrootd2.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd2.protocol.messages.AuthenticationRequest;

public interface AuthenticationHandler {
    public AbstractResponseMessage authenticate(AuthenticationRequest request);
    public String getProtocol();
}
