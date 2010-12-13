package org.dcache.xrootd2.security;

import javax.security.auth.Subject;

import org.dcache.xrootd2.protocol.messages.AbstractResponseMessage;
import org.dcache.xrootd2.protocol.messages.AuthenticationRequest;

public interface AuthenticationHandler {

    /**
     * Authenticate method, parsing the requests and creating adequate
     * responses. The internal state of the handler might be changed by this
     * method.
     *
     * @param request Request received from client
     * @return Response to be sent to the client
     */
    public AbstractResponseMessage authenticate(AuthenticationRequest request);

    /**
     * @return the protocol that is implemented by the authentication handler
     */
    public String getProtocol();

    /**
     * Get the subject containing the credentials/principals found during
     * authentication. It is recommended to check whether authentication is
     * completed before calling this method, or otherwise the subject may
     * contain no or partial information.
     * @return Subject populated during authentication
     */
    public Subject getSubject();

    /**
     * @return true if the authentication process is completed, false otherwise
     */
    public boolean isAuthenticationCompleted();

    /**
     * @return true if the provided authentication is strong
     */
    public boolean isStrongAuthentication();
}
