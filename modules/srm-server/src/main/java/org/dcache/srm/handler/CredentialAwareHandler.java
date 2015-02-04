package org.dcache.srm.handler;

import org.dcache.srm.request.RequestCredential;

/**
 * A handler that needs to know about any delegated credential the user has.
 */
public interface CredentialAwareHandler
{
    public void setCredential(RequestCredential credential);
}
