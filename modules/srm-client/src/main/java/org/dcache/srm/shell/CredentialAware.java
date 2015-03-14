package org.dcache.srm.shell;

import eu.emi.security.authn.x509.X509Credential;

/**
 *  Indicates that the class needs an X509 credential to work.
 */
public interface CredentialAware
{
    public void setCredential(X509Credential credential);
}
