package org.dcache.srm.request;

/**
 * A class that knows about a delegated credential.
 */
public interface DelegatedCredentialAware
{
    /**
     * The ID of the delegated credential or null if no such credential
     * is available.
     */
    Long getCredentialId();
}
