package org.dcache.services.login;

import java.util.Set;
import java.security.Principal;

import diskCacheV111.vehicles.Message;

/**
 * Requests a reverse map operation by a login cell.
 */
public class ReverseMapMessage extends Message
{
    private static final long serialVersionUID = -9031071645898374562L;

    private final Principal _principal;
    private Set<Principal> _mappedPrincipals;

    public ReverseMapMessage(Principal principal)
    {
        _principal = principal;
    }

    public Principal getPrincipal()
    {
        return _principal;
    }

    public void setMappedPrincipals(Set<Principal> principals)
    {
        _mappedPrincipals = principals;
    }

    public Set<Principal> getMappedPrincipals()
    {
        return _mappedPrincipals;
    }
}