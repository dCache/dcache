package org.dcache.gplazma;

import java.security.Principal;

/**
 *  This class represents that an attempt to do a forward identity lookup
 * (convert an arbitrary Principal to either a UID or GID principal) or the
 * reverse identity lookup (discovering the set of principals that map to the
 * supplied UID or GID) has failed.
 */
public class NoSuchPrincipalException extends Exception
{
    static final long serialVersionUID = 87356059991976395L;

    public NoSuchPrincipalException(Principal principal)
    {
        this("No such principal: " + principal.getName());
    }

    public NoSuchPrincipalException(String message)
    {
        super(message);
    }
}
