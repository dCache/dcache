package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.auth.attributes.Restriction;
import org.dcache.gplazma.AuthenticationException;

/**
 * A plugin that can extract one or more principals from a set of given
 * credentials. A plugin should throw AuthenticationException if it is unable to
 * extract any principals.
 * <p>
 * There are two authenticate methods for backwards compatibility.  Plugins are
 * encouraged to use the newer form.
 */
public interface GPlazmaAuthenticationPlugin extends GPlazmaPlugin
{
    /**
     * Extract principals from credentials, throwing AuthenticationException
     * if that is not possible.
     * <p>
     * New plugins should not implement this method.
     * @param publicCredentials credentials that do not require secrecy
     * @param privateCredentials credentials that must remain secret
     * @param principals set of principals discovered so far
     * @throws AuthenticationException
     */
    default void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals) throws AuthenticationException
    {
        throw new UnsupportedOperationException("A plugin must override one of the 'authenticate' methods");
    }


    /**
     * Extract principals from credentials, throwing AuthenticationException
     * if that is not possible.
     * @param publicCredentials credentials that do not require secrecy
     * @param privateCredentials credentials that must remain secret
     * @param principals set of principals discovered so far
     * @param restrictionStore the recipient of any Restriction from processing a credential
     * @throws AuthenticationException
     */
    default void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials,
            Set<Principal> principals, Set<Restriction> restrictions)
            throws AuthenticationException
    {
        authenticate(publicCredentials, privateCredentials, principals);
    }
}
