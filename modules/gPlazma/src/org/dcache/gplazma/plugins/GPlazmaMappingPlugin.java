package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;

/**
 * Mapping plugins which subclass that interface translate between different
 * kinds of principals. Such plugins can
 *  - map from received principals (DN, Kerberos Principal, Username, ...) to
 *    UID and a set of GIDs
 *  - map from principals to other principals (DN to username,...)
 *  - offer reverse mapping, e.g. from a UID to a DN
 *
 */
public interface GPlazmaMappingPlugin extends GPlazmaPlugin {
    public void map(SessionID sID,
                    Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
                throws AuthenticationException;

    public void reverseMap(SessionID sID,
                           Principal sourcePrincipal,
                           Set<Principal> principals) throws Exception;
}
