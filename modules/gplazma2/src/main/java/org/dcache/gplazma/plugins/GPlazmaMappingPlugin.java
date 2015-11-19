package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;

/**
 * Mapping plugins which subclass that interface translate between
 * different kinds of principals. Such plugins can:
 *
 *  - map from received principals (DN, Kerberos Principal, Username,
 *    ...) to UID and a set of GIDs
 *  - map from principals to other principals (DN to username,...)
 */
public interface GPlazmaMappingPlugin extends GPlazmaPlugin
{
    void map(Set<Principal> principals)
        throws AuthenticationException;
}
