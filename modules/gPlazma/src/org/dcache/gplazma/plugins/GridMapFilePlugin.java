package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.util.Set;
import java.util.Map;
import java.util.Collection;
import java.util.AbstractMap.SimpleImmutableEntry;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;

import gplazma.authz.plugins.gridmapfile.GridMapFile;

import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.LoginNamePrincipal;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.globus.gsi.jaas.GlobusPrincipal;

import static com.google.common.collect.Iterables.*;
import static com.google.common.base.Predicates.*;

/**
 * Maps GlobusPrincipal and KerberosPrincipal to GroupNamePrincipal
 * using a classic grid-mapfile.
 *
 * The plugin is silently skipped if principals already contains a
 * primary group name principal. This allows the plugin to be chained
 * with VORoleMapPlugin.
 */
public class GridMapFilePlugin
    implements GPlazmaMappingPlugin
{
    private final GridMapFile _gridMapFile;

    private static final String DEFAULT_GRIDMAP =
        "/etc/grid-security/grid-mapfile";
    private static final String GRIDMAP = "gridmap";

    public GridMapFilePlugin(String[] args)
    {
        Map<String,String> kvmap =
            ArgumentMapFactory.createFromKeyValuePairs(args);
        String path =
            ArgumentMapFactory.getValue(kvmap, GRIDMAP, DEFAULT_GRIDMAP);
        _gridMapFile = new GridMapFile(path);
    }

    private Map.Entry<Principal,String>
        getMappingFor(Set<Principal> principals)
    {
        Principal loginName =
            find(principals, instanceOf(LoginNamePrincipal.class), null);
        for (Principal principal: principals) {
            if (principal instanceof GlobusPrincipal ||
                principal instanceof KerberosPrincipal) {
                Collection<String> names =
                    _gridMapFile.getMappedUsernames(principal.getName());
                if (!names.isEmpty()) {
                    String name;
                    if (loginName == null) {
                        name = get(names, 0);
                    } else if (names.contains(loginName.getName())) {
                        name = loginName.getName();
                    } else {
                        continue;
                    }
                    return new SimpleImmutableEntry(principal, name);
                }
            }
        }
        return null;
    }

    public void map(SessionID sID,
                    Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
        throws AuthenticationException
    {
        if (any(principals, instanceOf(GroupNamePrincipal.class))) {
            return;
        }

        _gridMapFile.refresh();

        Map.Entry<Principal,String> entry = getMappingFor(principals);
        if (entry == null) {
            throw new AuthenticationException("No mapping for " + principals);
        }

        Principal principal = new GroupNamePrincipal(entry.getValue(), true);
        principals.add(principal);
        authorizedPrincipals.add(principal);
        authorizedPrincipals.add(entry.getKey());
    }

    public void reverseMap(SessionID sID,
                           Principal sourcePrincipal,
                           Set<Principal> principals)
        throws AuthenticationException
    {
        _gridMapFile.refresh();
        if (sourcePrincipal instanceof GroupNamePrincipal) {
            // TODO: need to extend GridMapFile to allow lookup for
            // reverse map. Also need to add a configuration option
            // that tells us whether the gridmap file maps to personal
            // user names or group names.
        }
    }
}