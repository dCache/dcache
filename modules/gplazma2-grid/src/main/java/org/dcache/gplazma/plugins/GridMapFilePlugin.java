package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.any;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Iterables.get;
import org.dcache.gplazma.util.GridMapFile;

import java.security.Principal;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.security.auth.kerberos.KerberosPrincipal;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;

/**
 * Maps GlobusPrincipal and KerberosPrincipal to UserNamePrincipal
 * using a classic grid-mapfile.
 *
 * The plugin is silently skipped if principals already contains a
 * user name principal.
 */
public class GridMapFilePlugin
    implements GPlazmaMappingPlugin
{
    private final GridMapFile _gridMapFile;

    private static final String GRIDMAP = "gplazma.gridmap.file";

    public GridMapFilePlugin(Properties properties)
    {
        String path = properties.getProperty(GRIDMAP);
        checkArgument(path != null, "Undefined property: " + GRIDMAP);
        _gridMapFile = new GridMapFile(path);
    }

    private Map.Entry<Principal,String> getMappingFor(Set<Principal> principals)
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
                    return new SimpleImmutableEntry<Principal,String>(principal, name);
                }
            }
        }
        return null;
    }

    public void map(Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
        throws AuthenticationException
    {
        if (any(principals, instanceOf(UserNamePrincipal.class))) {
            return;
        }

        _gridMapFile.refresh();

        Map.Entry<Principal,String> entry = getMappingFor(principals);
        if (entry == null) {
            throw new AuthenticationException("No mapping for " + principals);
        }

        Principal principal = new UserNamePrincipal(entry.getValue());
        principals.add(principal);
        authorizedPrincipals.add(entry.getKey());
    }
}