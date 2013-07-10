package org.dcache.gplazma.plugins;

import org.globus.gsi.jaas.GlobusPrincipal;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.LoginNamePrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.GridMapFile;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.*;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;


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
                    return new SimpleImmutableEntry<>(principal, name);
                }
            }
        }
        return null;
    }

    @Override
    public void map(Set<Principal> principals)
        throws AuthenticationException
    {
        if (any(principals, instanceOf(UserNamePrincipal.class))) {
            return;
        }

        _gridMapFile.refresh();

        Map.Entry<Principal,String> entry = getMappingFor(principals);
        checkAuthentication(entry != null, "no mapping");

        principals.add(new UserNamePrincipal(entry.getValue()));
    }
}
