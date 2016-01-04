package org.dcache.gplazma.plugins;

import com.google.common.collect.Lists;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.FQAN;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.util.NameRolePair;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;
import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Plugin that uses a vorolemap file for mapping FQANPrincipal and
 * GlobusPrincipal to GroupNamePrincipal.
 */
public class VoRoleMapPlugin implements GPlazmaMappingPlugin
{
    private static final Logger _log =
        LoggerFactory.getLogger(VoRoleMapPlugin.class);

    private static final long REFRESH_PERIOD =
        TimeUnit.SECONDS.toMillis(10);

    private static final String VOROLEMAP =
        "gplazma.vorolemap.file";

    private final SourceBackedPredicateMap<NameRolePair,String> _map;

    public VoRoleMapPlugin(Properties properties) throws IOException
    {
        String path = properties.getProperty(VOROLEMAP);

        checkArgument(path != null, "Undefined property: " + VOROLEMAP);

        _map = new SourceBackedPredicateMap<>(new FileLineSource(path, REFRESH_PERIOD), new VOMapLineParser());
    }

    /**
     * package visible constructor for testing purposes
     * @param map map of dnfqans to usernames
     */
    VoRoleMapPlugin(SourceBackedPredicateMap<NameRolePair,String> map)
    {
        _map = map;
    }

    private boolean containsPrimaryGroupName(Set<Principal> principals)
    {
        for (GroupNamePrincipal p: filter(principals, GroupNamePrincipal.class)) {
            if (p.isPrimaryGroup()) {
                return true;
            }
        }
        return false;
    }

    private boolean addMappingFor(GlobusPrincipal globusPrincipal, FQANPrincipal fqanPrincipal,
                                  FQAN fqan, boolean isPrimary, Set<Principal> principals)
    {
        String dn = globusPrincipal.getName();
        List<String> names =
            _map.getValuesForPredicatesMatching(new NameRolePair(dn, fqan.toString()));
        if (names.isEmpty()) {
            return false;
        }

        String name = names.get(0);
        principals.add(new GroupNamePrincipal(name, isPrimary));
        _log.info("VOMS authorization successful for user with DN: {} and FQAN: {} for user name: {}.", dn, fqanPrincipal, name);
        return true;
    }

    @Override
    public void map(Set<Principal> principals)
        throws AuthenticationException
    {
        List<FQANPrincipal> fqanPrincipals =
            Lists.newArrayList(filter(principals, FQANPrincipal.class));
        List<GlobusPrincipal> globusPrincipals =
            Lists.newArrayList(filter(principals, GlobusPrincipal.class));

        boolean hasPrimary = containsPrimaryGroupName(principals);
        boolean authorized = false;

        for (FQANPrincipal fqanPrincipal: fqanPrincipals) {
            boolean found = false;
            boolean isPrimary = fqanPrincipal.isPrimaryGroup() && !hasPrimary;
            FQAN fqan = fqanPrincipal.getFqan();
            do {
                for (GlobusPrincipal globusPrincipal: globusPrincipals) {
                    if (addMappingFor(globusPrincipal, fqanPrincipal, fqan, isPrimary, principals)) {
                        authorized = true;
                        found = true;
                        hasPrimary |= isPrimary;
                    }
                }
                fqan = fqan.getParent();
            } while (isPrimary && !found && fqan != null);
        }

        checkAuthentication(authorized, "no record");
    }
}
