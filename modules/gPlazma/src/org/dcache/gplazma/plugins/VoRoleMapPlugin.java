package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import java.io.IOException;
import java.security.Principal;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.dcache.auth.FQAN;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.globus.gsi.jaas.GlobusPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.google.common.collect.Lists;
import static com.google.common.collect.Iterables.*;
import static com.google.common.base.Predicates.*;

/**
 * Plugin that uses a vorolemap file for mapping FQANPrincipal and
 * GlobusPrincipal to GroupNamePrincipal.
 *
 * The plugin is silently skipped if principals already contains a
 * primary group name principal. This allows the plugin to be chained
 * with GridMapFilePlugin.
 */
public class VoRoleMapPlugin implements GPlazmaMappingPlugin
{
    private static final Logger _log =
        LoggerFactory.getLogger(VoRoleMapPlugin.class);

    private static final long REFRESH_PERIOD =
        TimeUnit.SECONDS.toMillis(10);

    private static final String VOROLEMAP_DEFAULT =
        "/etc/grid-security/grid-vorolemap";
    private static final String VOROLEMAP = "vorolemap";

    private final SourceBackedPredicateMap<NameRolePair,String> _map;

    public VoRoleMapPlugin(String[] args) throws IOException
    {
        Map<String,String> kvmap =
            ArgumentMapFactory.createFromKeyValuePairs(args);
        String path =
            ArgumentMapFactory.getValue(kvmap, VOROLEMAP, VOROLEMAP_DEFAULT);
        _map =
            new SourceBackedPredicateMap<NameRolePair,String>(new FileLineSource(path, REFRESH_PERIOD), new VOMapLineParser());
    }

    /**
     * package visible constructor for testing purposes
     * @param voMapCache map of dnfqans to usernames
     */
    VoRoleMapPlugin(SourceBackedPredicateMap<NameRolePair,String> map)
    {
        _map = map;
    }

    @Override
    public void map(SessionID sID,
                    Set<Principal> principals,
                    Set<Principal> authorizedPrincipals)
        throws AuthenticationException
    {
        if (any(principals, instanceOf(GroupNamePrincipal.class))) {
            return;
        }

        List<FQANPrincipal> fqanPrincipals =
            Lists.newArrayList(filter(principals, FQANPrincipal.class));
        List<GlobusPrincipal> globusPrincipals =
            Lists.newArrayList(filter(principals, GlobusPrincipal.class));

        boolean authorized = false;

        for (FQANPrincipal fqanPrincipal: fqanPrincipals) {
            boolean found = false;
            boolean primary = fqanPrincipal.isPrimaryGroup();
            FQAN fqan = fqanPrincipal.getFqan();
            do {
                for (GlobusPrincipal globusPrincipal: globusPrincipals) {
                    NameRolePair pair =
                        new NameRolePair(globusPrincipal.getName(),
                                         fqan.toString());
                    List<String> names =
                        _map.getValuesForPredicatesMatching(pair);
                    if (!names.isEmpty()) {
                        String name = names.get(0);
                        Principal groupPrincipal =
                            new GroupNamePrincipal(name, primary);
                        principals.add(groupPrincipal);
                        authorizedPrincipals.add(groupPrincipal);
                        authorizedPrincipals.add(fqanPrincipal);
                        authorizedPrincipals.add(globusPrincipal);
                        authorized = true;
                        found = true;
                        _log.info("VOMS authorzation successful for user with DN: {} and FQAN: {} for user name: {}.",
                                  new Object[] { globusPrincipal.getName(), fqanPrincipal.getName(), name });
                    }
                }
                fqan = fqan.getParent();
            } while (primary && !found && fqan != null);
        }

        if (!authorized) {
            String logmessage =
                String.format("VOMS Authorization failed. No authorization record for user with DNs: '%s' and FQANs: '%s' found.", globusPrincipals, fqanPrincipals);
            _log.info(logmessage);
            throw new AuthenticationException(logmessage);
        }
    }

    /**
     * Reverse mapping is not possible for VOMS, because mapping is
     * surjective and therefore the inverse is not uniquely defined.
     */
    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal,
            Set<Principal> principals) throws AuthenticationException
    {
        // TODO
    }
}
