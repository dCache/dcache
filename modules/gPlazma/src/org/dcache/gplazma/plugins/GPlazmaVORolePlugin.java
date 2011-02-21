/**
 * Project: dCache-hg
 * Package: org.dcache.gplazma.plugins
 *
 * created on Dec 2, 2010 by karsten
 */
package org.dcache.gplazma.plugins;

import gplazma.authz.util.NameRolePair;

import java.io.IOException;
import java.security.Principal;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Collections2;

/**
 * Plugin using VORoleMap and AuthzDB for authentication and mapping.
 * @author karsten
 *
 */
public class GPlazmaVORolePlugin implements GPlazmaAuthenticationPlugin, GPlazmaMappingPlugin {

    private static final Logger _log = LoggerFactory.getLogger(GPlazmaVORolePlugin.class);

    private static final String AUTHZDB_KEYNAME = "authzdb";
    private static final String VOROLEMAP_KEYNAME = "vorolemap";
    private static final Set<String> KEYNAMES = new HashSet<String>(Arrays.asList( new String[] { VOROLEMAP_KEYNAME, AUTHZDB_KEYNAME } ));

    private static final ClassTypePredicate<Principal> GLOBUSPRINCIPAL_CLASS_TYPE_PREDICATE = new ClassTypePredicate<Principal>(GlobusPrincipal.class);
    private static final ClassTypePredicate<Principal> FQANPRINCIPAL_CLASS_TYPE_PREDICATE = new ClassTypePredicate<Principal>(FQANPrincipal.class);
    private static final ClassTypePredicate<Principal> USERNAMEPRINCIPAL_CLASS_TYPE_PREDICATE = new ClassTypePredicate<Principal>(UserNamePrincipal.class);

    private final SourceBackedPredicateMap<NameRolePair, String> _cachedVOMap;
    private final SourceBackedPredicateMap<String, AuthzMapLineParser.UserAuthzInformation> _cachedAuthzMap;


    public GPlazmaVORolePlugin(String[] args) throws IOException {

        Map<String, String> kvmap = ArgumentMapFactory.create(KEYNAMES, args);
        _cachedVOMap = new SourceBackedPredicateMap<NameRolePair, String>(new FileLineSource(kvmap.get(VOROLEMAP_KEYNAME), 10000), new VOMapLineParser());
        _cachedAuthzMap = new SourceBackedPredicateMap<String, AuthzMapLineParser.UserAuthzInformation>(new FileLineSource(kvmap.get(AUTHZDB_KEYNAME), 10000), new AuthzMapLineParser());
    }

    /**
     * package visible constructor for testing purposes
     * @param voMapCache map of dnfqans to usernames
     * @param authzMapCache map of usernames to user information (e.q. uid/gid)
     */
    GPlazmaVORolePlugin(SourceBackedPredicateMap<NameRolePair, String> voMapCache, SourceBackedPredicateMap<String, AuthzMapLineParser.UserAuthzInformation> authzMapCache) {
        _cachedVOMap = voMapCache;
        _cachedAuthzMap = authzMapCache;
    }

    /**
     * @param sID ignored
     * @param publicCredentials ignored
     * @param privateCredentials ignored
     * @param identifiedPrincipals from this set FqanPrincipals and GlobusPrincipals are extracted and mapped to UserNamePrincipals
     * @throws AuthenticationException if no combination of fqan and dn could be mapped.
     */
    @Override
    public void authenticate(SessionID sID, Set<Object> publicCredentials,
            Set<Object> privateCredentials, Set<Principal> identifiedPrincipals)
            throws AuthenticationException {

        if (identifiedPrincipals == null) {
            _log.info("VOMS Authorization failed. No usable principal found.");
            throw new AuthenticationException("VOMS Authorization failed. No usable principal found.");
        }

        Collection<Principal> fqanPrincipals = new HashSet<Principal>(Collections2.filter(identifiedPrincipals, FQANPRINCIPAL_CLASS_TYPE_PREDICATE));
        Collection<Principal> globusPrincipals = new HashSet<Principal>(Collections2.filter(identifiedPrincipals, GLOBUSPRINCIPAL_CLASS_TYPE_PREDICATE));

        _log.debug("Login attempt by user '%s'", fqanPrincipals);
        boolean authorized = false;

        for (Principal fqanPrincipal : fqanPrincipals) {
            for (Principal globusPrincipal : globusPrincipals) {
                List<String> usernames = _cachedVOMap.getValuesForPredicatesMatching(new NameRolePair(globusPrincipal.getName(), fqanPrincipal.getName()));
                if (!usernames.isEmpty()) {
                    String username = usernames.get(0);
                    identifiedPrincipals.add(new UserNamePrincipal(username));
                    authorized  = true;
                    _log.info(String.format("VOMS authorzation successful for user with DN:'%s' and FQAN:'%s' for username '%s'.", globusPrincipal.getName(), fqanPrincipal.getName(), username));
                }
            }
        }

        if (!authorized) {
            String logmessage = String.format("VOMS Authorization failed. No authorization record for user with DNs: '%s' and FQANs: '%s' found.", globusPrincipals, fqanPrincipals);
            _log.info(logmessage);
            throw new AuthenticationException(logmessage);
        }
    }

    @Override
    public void map(SessionID sID, Set<Principal> principals,
            Set<Principal> authorizedPrincipals) throws AuthenticationException {
        if (principals == null || authorizedPrincipals == null) throw new IllegalArgumentException();

        Collection<Principal> userNamePrincipals = Collections2.filter(authorizedPrincipals, USERNAMEPRINCIPAL_CLASS_TYPE_PREDICATE);

        if (userNamePrincipals.isEmpty()) return;

        _log.info("Mapping for user '{}'.", userNamePrincipals);

        for (Principal principal : userNamePrincipals) {
            Collection<AuthzMapLineParser.UserAuthzInformation> result = _cachedAuthzMap.getValuesForPredicatesMatching(principal.getName());

            for (AuthzMapLineParser.UserAuthzInformation mapping : result) {
                principals.add(new UidPrincipal(mapping.getUid()));
                principals.add(new GidPrincipal(mapping.getGid(), true));
            }
        }
    }

    /**
     * Reverse mapping is not possible for VOMS, because mapping is surjective and therefore
     * the inverse is not uniquely defined.
     */
    @Override
    public void reverseMap(SessionID sID, Principal sourcePrincipal,
            Set<Principal> principals) throws AuthenticationException {
    }

}
