package org.dcache.gplazma.plugins;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.getFirst;
import com.google.common.collect.Sets;
import gplazma.authz.AuthorizationController;
import gplazma.authz.AuthorizationException;
import java.io.Serializable;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.*;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.*;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.ReadOnly;
import org.dcache.auth.attributes.RootDirectory;
import org.dcache.gplazma.AuthenticationException;
import org.globus.gsi.jaas.GlobusPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * gplazma 2 plugin which uses gplazma 1 to perform login.
 *
 * @since 2.2
 */
public class GplazmaLegacy implements GPlazmaAuthenticationPlugin, GPlazmaMappingPlugin, GPlazmaSessionPlugin {

    private final static Logger _log = LoggerFactory.getLogger(GplazmaLegacy.class);

    private final static String GPLAZMA1_POLICY_OPTION = "gplazma.legacy.config";
    private final static List<String> NO_ROLES = Collections.emptyList();
    private final String _policyFile;

    public GplazmaLegacy(Properties properties) {
        _policyFile = properties.getProperty(GPLAZMA1_POLICY_OPTION);
        checkArgument(_policyFile != null, "Undefined property: " + GPLAZMA1_POLICY_OPTION);
    }

    @Override
    public void authenticate(Set<Object> publicCredentials, Set<Object> privateCredentials, Set<Principal> identifiedPrincipals) throws AuthenticationException {
        try {
            AuthorizationController gplazma =
                    new AuthorizationController(_policyFile);

            /*
             * Since the legacy interface to gPlazma has explicit knowledge
             * about various types of Principals, we are forced to iterate over
             * the Principals and feed them into the legacy interface.
             *
             * The old and the new interface do not fit very well together, so
             * this is essentially a big hack.
             */
            Principal userPrincipal = getFirst(filter(identifiedPrincipals, LoginNamePrincipal.class), null);
            String user = userPrincipal == null ? null : userPrincipal.getName();

            /*
             * Attempt to login using the X509Certificate chain.
             */
            X509Certificate[] chain = getFirst(filter(publicCredentials, X509Certificate[].class), null);
            if (chain != null) {
                AuthorizationRecord authrec = RecordConvert.gPlazmaToAuthorizationRecord(
                        gplazma.authorize(chain, user, null, null));
                _log.debug("Authenticating by chain: {}", authrec);
                identifiedPrincipals.add(new AuthorizationRecordPrincipal(authrec));
                return;
            }

            /*
             * Attempt to log in with DN and FQAN.
             */
            GlobusPrincipal dn = getFirst(filter(identifiedPrincipals, GlobusPrincipal.class), null);
            if (dn != null) {
                Set<FQANPrincipal> fqanPrincipals = Sets.newHashSet(filter(publicCredentials, FQANPrincipal.class));
                List<String> fqans = new ArrayList();
                for (Principal principal : fqanPrincipals) {
                    fqans.add(principal.getName());
                }

                AuthorizationRecord authrec = RecordConvert.gPlazmaToAuthorizationRecord(
                        gplazma.authorize(dn.getName(), fqans, null, user, null, null));
                _log.debug("Authenticating by DN+FQAN: {}", authrec);
                identifiedPrincipals.add(new AuthorizationRecordPrincipal(authrec));
                return;
            }

            /*
             * Attempt to log in with Kerberos and User name.
             */
            KerberosPrincipal principal = getFirst(filter(privateCredentials, KerberosPrincipal.class), null);
            if (principal != null) {
                AuthorizationRecord authrec = RecordConvert.gPlazmaToAuthorizationRecord(
                        gplazma.authorize(principal.getName(), NO_ROLES, null, user, null, null));
                _log.debug("Authenticating by Kerberos principal: {}", authrec);
                identifiedPrincipals.add(new AuthorizationRecordPrincipal(authrec));
                return;
            }
        } catch (AuthorizationException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
        throw new AuthenticationException("no credentials or principals that gPlazma 1 understands");
    }

    @Override
    public void map(Set<Principal> principals, Set<Principal> authorizedPrincipals) throws AuthenticationException {
        AuthorizationRecordPrincipal authrecPrincipal =
                getFirst(filter(principals, AuthorizationRecordPrincipal.class), null);

        if (authrecPrincipal == null) {
            throw new AuthenticationException("not logged in via gPlazma 1");
        }

        authorizedPrincipals.addAll(authrecPrincipal.getAuthrec().toSubject().getPrincipals());
        authorizedPrincipals.add(authrecPrincipal);
    }

    @Override
    public void session(Set<Principal> authorizedPrincipals, Set<Object> attrib) throws AuthenticationException {
        AuthorizationRecordPrincipal authrecPrincipal =
                getFirst(filter(authorizedPrincipals, AuthorizationRecordPrincipal.class), null);

        if (authrecPrincipal == null) {
            throw new AuthenticationException("not logged in via gPlazma 1");
        }

        AuthorizationRecord authrec = authrecPrincipal.getAuthrec();
        attrib.add(new HomeDirectory(authrec.getHome()));
        attrib.add(new RootDirectory(authrec.getRoot()));
        attrib.add(new ReadOnly(authrec.isReadOnly()));
    }

    /**
     * special {@link Principal} to incapsulate {@link AuthorizationRecord}
     */
    private static class AuthorizationRecordPrincipal implements Principal, Serializable {

        private static final long serialVersionUID = -4242349585261079837L;
        private final AuthorizationRecord _authrec;

        public AuthorizationRecordPrincipal(AuthorizationRecord authrec) {
            _authrec = authrec;
        }

        public String getName() {
            return _authrec.getName();
        }

        public AuthorizationRecord getAuthrec() {
            return _authrec;
        }
    }
}
