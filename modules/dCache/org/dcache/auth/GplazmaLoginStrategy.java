package org.dcache.auth;

import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.security.cert.X509Certificate;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.globus.gsi.jaas.GlobusPrincipal;
import gplazma.authz.AuthorizationException;

import diskCacheV111.vehicles.AuthenticationMessage;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;

/**
 * A LoginStrategy that wraps the AuthzQueryHelper gPlazma client
 * stub.
 *
 * Supports login for Subjects with the following principals:
 *
 *    - A GlobusPrincipal, one or more FQANPrincipals and an
 *      optional UserNamePrincipal
 *    - A KerberosPrincipal and a UserNamePrincipal
 *
 * Alternatively login can be made with a X509Certificate[]
 * certificate chain as a public credential and an optional
 * UserNamePrincipal.
 */
public class GplazmaLoginStrategy implements LoginStrategy
{
    private final static List<String> NO_ROLES = Collections.emptyList();
    private final AuthzQueryHelper _authzQueryHelper;

    public GplazmaLoginStrategy(AuthzQueryHelper authzQueryHelper)
    {
        _authzQueryHelper = authzQueryHelper;
    }

    private LoginReply toReply(AuthenticationMessage message)
    {
        AuthorizationRecord authrec =
            RecordConvert.gPlazmaToAuthorizationRecord(message.getgPlazmaAuthzMap());
        LoginReply reply =
            new LoginReply(Subjects.getSubject(authrec), new HashSet<LoginAttribute>());
        reply.getLoginAttributes().add(new HomeDirectory(authrec.getHome()));
        reply.getLoginAttributes().add(new RootDirectory(authrec.getRoot()));
        return reply;
    }

    public LoginReply login(Subject subject) throws CacheException
    {
        try {
            /* Since the legacy interface to gPlazma had explicit
             * knowledge about various types of Principals, we are forced
             * to iterate over the Principals and feed them into the
             * legacy interface.
             *
             * The old and the new interface do not fit very well
             * together, so this is essentially a big hack.
             */

            String user = Subjects.getUserName(subject);

            /* Attempt to login using the X509Certificate chain.
             */
            Set<X509Certificate[]> chains =
                subject.getPublicCredentials(X509Certificate[].class);
            for (X509Certificate[] chain: chains) {
                return toReply(_authzQueryHelper.authorize(chain, user));
            }

            /* Attempt to log in with DN and FQAN.
             */
            Collection<String> fqanCollection = Subjects.getFqans(subject);
            List<String> fqans = null;
            if (!fqanCollection.isEmpty()) {
                fqans = new ArrayList(fqanCollection);
            }

            for (GlobusPrincipal dn: subject.getPrincipals(GlobusPrincipal.class)) {
                return toReply(_authzQueryHelper.authorize(dn.getName(), fqans, user));
            }

            /* Attempt to log in with Kerberos and User name.
             */
            for (KerberosPrincipal principal: subject.getPrincipals(KerberosPrincipal.class)) {
                return toReply(_authzQueryHelper.authorize(principal.getName(),
                                                           NO_ROLES,
                                                           user));
            }
        } catch (AuthorizationException e) {
            throw new PermissionDeniedCacheException("Login failed: " + e.getMessage());
        }

        throw new IllegalArgumentException("Subject is not supported by GplazmaLoginStrategy");
    }

    // map
    // reverse
}