package org.dcache.auth;

import java.util.Set;
import java.util.HashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.security.cert.X509Certificate;
import javax.security.auth.Subject;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.globus.gsi.jaas.GlobusPrincipal;
import gplazma.authz.AuthorizationException;
import gplazma.authz.AuthorizationController;
import gplazma.authz.util.NameRolePair;
import gplazma.authz.records.gPlazmaAuthorizationRecord;

import diskCacheV111.vehicles.AuthenticationMessage;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;

import org.dcache.auth.attributes.LoginAttribute;
import org.dcache.auth.attributes.HomeDirectory;
import org.dcache.auth.attributes.RootDirectory;

/**
 * A LoginStrategy that wraps a gPlazma AuthorizationController.
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
    private String _policyFile;

    public GplazmaLoginStrategy()
    {
    }

    public void setPolicyFile(String policyFile)
    {
        if (policyFile == null) {
            throw new NullPointerException();
        }
        _policyFile = policyFile;
    }

    public String getPolicyFile()
    {
        return _policyFile;
    }

    private LoginReply toReply(Map<NameRolePair,gPlazmaAuthorizationRecord> map)
    {
        AuthorizationRecord authrec =
            RecordConvert.gPlazmaToAuthorizationRecord(map);
        LoginReply reply =
            new LoginReply(Subjects.getSubject(authrec), new HashSet<LoginAttribute>());
        reply.getLoginAttributes().add(new HomeDirectory(authrec.getHome()));
        reply.getLoginAttributes().add(new RootDirectory(authrec.getRoot()));
        return reply;
    }

    public LoginReply login(Subject subject) throws CacheException
    {
        try {
            AuthorizationController gplazma =
                new AuthorizationController(_policyFile);

            /* Since the legacy interface to gPlazma has explicit
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
                return toReply(gplazma.authorize(chain, user, null, null));
            }

            /* Attempt to log in with DN and FQAN.
             */
            List<String> fqans = new ArrayList(Subjects.getFqans(subject));
            for (GlobusPrincipal dn: subject.getPrincipals(GlobusPrincipal.class)) {
                return toReply(gplazma.authorize(dn.getName(), fqans, null, user, null, null));
            }

            /* Attempt to log in with Kerberos and User name.
             */
            for (KerberosPrincipal principal: subject.getPrincipals(KerberosPrincipal.class)) {
                return toReply(gplazma.authorize(principal.getName(), NO_ROLES, null, user, null, null));
            }
        } catch (AuthorizationException e) {
            throw new PermissionDeniedCacheException("Login failed: " + e.getMessage());
        }

        throw new IllegalArgumentException("Subject is not supported by GplazmaLoginStrategy");
    }

    // map
    // reverse
}