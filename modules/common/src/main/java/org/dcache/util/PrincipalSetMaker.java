package org.dcache.util;

import com.google.common.collect.Sets;
import java.net.URI;
import java.security.Principal;
import java.util.Collections;
import java.util.Set;
import javax.security.auth.kerberos.KerberosPrincipal;
import org.dcache.auth.DesiredRole;
import org.dcache.auth.EmailAddressPrincipal;
import org.dcache.auth.ExemptFromNamespaceChecks;
import org.dcache.auth.FQANPrincipal;
import org.dcache.auth.FullNamePrincipal;
import org.dcache.auth.GidPrincipal;
import org.dcache.auth.GroupNamePrincipal;
import org.dcache.auth.OAuthProviderPrincipal;
import org.dcache.auth.OidcSubjectPrincipal;
import org.dcache.auth.UidPrincipal;
import org.dcache.auth.UserNamePrincipal;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

/**
 * The PrincipalSetMaker is a class that allows code to easily build a Set of principals using the
 * fluent interface.  The final build method provides the set of principals.
 * <p>
 * An example of the intended use of this class is: {@code import static
 * org.dcache.gplazma.plugins.PrincipalSetMaker.aSetOfPrincipals;
 * <p>
 * // ...
 * <p>
 * Set<Principal> principals = aSetOfPrincipals(). withUid(200). withDn("/O=ACME/CN=Wile E Coyote").
 * build(); }
 */
public class PrincipalSetMaker {

    private final Set<Principal> _principals = Sets.newHashSet();
    private final Set<Principal> _unmodifiableView =
          Collections.unmodifiableSet(_principals);

    public static PrincipalSetMaker aSetOfPrincipals() {
        return new PrincipalSetMaker();
    }

    /**
     * Add a UID Principal to the set.
     *
     * @param uid the id to add
     */
    public PrincipalSetMaker withUid(int uid) {
        _principals.add(new UidPrincipal(uid));
        return this;
    }

    /**
     * Add a username Principal to the set.
     *
     * @param name the username to add
     *
     */
    public PrincipalSetMaker withUsername(String username) {
        _principals.add(new UserNamePrincipal(username));
        return this;
    }

    /**
     * Add a Full Name Principal to the set.
     *
     * @param name the full name of the user.
     *
     */
    public PrincipalSetMaker withFullname(String name) {
        _principals.add(new FullNamePrincipal(name));
        return this;
    }


    /**
     * Add a primary groupname Principal to the set.
     *
     * @param name the name of the group
     */
    public PrincipalSetMaker withPrimaryGroupname(String name) {
        _principals.add(new GroupNamePrincipal(name, true));
        return this;
    }

    /**
     * Add a non-primary groupname Principal to the set.
     *
     * @param name the name of the group
     */
    public PrincipalSetMaker withGroupname(String name) {
        _principals.add(new GroupNamePrincipal(name));
        return this;
    }

    /**
     * Add a primary gid Principal to the set.
     *
     * @param gid the id to add
     */
    public PrincipalSetMaker withPrimaryGid(int gid) {
        _principals.add(new GidPrincipal(gid, true));
        return this;
    }

    /**
     * Add a non-primary gid Principal to the set.
     *
     * @param gid the id to add
     */
    public PrincipalSetMaker withGid(int gid) {
        _principals.add(new GidPrincipal(gid, false));
        return this;
    }

    /**
     * Add a DN to the set of principals.  The DN should use the OpenSSL format; for example
     * "/O=ACME/CN=Wile E Coyote".
     *
     * @param dn the DN in OpenSSL format.
     */
    public PrincipalSetMaker withDn(String dn) {
        _principals.add(new GlobusPrincipal(dn));
        return this;
    }

    /**
     * Add a primary FQAN to the set.  The primary FQAN is the first FQAN in the attribute
     * certificate from a VOMS server.
     *
     * @param fqan the FQAN to add
     */
    public PrincipalSetMaker withPrimaryFqan(String fqan) {
        _principals.add(new FQANPrincipal(fqan, true));
        return this;
    }

    /**
     * Add a non-primary FQAN to the set.  The primary FQAN is the first FQAN in the attribute
     * certificate from a VOMS server.
     *
     * @param fqan the FQAN to add
     */
    public PrincipalSetMaker withFqan(String name) {
        _principals.add(new FQANPrincipal(name));
        return this;
    }

    /**
     * Add a DesiredRole to the set.
     *
     * @param name the role the user is requesting.
     */
    public PrincipalSetMaker withDesiredRole(String name) {
        _principals.add(new DesiredRole(name));
        return this;
    }

    /**
     * Add an OIDC principal to the set.
     *
     * @param sub the OIDC 'sub' of this user.
     * @param op  the name/alias of the OAuth2 Provider that asserted this identity.
     */
    public PrincipalSetMaker withOidc(String sub, String op) {
        _principals.add(new OidcSubjectPrincipal(sub, op));
        return this;
    }

    /**
     * Add an OAuth2 Provider (OP) to the set.
     *
     * @param alias the name/alias of this OAuth2 Provider.
     * @param uri the URI identity of the OAuth2 Provider.
     */
    public PrincipalSetMaker withOauth2Provider(String alias, URI uri) {
        _principals.add(new OAuthProviderPrincipal(alias, uri));
        return this;
    }

    /**
     * Add an Email principal to the set.
     *
     * @param address The email address to be added.
     */
    public PrincipalSetMaker withEmail(String address) {
        _principals.add(new EmailAddressPrincipal(address));
        return this;
    }

    /**
     * Add a Kerberos principal to the set.
     *
     * @param kerberos the Principal to be added.
     */
    public PrincipalSetMaker withKerberos(String kerberos) {
        _principals.add(new KerberosPrincipal(kerberos));
        return this;
    }

    public PrincipalSetMaker withExemptFromNamespaceChecks() {
        _principals.add(new ExemptFromNamespaceChecks());
        return this;
    }

    /**
     * Provide a unmodifiable view of the set of principals.
     */
    public Set<Principal> build() {
        return _unmodifiableView;
    }
}
