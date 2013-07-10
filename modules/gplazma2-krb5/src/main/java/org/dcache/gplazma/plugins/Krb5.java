package org.dcache.gplazma.plugins;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.Principal;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.UserNamePrincipal;
import org.dcache.gplazma.AuthenticationException;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * A {@link GPlazmaMappingPlugin} converts {@code user@DOMAIN.COM} to
 * {@link UserNamePrincipal} corresponding to {@code user} ( without
 * domain ).  For more complex mappings, like {@code
 * remte-user@DOMAIN.COM} to {@code local-user}, {@link
 * GridMapFilePlugin} can be used.
 *
 * To enable, add following likes into gplazma.conf:
 * <pre>
 *     <b>map requisite krb5</b>
 * </pre>
 */
public class Krb5 implements GPlazmaMappingPlugin
{
    public Krb5(Properties properties) {
        /*
         * enforced by pluggin interface
         */
    }

    @Override
    public void map(Set<Principal> principals) throws AuthenticationException {
        Set<Principal> kerberosPrincipals = new HashSet<>();
        for (Principal principal : principals) {
            if (principal instanceof KerberosPrincipal) {
                kerberosPrincipals.add(new UserNamePrincipal(stripDomain(principal.getName())));
            }
        }
        checkAuthentication(!kerberosPrincipals.isEmpty(),
                "no Kerberos principals");

        principals.addAll(kerberosPrincipals);
    }

    private String stripDomain(String s) {
        int n = s.indexOf('@');
        if (n != -1) {
            return s.substring(0, n);
        }
        return s;
    }
}
