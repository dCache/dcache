package org.dcache.gplazma.plugins;

import org.globus.gsi.jaas.GlobusPrincipal;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.Set;

import org.dcache.auth.util.X509Utils;
import org.dcache.gplazma.AuthenticationException;

import static org.dcache.gplazma.util.Preconditions.checkAuthentication;

/**
 * Extracts GlobusPrincipals from any X509Certificate certificate
 * chain in the public credentials.
 *
 * The certificate is not validated (ie no CRL checks and no CA
 * signature check). It is assumed that the door did this check
 * already.
 */
public class X509Plugin implements GPlazmaAuthenticationPlugin
{
    public X509Plugin(Properties properties) {}

    @Override
    public void authenticate(Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        boolean found = false;
        for (Object credential : publicCredentials) {
            if (credential instanceof X509Certificate[]) {
                X509Certificate[] chain = (X509Certificate[]) credential;
                String dn
                    = X509Utils.getSubjectFromX509Chain(chain, false);
                identifiedPrincipals.add(new GlobusPrincipal(dn));
                found = true;
            }
        }
        checkAuthentication(found, "no X509 certificate chain");
    }
}
