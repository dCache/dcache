package org.dcache.gplazma.plugins;

import java.security.Principal;
import java.security.cert.CertPath;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;

import static org.dcache.gplazma.util.CertPaths.getOriginalUserDnAsGlobusPrincipal;
import static org.dcache.gplazma.util.CertPaths.isX509CertPath;
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
            if (isX509CertPath(credential)) {
                identifiedPrincipals.add(getOriginalUserDnAsGlobusPrincipal((CertPath) credential));
                found = true;
            }
        }
        checkAuthentication(found, "no X509 certificate chain");
    }

}
