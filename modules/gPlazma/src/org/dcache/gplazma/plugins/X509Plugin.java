package org.dcache.gplazma.plugins;

import gplazma.authz.AuthorizationException;
import gplazma.authz.util.X509CertUtil;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Properties;
import java.util.Set;

import org.dcache.gplazma.AuthenticationException;
import org.dcache.gplazma.SessionID;
import org.globus.gsi.jaas.GlobusPrincipal;

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
    public void authenticate(SessionID sID,
                             Set<Object> publicCredentials,
                             Set<Object> privateCredentials,
                             Set<Principal> identifiedPrincipals)
        throws AuthenticationException
    {
        try {
            boolean found = false;
            for (Object credential: publicCredentials) {
                if (credential instanceof X509Certificate[]) {
                    X509Certificate[] chain = (X509Certificate[]) credential;
                    String dn =
                        X509CertUtil.getSubjectFromX509Chain(chain, false);
                    identifiedPrincipals.add(new GlobusPrincipal(dn));
                    found = true;
                }
            }
            if (!found) {
                throw new AuthenticationException("X509 certificate chain missing");
            }
        } catch (AuthorizationException e) {
            throw new AuthenticationException(e.getMessage(), e);
        }
    }
}
