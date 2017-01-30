package org.dcache.srm.util;

import eu.emi.security.authn.x509.X509Credential;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Date;

/**
 * Utility class with static methods to handle X.509 Credentials.
 */
public class Credentials
{
    private Credentials()
    {
        // prevent instantiation.
    }

    public static X509Credential checkValid(X509Credential credential) throws IOException
    {
        Date now = new Date();
        X509Certificate certificate = credential.getCertificate();

        if (certificate.getNotAfter().before(now)) {
            throw new IOException("X.509 credentials have expired");
        }

        if (certificate.getNotBefore().after(now)) {
            throw new IOException("X.509 credential not yet valid");
        }

        return credential;
    }
}
