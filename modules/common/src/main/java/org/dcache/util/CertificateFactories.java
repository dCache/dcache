package org.dcache.util;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;

public class CertificateFactories
{
    public static final String X_509 = "X.509";

    static
    {
        Security.addProvider(new BouncyCastleProvider());
    }

    private CertificateFactories()
    {
    }

    /**
     * Returns an X.509 CertificateFactory.
     *
     * @throws RuntimeException if the factory could not be instantiated
     */
    public static CertificateFactory newX509CertificateFactory()
    {
        try {
            return CertificateFactory.getInstance(X_509, BouncyCastleProvider.PROVIDER_NAME);
        } catch (CertificateException e) {
            throw new RuntimeException("Failed to create X.509 certificate factory: " + e.getMessage(), e);
        } catch (NoSuchProviderException e) {
            throw new RuntimeException("Failed to load bouncy castle provider: " + e.getMessage(), e);
        }
    }
}
