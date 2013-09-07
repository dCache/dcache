package org.dcache.util;

import com.google.common.base.Preconditions;

import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.List;

public class CertPaths
{
    private CertPaths()
    {
    }

    public static boolean isX509CertPath(Object credential)
    {
        return credential instanceof CertPath && ((CertPath) credential).getType().equals(CertificateFactories.X_509);
    }

    public static X509Certificate[] getX509Certificates(CertPath certPath)
    {
        Preconditions.checkArgument(certPath.getType().equals(CertificateFactories.X_509));
        List<X509Certificate> certificates = (List<X509Certificate>) certPath.getCertificates();
        return certificates.toArray(new X509Certificate[certificates.size()]);
    }
}
