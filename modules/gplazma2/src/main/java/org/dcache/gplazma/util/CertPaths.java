package org.dcache.gplazma.util;

import com.google.common.base.Preconditions;
import eu.emi.security.authn.x509.proxy.ProxyUtils;
import org.globus.gsi.gssapi.jaas.GlobusPrincipal;

import java.security.cert.CertPath;
import java.security.cert.X509Certificate;
import java.util.List;

import org.dcache.util.CertificateFactories;

import static eu.emi.security.authn.x509.impl.OpensslNameUtils.convertFromRfc2253;
import static eu.emi.security.authn.x509.proxy.ProxyUtils.getOriginalUserDN;

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

    public static GlobusPrincipal getOriginalUserDnAsGlobusPrincipal(CertPath credential)
    {
        X509Certificate[] chain = getX509Certificates(credential);
        String globusDn = convertFromRfc2253(getOriginalUserDN(chain).getName(), true);
        return new GlobusPrincipal(globusDn);
    }

    public static X509Certificate getEndEntityCertificate(CertPath credential)
    {
        X509Certificate[] chain = getX509Certificates(credential);
        return ProxyUtils.getEndUserCertificate(chain);
    }
}
