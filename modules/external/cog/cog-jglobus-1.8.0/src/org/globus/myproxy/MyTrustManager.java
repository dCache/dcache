package org.globus.myproxy;

import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;

import javax.net.ssl.X509TrustManager;

public class MyTrustManager implements X509TrustManager
{
    private X509Certificate[] certs = null;

    public X509Certificate[] getAcceptedIssuers() {
        return this.certs;
    }

    public void checkClientTrusted(X509Certificate[] certs, String authType)
        throws CertificateException {
        throw new CertificateException(
            "checkClientTrusted not implemented by org.globus.myproxy.MyTrustManager");
    }

    public void checkServerTrusted(X509Certificate[] certs, String authType)
        throws CertificateException {
        this.certs = new X509Certificate[certs.length-1];
        System.arraycopy(certs, 1, this.certs, 0, certs.length-1);
    }
}
