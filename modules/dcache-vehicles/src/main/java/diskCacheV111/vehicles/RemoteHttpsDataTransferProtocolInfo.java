package diskCacheV111.vehicles;

import com.google.common.collect.ImmutableMap;
import eu.emi.security.authn.x509.X509Credential;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

import org.dcache.auth.OpenIdCredential;

/**
 * Provides information for HTTP transfer of a file using SSL/TLS encryption.
 */
public class RemoteHttpsDataTransferProtocolInfo extends RemoteHttpDataTransferProtocolInfo
{
    private static final long serialVersionUID = 1L;

    private final PrivateKey key;
    private final X509Certificate[] certChain;

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
                                               int minor, InetSocketAddress addr, String url,
                                               boolean isVerificationRequired, ImmutableMap<String,String> headers,
                                               X509Credential credential)
    {
        this(protocol, major, minor, addr, url, isVerificationRequired,
                headers, credential == null ? null : credential.getKey(),
                credential == null ? null : credential.getCertificateChain());
    }

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
            int minor, InetSocketAddress addr, String url,
            boolean isVerificationRequired, ImmutableMap<String,String> headers,
            PrivateKey privateKey, X509Certificate[] certificateChain)
    {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers);
        key = privateKey;
        certChain = certificateChain;
    }

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
                                               int minor, InetSocketAddress addr, String url,
                                               boolean isVerificationRequired, ImmutableMap<String,String> headers,
                                               OpenIdCredential token)
    {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers, token);
        key = null;
        certChain = null;
    }

    public boolean hasCredential()
    {
        return key != null && certChain != null;
    }

    public PrivateKey getPrivateKey()
    {
        return key;
    }

    public X509Certificate[] getCertificateChain()
    {
        return certChain;
    }
}
