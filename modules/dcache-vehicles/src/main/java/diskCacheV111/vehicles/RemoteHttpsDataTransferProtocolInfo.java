package diskCacheV111.vehicles;

import com.google.common.collect.ImmutableMap;
import eu.emi.security.authn.x509.X509Credential;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.Optional;

import org.dcache.auth.OpenIdCredential;
import org.dcache.util.ChecksumType;

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
                                               X509Credential credential,
                                               Optional<ChecksumType> desiredChecksum)
    {
        this(protocol, major, minor, addr, url, isVerificationRequired,
                headers, credential == null ? null : credential.getKey(),
                credential == null ? null : credential.getCertificateChain(),
                desiredChecksum);
    }

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
            int minor, InetSocketAddress addr, String url,
            boolean isVerificationRequired, ImmutableMap<String,String> headers,
            PrivateKey privateKey, X509Certificate[] certificateChain,
            Optional<ChecksumType> desiredChecksum)
    {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers, desiredChecksum);
        key = privateKey;
        certChain = certificateChain;
    }

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
                                               int minor, InetSocketAddress addr, String url,
                                               boolean isVerificationRequired, ImmutableMap<String,String> headers,
                                               Optional<ChecksumType> desiredChecksum,
                                               OpenIdCredential token)
    {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers, desiredChecksum, token);
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
