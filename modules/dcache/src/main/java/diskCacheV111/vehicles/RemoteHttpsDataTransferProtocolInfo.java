package diskCacheV111.vehicles;

import com.google.common.collect.ImmutableMap;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.ietf.jgss.GSSException;

import java.net.InetSocketAddress;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;

/**
 * Provides information for HTTP transfer of a file using SSL/TLS encryption.
 */
public class RemoteHttpsDataTransferProtocolInfo extends RemoteHttpDataTransferProtocolInfo
{
    private static final long serialVersionUID = 1L;

    private final PrivateKey key;
    private final X509Certificate[] certChain;

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
            int minor, InetSocketAddress addr, int buf_size, String url,
            boolean isVerificationRequired, ImmutableMap<String,String> headers,
            GlobusGSSCredentialImpl credential) throws GSSException
    {
        this(protocol, major, minor, addr, buf_size, url, isVerificationRequired,
                headers, credential == null ? null : credential.getPrivateKey(),
                credential == null ? null : credential.getCertificateChain());
    }

    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
            int minor, InetSocketAddress addr, int buf_size, String url,
            boolean isVerificationRequired, ImmutableMap<String,String> headers,
            PrivateKey privateKey, X509Certificate[] certificateChain)
    {
        super(protocol, major, minor, addr, buf_size, url, isVerificationRequired, headers);
        key = privateKey;
        certChain = certificateChain;
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
