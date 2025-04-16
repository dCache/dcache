package diskCacheV111.vehicles;

import com.google.common.collect.ImmutableMap;
import eu.emi.security.authn.x509.X509Credential;
import eu.emi.security.authn.x509.impl.KeyAndCertCredential;
import java.net.InetSocketAddress;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.cert.X509Certificate;
import java.util.List;
import org.dcache.auth.OpenIdCredential;
import org.dcache.util.ChecksumType;

import static java.util.Objects.requireNonNull;
import javax.annotation.Nonnull;

/**
 * Provides information for HTTP transfer of a file using SSL/TLS encryption.
 */
public class RemoteHttpsDataTransferProtocolInfo extends RemoteHttpDataTransferProtocolInfo {

    private static final long serialVersionUID = 1L;

    private final PrivateKey key;
    private final X509Certificate[] certChain;

    /**
     * @param desiredChecksums Desired checksum to calculate for this transfer.
     * The first checksum is used as a fall-back for old pools that only support
     * a single checksum.
     */
    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          X509Credential credential,
          @Nonnull
          List<ChecksumType> desiredChecksums) {
        this(protocol, major, minor, addr, url, isVerificationRequired,
              headers, credential == null ? null : credential.getKey(),
              credential == null ? null : credential.getCertificateChain(),
              desiredChecksums);
    }

    /**
     * @param desiredChecksums Desired checksum to calculate for this transfer.
     * The first checksum is used as a fall-back for old pools that only support
     * a single checksum.
     */
    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          PrivateKey privateKey, X509Certificate[] certificateChain,
          @Nonnull
          List<ChecksumType> desiredChecksums) {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers,
                desiredChecksums);
        key = privateKey;
        certChain = certificateChain;
    }

    /**
     * @param desiredChecksums Desired checksum to calculate for this transfer.
     * The first checksum is used as a fall-back for old pools that only support
     * a single checksum.
     */
    public RemoteHttpsDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          @Nonnull
          List<ChecksumType> desiredChecksums,
          OpenIdCredential token) {
        super(protocol, major, minor, addr, url, isVerificationRequired, headers,
                desiredChecksums, token);
        key = null;
        certChain = null;
    }

    public boolean hasCredential() {
        return key != null && certChain != null;
    }

    public PrivateKey getPrivateKey() {
        return key;
    }

    public X509Certificate[] getCertificateChain() {
        return certChain;
    }

    public X509Credential getCredential() throws KeyStoreException {
        return new KeyAndCertCredential(requireNonNull(key), requireNonNull(certChain));
    }
}
