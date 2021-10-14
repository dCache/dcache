package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Optional;
import org.dcache.auth.OpenIdCredential;
import org.dcache.util.ChecksumType;

/**
 * @author Patrick F.
 * @author Timur Perelmutov. timur@fnal.gov
 * @version 0.0, 28 Jun 2002
 */
public class RemoteHttpDataTransferProtocolInfo implements IpProtocolInfo {

    private final String name;
    private final int minor;
    private final int major;
    private final InetSocketAddress addr;
    private final String sourceHttpUrl;
    private final boolean isVerificationRequired;
    private final ImmutableMap<String, String> headers;
    private final OpenIdCredential openIdCredential;
    private final ChecksumType desiredChecksum;

    private static final long serialVersionUID = 4482469147378465931L;

    public RemoteHttpDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          Optional<ChecksumType> desiredChecksum) {
        this(protocol, minor, major, addr, url, isVerificationRequired, headers, desiredChecksum,
              null);
    }

    public RemoteHttpDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          Optional<ChecksumType> desiredChecksum,
          OpenIdCredential openIdCredential) {
        this.name = protocol;
        this.minor = minor;
        this.major = major;
        this.addr = addr;
        this.sourceHttpUrl = url;
        this.isVerificationRequired = isVerificationRequired;
        this.headers = requireNonNull(headers);
        this.openIdCredential = openIdCredential;
        this.desiredChecksum = desiredChecksum.orElse(null);
    }

    public URI getUri() {
        return URI.create(sourceHttpUrl);
    }

    @Override
    public String getProtocol() {
        return name;
    }

    @Override
    public int getMinorVersion() {
        return minor;
    }

    @Override
    public int getMajorVersion() {
        return major;
    }

    @Override
    public String getVersionString() {
        return name + '-' + major + '.' + minor;
    }

    public boolean isVerificationRequired() {
        return isVerificationRequired;
    }

    public ImmutableMap<String, String> getHeaders() {
        return headers;
    }

    @Override
    public String toString() {
        return getVersionString() + ':' + sourceHttpUrl;
    }

    @Override
    public InetSocketAddress getSocketAddress() {
        return addr;
    }

    public OpenIdCredential getTokenCredential() {
        return openIdCredential;
    }

    public boolean hasTokenCredential() {
        return openIdCredential != null;
    }

    public Optional<ChecksumType> getDesiredChecksum() {
        return Optional.ofNullable(desiredChecksum);
    }
}
