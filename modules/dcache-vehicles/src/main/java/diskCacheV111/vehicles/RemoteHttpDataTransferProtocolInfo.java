package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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
    @Nullable
    private final ChecksumType desiredChecksum;
    @Nonnull
    private Set<ChecksumType> desiredChecksums;

    private static final long serialVersionUID = 4482469147378465931L;

    /**
     * @param desiredChecksums Desired checksum to calculate for this transfer.
     * The first checksum is used as a fall-back for old pools that only support
     * a single checksum.
     */
    public RemoteHttpDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          List<ChecksumType> desiredChecksums) {
        this(protocol, minor, major, addr, url, isVerificationRequired, headers,
                desiredChecksums, null);
    }

    /**
     * @param desiredChecksums Desired checksum to calculate for this transfer.
     * The first checksum is used as a fall-back for old pools that only support
     * a single checksum.
     */
    public RemoteHttpDataTransferProtocolInfo(String protocol, int major,
          int minor, InetSocketAddress addr, String url,
          boolean isVerificationRequired, ImmutableMap<String, String> headers,
          @Nonnull
          List<ChecksumType> desiredChecksums,
          OpenIdCredential openIdCredential) {
        this.name = protocol;
        this.minor = minor;
        this.major = major;
        this.addr = addr;
        this.sourceHttpUrl = url;
        this.isVerificationRequired = isVerificationRequired;
        this.headers = requireNonNull(headers);
        this.openIdCredential = openIdCredential;
        this.desiredChecksum = desiredChecksums.isEmpty() ? null : desiredChecksums.get(0);
        this.desiredChecksums = Set.copyOf(desiredChecksums);
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

    public Set<ChecksumType> getDesiredChecksums() {
        return desiredChecksums;
    }

    private void readObject(java.io.ObjectInputStream stream)
          throws ClassNotFoundException, IOException {
        stream.defaultReadObject();

        // Handle objects sent from old doors.
        if (desiredChecksums == null) {
            desiredChecksums = desiredChecksum == null
                    ? Collections.emptySet()
                    : Set.of(desiredChecksum);
        }
    }
}
