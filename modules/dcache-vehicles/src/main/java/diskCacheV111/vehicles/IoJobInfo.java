package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;
import java.net.InetSocketAddress;
import java.util.List;
import javax.annotation.Nullable;

public class IoJobInfo extends JobInfo {

    @Nullable
    private final Long _requestedBytes;
    private final long _bytesTransferred;
    private final long _transferTime;
    private final long _lastTransferred;
    private final List<InetSocketAddress> _remoteConnections;
    private final PnfsId _pnfsId;

    private static final long serialVersionUID = -7987228538353684951L;

    public IoJobInfo(long submitTime, long startTime, String state, int id, String clientName,
          long clientId,
          PnfsId pnfsId, long bytesTransferred, Long requestedBytes, long transferTime,
          long lastTransferred,
          @Nullable List<InetSocketAddress> remoteConnections) {
        super(submitTime, startTime, state, id, clientName, clientId);
        _pnfsId = pnfsId;
        _bytesTransferred = bytesTransferred;
        _transferTime = transferTime;
        _lastTransferred = lastTransferred;
        _remoteConnections = remoteConnections;
        _requestedBytes = requestedBytes;
    }

    public long getTransferTime() {
        return _transferTime;
    }

    public long getBytesTransferred() {
        return _bytesTransferred;
    }

    public long getLastTransferred() {
        return _lastTransferred;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public String toString() {
        return super.toString() +
              _pnfsId +
              ";B=" + _bytesTransferred +
              ";T=" + _transferTime +
              ";L=" + ((System.currentTimeMillis() - _lastTransferred) / 1000) + ';';
    }

    /**
     * A list of remote TCP endpoints to which the mover is connected.  Returns null if this
     * information is not available.
     */
    @Nullable
    public List<InetSocketAddress> remoteConnections() {
        return _remoteConnections;
    }

    /**
     * The expected number of bytes for this transfer, if known.  Returns null if the value is
     * unknown.
     */
    @Nullable
    public Long requestedBytes() {
        return _requestedBytes;
    }
}
