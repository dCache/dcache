package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellAddressCore;
import java.io.IOException;
import java.io.Serializable;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.security.auth.Subject;
import org.dcache.auth.Subjects;

public class IoDoorEntry implements Serializable {

    private final long _serialId;
    private final PnfsId _pnfsId;
    private String _pool;
    private final String _status;
    private final long _waitingSince;
    private final String _replyHost;
    private final Subject _subject;
    private final String _path;

    @Nullable
    private final String _protocol; // overrides door protocol, if door supports multiple protocols.

    @Nullable
    private final CellAddressCore _intermediate; // The cell that started the mover if not the door.

    private static final long serialVersionUID = 7283617314269359997L;

    public IoDoorEntry(long serialId, PnfsId pnfsId, String path,
          Subject subject, String pool, String status,
          long waitingSince, String replyHost) {
        this(serialId, pnfsId, path, subject, pool, status, waitingSince,
                replyHost, null, null);
    }

    public IoDoorEntry(long serialId, PnfsId pnfsId, String path,
          Subject subject, String pool, String status,
          long waitingSince, String replyHost,
          @Nullable CellAddressCore intermediate,
          @Nullable String protocol) {
        _serialId = serialId;
        _pnfsId = pnfsId;
        _path = path;
        _subject = subject;
        _pool = pool;
        _status = status;
        _waitingSince = waitingSince;
        _replyHost = requireNonNull(replyHost);
        _intermediate = intermediate;
        _protocol = protocol;
    }

    public long getSerialId() {
        return _serialId;
    }

    @Nullable
    public String getPath() {
        return _path;
    }

    @Nullable
    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    @Nullable
    public String getPool() {
        return _pool;
    }

    @Nullable
    public String getStatus() {
        return _status;
    }

    /**
     * Under certain conditions it is possible to receive transfer info sent before login has been
     * performed by the door.  It is thus possible that the Subject also be <code>null</code>.
     */
    @Nullable
    public Subject getSubject() {
        return _subject;
    }

    public long getWaitingSince() {
        return _waitingSince;
    }

    @Nonnull
    public String getReplyHost() {
        return _replyHost;
    }

    public String toString() {
        return _serialId + ";" + _pnfsId + ';' + Subjects.getDisplayName(_subject) + ';'
              + _replyHost + ';' + _pool + ';' + _status + ';'
              + (System.currentTimeMillis() - _waitingSince) + ';';
    }

    /**
     * Optionally the protocol for this transfer.  Empty if this transfer is
     * the default protocol for this door.
     */
    public Optional<String> getProtocol() {
        return Optional.ofNullable(_protocol);
    }

    /**
     * Optionally the address of the cell that started the mover.  Empty if
     * the door started the mover.
     */
    public Optional<CellAddressCore> getIntermediate() {
        return Optional.ofNullable(_intermediate);
    }

    private void readObject(java.io.ObjectInputStream stream)
          throws IOException, ClassNotFoundException {
        stream.defaultReadObject();
        if (_pool != null) {
            _pool = _pool.intern();
        }
    }
}