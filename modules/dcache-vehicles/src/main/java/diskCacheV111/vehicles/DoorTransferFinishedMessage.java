package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;
import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import org.dcache.vehicles.FileAttributes;

/**
 * Signals the completion of a transfer on a pool.
 */
@ParametersAreNonnullByDefault
public class DoorTransferFinishedMessage extends Message {

    private final ProtocolInfo _protocol;
    private final FileAttributes _fileAttributes;
    private final PnfsId _pnfsId;
    private final String _poolName;
    private final String _ioQueueName;
    private static final long serialVersionUID = -7563456962335030196L;

    private MoverInfoMessage moverInfo;

    public DoorTransferFinishedMessage(long id,
          PnfsId pnfsId,
          ProtocolInfo protocol,
          FileAttributes fileAttributes,
          String poolName,
          @Nullable String ioQueueName) {
        setId(id);
        _fileAttributes = requireNonNull(fileAttributes);
        _protocol = requireNonNull(protocol);
        _pnfsId = requireNonNull(pnfsId);
        _poolName = requireNonNull(poolName);
        _ioQueueName = ioQueueName;
    }

    public String getIoQueueName() {
        return _ioQueueName;
    }

    public ProtocolInfo getProtocolInfo() {
        return _protocol;
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public MoverInfoMessage getMoverInfo() {
        return moverInfo;
    }

    public void setMoverInfo(MoverInfoMessage moverInfo) {
        this.moverInfo = moverInfo;
    }

    public PnfsId getPnfsId() {
        return _pnfsId;
    }

    public String getPoolName() {
        return _poolName;
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}


