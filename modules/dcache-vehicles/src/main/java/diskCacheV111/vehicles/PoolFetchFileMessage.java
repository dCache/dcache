package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import diskCacheV111.util.PnfsId;
import org.dcache.vehicles.FileAttributes;

/**
 * restore file from HSM
 */
public class PoolFetchFileMessage extends PoolMessage {

    private final FileAttributes _fileAttributes;

    private static final long serialVersionUID = 1856537534158868883L;

    public PoolFetchFileMessage(String poolName, FileAttributes fileAttributes) {
        super(poolName);
        _fileAttributes = requireNonNull(fileAttributes);
        setReplyRequired(true);
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public PnfsId getPnfsId() {
        return _fileAttributes.getPnfsId();
    }

    @Override
    public String getDiagnosticContext() {
        return super.getDiagnosticContext() + ' ' + getPnfsId();
    }
}
