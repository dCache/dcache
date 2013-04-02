package diskCacheV111.vehicles;

import java.io.IOException;
import java.io.ObjectInputStream;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * Signals that a file was flushed.
 */
public class PoolFileFlushedMessage extends PnfsMessage
{
    @Deprecated // Can be removed in 2.7
    private StorageInfo _storageInfo;
    private FileAttributes _fileAttributes;
    private final String _poolName;

    private static final long serialVersionUID = 1856537534158868883L;

    public PoolFileFlushedMessage(String poolName, PnfsId pnfsId, FileAttributes fileAttributes) {
    	super(pnfsId);
        _poolName = poolName;
        _fileAttributes = fileAttributes;
        setReplyRequired(true);
    }

    public FileAttributes getFileAttributes() {
        return _fileAttributes;
    }

    public String getPoolName() {
    	return _poolName;
    }

    private void readObject(ObjectInputStream stream)
            throws IOException, ClassNotFoundException
    {
        stream.defaultReadObject();
        if (_fileAttributes == null) {
            _fileAttributes = new FileAttributes();
            if (_storageInfo != null) {
                StorageInfos.injectInto(_storageInfo, _fileAttributes);
            }
            _fileAttributes.setPnfsId(getPnfsId());
        }
    }
}
