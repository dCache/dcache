package diskCacheV111.vehicles;

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
        if (_fileAttributes == null && _storageInfo != null) {
            _fileAttributes = new FileAttributes();
            _fileAttributes.setStorageInfo(_storageInfo);
            _fileAttributes.setAccessLatency(_storageInfo.getAccessLatency());
            _fileAttributes.setRetentionPolicy(_storageInfo.getRetentionPolicy());
            _fileAttributes.setSize(_storageInfo.getFileSize());
        }
        return _fileAttributes;
    }

    public String getPoolName() {
    	return _poolName;
    }
}
