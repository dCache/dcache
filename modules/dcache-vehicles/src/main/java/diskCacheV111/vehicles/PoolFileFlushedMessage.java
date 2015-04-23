package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import org.dcache.vehicles.FileAttributes;

/**
 * Signals that a file was flushed.
 */
public class PoolFileFlushedMessage extends PnfsMessage
{
    private final FileAttributes _fileAttributes;
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
}
