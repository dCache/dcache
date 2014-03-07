package diskCacheV111.vehicles;

import org.dcache.namespace.FileAttribute;
import org.dcache.vehicles.FileAttributes;

/**
 * Requests pool manager to provide a pool to which a file with the
 * given properties can be written.
 *
 * Preallocated space may be specified. In this case pool manager will
 * select a pool with enough free space to hold the preallcoated space.
 *
 * The preallocated space is not the same as the expected file size. It
 * is not an error if the final size of the file differs from the
 * preallocated space. If an expected file size is known, then it should
 * be made available as one of the file attributes.
 */
public class PoolMgrSelectWritePoolMsg extends PoolMgrSelectPoolMsg
{
    private static final long serialVersionUID = 1935227143005174577L;

    private final long _preallocated;

    /**
     * @param fileAttributes FileAttributes of the file to read
     * @param protocolInfo ProtocolInfo describe the transfer
     */
    public PoolMgrSelectWritePoolMsg(FileAttributes fileAttributes,
                                     ProtocolInfo protocolInfo)
    {
        this(fileAttributes, protocolInfo,
             fileAttributes.isDefined(FileAttribute.SIZE) ? fileAttributes.getSize() : 0);
    }

    /**
     * @param fileAttributes FileAttributes of the file to read
     * @param protocolInfo ProtocolInfo describe the transfer
     * @param preallocated Space in bytes preallocated to the file, or
     *                     zero if no space is preallocated
     */
    public PoolMgrSelectWritePoolMsg(FileAttributes fileAttributes,
                                     ProtocolInfo protocolInfo,
                                     long preallocated)
    {
        super(fileAttributes, protocolInfo);
        _preallocated = preallocated;
    }

    public long getPreallocated()
    {
        return _preallocated;
    }
}
