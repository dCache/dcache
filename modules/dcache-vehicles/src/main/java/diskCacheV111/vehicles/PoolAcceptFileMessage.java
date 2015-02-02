package diskCacheV111.vehicles;

import java.util.EnumSet;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.namespace.FileAttribute.ACCESS_LATENCY;
import static org.dcache.namespace.FileAttribute.RETENTION_POLICY;
import static org.dcache.namespace.FileAttribute.SIZE;

public class PoolAcceptFileMessage extends PoolIoFileMessage
{
    private static final long serialVersionUID = 7898737438685700742L;

    private final long _preallocated;

    public PoolAcceptFileMessage(String pool,
                                 ProtocolInfo protocolInfo,
                                 FileAttributes fileAttributes)
    {
        this(pool, protocolInfo, fileAttributes,
             fileAttributes.isDefined(SIZE) ? fileAttributes.getSize() : 0);
    }

    public PoolAcceptFileMessage(String pool,
                                 ProtocolInfo protocolInfo,
                                 FileAttributes fileAttributes,
                                 long preallocated)
    {
        super(pool, protocolInfo, fileAttributes);
        checkArgument(fileAttributes.isDefined(
                EnumSet.of(ACCESS_LATENCY, RETENTION_POLICY)));
        _preallocated = preallocated;
    }

    public long getPreallocated()
    {
        return _preallocated;
    }
}
