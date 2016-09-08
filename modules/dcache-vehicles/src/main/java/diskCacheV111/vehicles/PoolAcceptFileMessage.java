package diskCacheV111.vehicles;

import java.util.EnumSet;

import org.dcache.pool.assumption.Assumption;
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
                                 FileAttributes fileAttributes,
                                 Assumption assumption)
    {
        this(pool, protocolInfo, fileAttributes, assumption,
             fileAttributes.isDefined(SIZE) ? fileAttributes.getSize() : 0);
    }

    public PoolAcceptFileMessage(String pool,
                                 ProtocolInfo protocolInfo,
                                 FileAttributes fileAttributes,
                                 Assumption assumption,
                                 long preallocated)
    {
        super(pool, protocolInfo, fileAttributes, assumption);
        checkArgument(fileAttributes.isDefined(
                EnumSet.of(ACCESS_LATENCY, RETENTION_POLICY)));
        _preallocated = preallocated;
    }

    public long getPreallocated()
    {
        return _preallocated;
    }
}
