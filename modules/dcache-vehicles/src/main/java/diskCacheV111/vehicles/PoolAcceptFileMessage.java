package diskCacheV111.vehicles;

import java.util.EnumSet;
import java.util.OptionalLong;

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
    private final long _maximumSize;

    public PoolAcceptFileMessage(String pool,
                                 ProtocolInfo protocolInfo,
                                 FileAttributes fileAttributes,
                                 Assumption assumption,
                                 OptionalLong maximumSize)
    {
        this(pool, protocolInfo, fileAttributes, assumption, maximumSize,
             fileAttributes.isDefined(SIZE) ? fileAttributes.getSize() : 0);
    }

    public PoolAcceptFileMessage(String pool,
                                 ProtocolInfo protocolInfo,
                                 FileAttributes fileAttributes,
                                 Assumption assumption,
                                 OptionalLong maximumSize,
                                 long preallocated)
    {
        super(pool, protocolInfo, fileAttributes, assumption);
        checkArgument(fileAttributes.isDefined(
                EnumSet.of(ACCESS_LATENCY, RETENTION_POLICY)));
        _preallocated = preallocated;
        _maximumSize = maximumSize.orElse(0);
    }

    public long getPreallocated()
    {
        return _preallocated;
    }

    public OptionalLong getMaximumSize()
    {
        return _maximumSize == 0 ? OptionalLong.empty() : OptionalLong.of(_maximumSize);
    }
}
