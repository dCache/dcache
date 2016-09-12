package diskCacheV111.vehicles;

import javax.annotation.Nonnull;

import java.util.EnumSet;

import diskCacheV111.poolManager.RequestContainerV5;

import org.dcache.pool.assumption.Assumption;
import org.dcache.pool.assumption.Assumptions;
import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolMgrSelectPoolMsg extends PoolMgrGetPoolMsg {

    private static final long serialVersionUID = -5874326080375390208L;

    private ProtocolInfo _protocolInfo;
    private String       _ioQueueName;
    private String       _pnfsPath;
    private String       _linkGroup;
    private final EnumSet<RequestContainerV5.RequestState> _allowedStates;

    private String _transferPath;

    private Assumption _assumption;

    public PoolMgrSelectPoolMsg(FileAttributes fileAttributes,
                                ProtocolInfo protocolInfo)
    {
        this(fileAttributes, protocolInfo, RequestContainerV5.allStates);
    }

    public PoolMgrSelectPoolMsg(FileAttributes fileAttributes,
                                ProtocolInfo protocolInfo,
                                EnumSet<RequestContainerV5.RequestState> allowedStates)
    {
        super(fileAttributes);
        _protocolInfo = checkNotNull(protocolInfo);
        _allowedStates = checkNotNull(allowedStates);
    }

    public void setAssumption(Assumption assumption)
    {
        _assumption = assumption;
    }

    public Assumption getAssumption()
    {
        return _assumption == null ? Assumptions.none() : _assumption;
    }

    @Nonnull
    public ProtocolInfo getProtocolInfo(){ return _protocolInfo; }
    public void setProtocolInfo( ProtocolInfo protocolInfo ){ _protocolInfo = checkNotNull(protocolInfo); }
    public void setIoQueueName( String ioQueueName ){ _ioQueueName = ioQueueName ; }
    public String getIoQueueName(){ return _ioQueueName ; }

    public String getBillingPath() {
        return _pnfsPath;
    }

    public void setBillingPath(String pnfsPath) {
        _pnfsPath = pnfsPath;
    }

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path) {
        _transferPath = path;
    }

    public void setLinkGroup(String linkGroup) {
        _linkGroup = linkGroup;
    }

    public String getLinkGroup() {
        return _linkGroup;
    }

    @Nonnull
    public EnumSet<RequestContainerV5.RequestState> getAllowedStates() {
        return _allowedStates;
    }

}
