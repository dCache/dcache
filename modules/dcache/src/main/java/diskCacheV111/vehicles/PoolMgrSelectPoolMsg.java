package diskCacheV111.vehicles;

import static java.util.Objects.requireNonNull;

import diskCacheV111.poolManager.RequestContainerV5;
import java.util.EnumSet;
import java.util.Set;
import javax.annotation.Nonnull;
import org.dcache.vehicles.FileAttributes;

public class PoolMgrSelectPoolMsg extends PoolMgrGetPoolMsg {

    private static final long serialVersionUID = -5874326080375390208L;

    private ProtocolInfo _protocolInfo;
    private String _ioQueueName;
    private String _pnfsPath;
    private String _linkGroup;
    private Set<String> _excludedHosts;
    private final EnumSet<RequestContainerV5.RequestState> _allowedStates;

    private String _transferPath;

    public PoolMgrSelectPoolMsg(FileAttributes fileAttributes,
          ProtocolInfo protocolInfo) {
        this(fileAttributes, protocolInfo, RequestContainerV5.allStates);
    }

    public PoolMgrSelectPoolMsg(FileAttributes fileAttributes,
          ProtocolInfo protocolInfo,
          EnumSet<RequestContainerV5.RequestState> allowedStates) {
        super(fileAttributes);
        _protocolInfo = requireNonNull(protocolInfo);
        _allowedStates = requireNonNull(allowedStates);
    }

    @Nonnull
    public ProtocolInfo getProtocolInfo() {
        return _protocolInfo;
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo) {
        _protocolInfo = requireNonNull(protocolInfo);
    }

    public void setIoQueueName(String ioQueueName) {
        _ioQueueName = ioQueueName;
    }

    public String getIoQueueName() {
        return _ioQueueName;
    }

    public String getBillingPath() {
        return _pnfsPath;
    }

    public void setBillingPath(String pnfsPath) {
        _pnfsPath = pnfsPath;
    }

    public String getTransferPath() {
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

    public void setExcludedHosts(Set<String> excludedHosts) {
        _excludedHosts = excludedHosts;
    }

    public Set<String> getExcludedHosts() {
        return _excludedHosts;
    }
}
