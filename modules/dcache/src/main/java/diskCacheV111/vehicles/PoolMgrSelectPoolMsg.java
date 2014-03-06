// $Id: PoolMgrSelectPoolMsg.java,v 1.7 2006-10-10 13:50:49 tigran Exp $

package diskCacheV111.vehicles ;

import javax.annotation.Nonnull;

import java.util.EnumSet;

import diskCacheV111.poolManager.RequestContainerV5;
import diskCacheV111.util.FsPath;

import org.dcache.vehicles.FileAttributes;

import static com.google.common.base.Preconditions.checkNotNull;

public class PoolMgrSelectPoolMsg extends PoolMgrGetPoolMsg {

    private static final long serialVersionUID = -5874326080375390208L;

    private ProtocolInfo _protocolInfo;
    private String       _ioQueueName;
    private String       _pnfsPath;
    private String       _linkGroup;
    private final EnumSet<RequestContainerV5.RequestState> _allowedStates;

    private boolean _skipCostUpdate;

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

    public void setSkipCostUpdate(boolean value)
    {
        _skipCostUpdate = value;
    }

    public boolean getSkipCostUpdate()
    {
        return _skipCostUpdate;
    }

    @Nonnull
    public ProtocolInfo getProtocolInfo(){ return _protocolInfo; }
    public void setProtocolInfo( ProtocolInfo protocolInfo ){ _protocolInfo = checkNotNull(protocolInfo); }
    public void setIoQueueName( String ioQueueName ){ _ioQueueName = ioQueueName ; }
    public String getIoQueueName(){ return _ioQueueName ; }

    public FsPath getPnfsPath() {
        return _pnfsPath != null ? new FsPath(_pnfsPath) : null;
    }

    public void setPnfsPath(FsPath pnfsPath) {
        this._pnfsPath = pnfsPath.toString();
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
