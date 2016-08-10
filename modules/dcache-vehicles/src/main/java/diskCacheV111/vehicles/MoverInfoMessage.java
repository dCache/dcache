package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

public class MoverInfoMessage extends PnfsFileInfoMessage
{
    private long _dataTransferred;
    private long _connectionTime;

    private ProtocolInfo _protocolInfo;
    private boolean _fileCreated;
    private String _initiator = "<undefined>";
    private boolean _isP2p;

    private static final long serialVersionUID = -7013160118909496211L;
    private String _transferPath;

    public MoverInfoMessage(CellAddressCore address, PnfsId pnfsId)
    {
        super("transfer", "pool", address, pnfsId);
    }

    public void setFileCreated(boolean created)
    {
        _fileCreated = created;
    }

    public void setTransferAttributes(
            long dataTransferred,
            long connectionTime,
            ProtocolInfo protocolInfo)
    {
        _dataTransferred = dataTransferred;
        _connectionTime = connectionTime;
        _protocolInfo = protocolInfo;
    }

    public void setInitiator(String transaction)
    {
        _initiator = transaction;
    }

    public void setP2P(boolean isP2p)
    {
        _isP2p = isP2p;
    }

    public String getInitiator()
    {
        return _initiator;
    }

    public long getDataTransferred()
    {
        return _dataTransferred;
    }

    public long getConnectionTime()
    {
        return _connectionTime;
    }

    public boolean isFileCreated()
    {
        return _fileCreated;
    }

    public boolean isP2P()
    {
        return _isP2p;
    }

    public ProtocolInfo getProtocolInfo()
    {
        return _protocolInfo;
    }

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path)
    {
        _transferPath = path;
    }

    @Override
    public String toString()
    {
        return "MoverInfoMessage{" +
               "dataTransferred=" + _dataTransferred +
               ", connectionTime=" + _connectionTime +
               ", protocolInfo=" + _protocolInfo +
               ", fileCreated=" + _fileCreated +
               ", initiator='" + _initiator + '\'' +
               ", isP2p=" + _isP2p +
               ", transferPath='" + _transferPath + '\'' +
               "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
