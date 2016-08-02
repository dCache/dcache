package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

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

    public MoverInfoMessage(String cellName,
                            PnfsId pnfsId)
    {

        super("transfer", "pool", cellName, pnfsId);
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

    public String getAdditionalInfo()
    {
        return _dataTransferred + " "
               + _connectionTime + ' '
               + _fileCreated + " {"
               + _protocolInfo + "} ["
               + _initiator + "] ";
    }

    public String toString()
    {
        return getInfoHeader() + ' ' +
               getFileInfo() + ' ' +
               getAdditionalInfo() +
               getResult();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
