package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

public class PoolHitInfoMessage extends PnfsFileInfoMessage {

    private ProtocolInfo _protocolInfo;
    private boolean      _fileCached;

    private static final long serialVersionUID = -1487408937648228544L;
    private String _transferPath;

    public PoolHitInfoMessage(CellAddressCore address, PnfsId pnfsId)
    {
		super("hit", "pool", address, pnfsId);
    }

    public void setFileCached(boolean cached)
    {
		_fileCached = cached;
    }

    public void setProtocolInfo(ProtocolInfo protocolInfo)
    {
		_protocolInfo = protocolInfo;
    }

    public boolean getFileCached()
    {
		return _fileCached;
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
        return "PoolHitInfoMessage{" +
               "protocolInfo=" + _protocolInfo +
               ", fileCached=" + _fileCached +
               ", transferPath='" + _transferPath + '\'' +
               "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
