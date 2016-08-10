package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

public class WarningPnfsFileInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = -5457677492665743755L;
    private String _transferPath;

    public WarningPnfsFileInfoMessage(String cellType,
                                      CellAddressCore address,
                                      PnfsId pnfsId,
                                      int rc,
                                      String returnMessage)
    {
        super("warning", cellType, address, pnfsId);
        setResult(rc, returnMessage);
    }

    @Override
    public String toString()
    {
        return "WarningPnfsFileInfoMessage{" +
               "transferPath='" + _transferPath + '\'' +
               "} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }

    public String getTransferPath()
    {
        return _transferPath != null ? _transferPath : getBillingPath();
    }

    public void setTransferPath(String path)
    {
        _transferPath = path;
    }
}
