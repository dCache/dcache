package diskCacheV111.vehicles;

import diskCacheV111.util.FsPath;
import diskCacheV111.util.PnfsId;

public class WarningPnfsFileInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = -5457677492665743755L;
    private String _transferPath;

    public WarningPnfsFileInfoMessage(String cellType,
                                      String cellName,
                                      PnfsId pnfsId,
                                      int rc,
                                      String returnMessage)
    {
        super("warning", cellType, cellName, pnfsId);
        setResult(rc, returnMessage);
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

    public void setTransferPath(FsPath path)
    {
        setTransferPath(path.toString());
    }
}
