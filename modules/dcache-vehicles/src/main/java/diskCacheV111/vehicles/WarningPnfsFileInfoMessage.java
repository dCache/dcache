package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class WarningPnfsFileInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = -5457677492665743755L;

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
}
