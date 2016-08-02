package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

public class RemoveFileInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = 705215552239829093L;

    public RemoveFileInfoMessage(String cellName, PnfsId pnfsId)
    {
        super("remove", "pool", cellName, pnfsId);
    }

    public String toString()
    {
        return getInfoHeader() + ' ' +
               getFileInfo() + ' ' +
               getResult();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
