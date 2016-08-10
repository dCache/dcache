package diskCacheV111.vehicles;

import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellAddressCore;

public class RemoveFileInfoMessage extends PnfsFileInfoMessage
{
    private static final long serialVersionUID = 705215552239829093L;

    public RemoveFileInfoMessage(CellAddressCore address, PnfsId pnfsId)
    {
        super("remove", "pool", address, pnfsId);
    }

    @Override
    public String toString()
    {
        return "RemoveFileInfoMessage{} " + super.toString();
    }

    @Override
    public void accept(InfoMessageVisitor visitor)
    {
        visitor.visit(this);
    }
}
