package dmg.cells.nucleus;

import java.io.Serializable;

public class CellExceptionMessage extends CellMessage
{
    private static final long serialVersionUID = -5819709105553527283L;

    public CellExceptionMessage(CellPath addr, Serializable msg)
    {
        super(addr, msg);
    }

    private CellExceptionMessage()
    {
    }

    @Override
    protected CellMessage cloneWithoutFields()
    {
        return new CellExceptionMessage();
    }
}
