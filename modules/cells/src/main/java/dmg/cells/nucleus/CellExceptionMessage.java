package dmg.cells.nucleus;

/**
 * Specialized CellMessage for delivering NoRouteToCellException. Prevents
 * that a delivery failure bounces back and forth.
 */
public class CellExceptionMessage extends CellMessage
{
    private static final long serialVersionUID = -5819709105553527283L;

    public CellExceptionMessage(CellPath addr, NoRouteToCellException msg)
    {
        super(addr, msg);
    }

    public NoRouteToCellException getException()
    {
        return (NoRouteToCellException) getMessageObject();
    }
}
