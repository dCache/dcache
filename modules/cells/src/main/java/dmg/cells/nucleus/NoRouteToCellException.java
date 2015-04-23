package dmg.cells.nucleus;

import static java.util.Objects.requireNonNull;

public class NoRouteToCellException extends Exception
{
    private static final long serialVersionUID = -7538969590898439933L;

    private final UOID _uoid;
    private final CellPath _path;

    public NoRouteToCellException(UOID uoid, CellPath path, String str)
    {
        super(str);
        _uoid = requireNonNull(uoid);
        _path = path.clone();
    }

    @Override
    public String toString()
    {
        return "NoRouteToCell[" + getMessage() + "]";
    }

    @Override
    public String getMessage()
    {
        return "Failed to deliver message " + _uoid + " to " + _path + ": " + super.getMessage();
    }

    public UOID getUOID()
    {
        return _uoid;
    }

    public CellPath getDestinationPath()
    {
        return _path;
    }
}
