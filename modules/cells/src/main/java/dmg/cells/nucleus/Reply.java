package dmg.cells.nucleus;

import java.io.Serializable;

public interface Reply extends Serializable
{
    void deliver(CellEndpoint endpoint, CellMessage envelope)
        throws NoRouteToCellException;
}
