package dmg.cells.nucleus;

import java.io.NotSerializableException;

public interface Reply
{
    void deliver(CellEndpoint endpoint, CellMessage envelope)
        throws NotSerializableException,
               NoRouteToCellException;
}