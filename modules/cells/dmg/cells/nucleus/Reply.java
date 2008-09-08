package dmg.cells.nucleus;

public interface Reply
{
    void deliver(CellEndpoint endpoint, CellMessage envelope)
        throws NoRouteToCellException;
}