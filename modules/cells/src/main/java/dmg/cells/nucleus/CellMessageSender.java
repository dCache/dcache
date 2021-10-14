package dmg.cells.nucleus;

/**
 * Classes implementing this interface can send Cell messages through a cell endpoint. The
 * CellEndpoint is registered via a setter method.
 */
public interface CellMessageSender {

    void setCellEndpoint(CellEndpoint endpoint);
}
