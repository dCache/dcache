package dmg.cells.nucleus;

import java.io.NotSerializableException;

/**
 * Interface for sending messages.
 */
public interface CellEndpoint
{
    void sendMessage(CellMessage envelope)
        throws NotSerializableException,
               NoRouteToCellException;

    void sendMessage(CellMessage envelope,
                            CellMessageAnswerable callback,
                            long    timeout)
        throws NotSerializableException;

    void sendMessage(CellMessage msg,
                            boolean locally, boolean remotely)
        throws NotSerializableException,
               NoRouteToCellException;

    CellMessage sendAndWait(CellMessage msg,
                                   long millisecs)
        throws NotSerializableException,
               NoRouteToCellException,
               InterruptedException;

    CellInfo getCellInfo();
}