package org.dcache.cell;

import java.io.NotSerializableException;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;

/**
 * Interface for sending messages.
 */
public interface CellEndpoint
{
    public void sendMessage(CellMessage envelope)
        throws NotSerializableException,
               NoRouteToCellException;

    public void sendMessage(CellMessage envelope,
                            CellMessageAnswerable callback,
                            long    timeout)
        throws NotSerializableException;

    public void sendMessage(CellMessage msg,
                            boolean locally, boolean remotely)
        throws NotSerializableException,
               NoRouteToCellException;

    public CellMessage sendAndWait(CellMessage msg,
                                   long millisecs)
        throws NotSerializableException,
               NoRouteToCellException,
               InterruptedException;
}