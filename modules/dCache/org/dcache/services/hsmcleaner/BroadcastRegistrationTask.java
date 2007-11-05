package org.dcache.services.hsmcleaner;

import java.util.TimerTask;
import java.io.NotSerializableException;

import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.multicaster.BroadcastRegisterMessage;

/** 
 * TimerTask to periodically register a broadcast subscription.
 */
public class BroadcastRegistrationTask extends TimerTask
{
    /** Cell used to send message. */
    private final CellAdapter _cell;

    /** Name of the message class to register. */
    private final String _eventClass;

    /** Target cell to which to send broadcasts. */
    private final CellPath _target;

    private static CellPath _broadcast = new CellPath("broadcast");

    public BroadcastRegistrationTask(CellAdapter cell, String eventClass, CellPath target)
    {
        _cell = cell;
        _eventClass = eventClass;
        _target = target;
    }

    public void run()
    {   
        try {
            BroadcastRegisterMessage message = 
                new BroadcastRegisterMessage(_eventClass, _target);
            _cell.sendMessage(new CellMessage(_broadcast, message));
        } catch (NotSerializableException e) {
            throw new RuntimeException("Failed to register with broadcast cell", e);
        } catch (NoRouteToCellException e) {
            _cell.esay("Failed to register with broadcast cell: No route to cell");
        }
    }
}

