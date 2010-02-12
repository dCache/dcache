package org.dcache.services.hsmcleaner;

import java.util.TimerTask;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.multicaster.BroadcastRegisterMessage;
import dmg.cells.services.multicaster.BroadcastUnregisterMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TimerTask to periodically register a broadcast subscription.
 */
public class BroadcastRegistrationTask extends TimerTask
{
    /** Cell used to send message. */
    private final CellEndpoint _cellEndpoint;

    /** Name of the message class to register. */
    private final String _eventClass;

    /** Target cell to which to send broadcasts. */
    private final CellPath _target;

    private static CellPath _broadcast = new CellPath("broadcast");

	private static final Logger _logger = LoggerFactory.getLogger(BroadcastRegistrationTask.class);

    public BroadcastRegistrationTask(CellEndpoint cellEndpoint, String eventClass, CellPath target)
    {
        _cellEndpoint = cellEndpoint;
        _eventClass = eventClass;
        _target = target;
    }

    public void run()
    {
        register();
    }

    /**
     * Sends a registration message to the broadcast cell.
     *
     * Communication failures are logged and then ignored.
     */
    public void register()
    {
        try {
            BroadcastRegisterMessage message =
                new BroadcastRegisterMessage(_eventClass, _target);
            _cellEndpoint.sendMessage(new CellMessage(_broadcast, message));
        } catch (NoRouteToCellException e) {
            _logger.error("Failed to register with broadcast cell: No route to cell");
        }
    }

    /**
     * Sends an unregistration message to the broadcast cell.
     *
     * Communication failures are logged and then ignored.
     */
    public void unregister()
    {
        try {
            BroadcastUnregisterMessage message =
                new BroadcastUnregisterMessage(_eventClass, _target);
            _cellEndpoint.sendMessage(new CellMessage(_broadcast, message));
        } catch (NoRouteToCellException e) {
            _logger.error("Failed to register with broadcast cell: No route to cell");
        }
    }
}

