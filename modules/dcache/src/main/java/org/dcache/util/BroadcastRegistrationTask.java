package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.multicaster.BroadcastRegisterMessage;
import dmg.cells.services.multicaster.BroadcastUnregisterMessage;

/**
 * TimerTask to periodically register a broadcast subscription.
 */
public class BroadcastRegistrationTask implements Runnable
{
    /** Cell used to send message. */
    private final CellEndpoint _cellEndpoint;

    /** Name of the message class to register. */
    private final String _eventClass;

    /** Target cell to which to send broadcasts. */
    private final CellPath _target;

    private static CellPath _broadcast = new CellPath("broadcast");

    private static final Logger _logger =
        LoggerFactory.getLogger(BroadcastRegistrationTask.class);

    private long _expires;
    private boolean _isCancelOnFailure;

    public BroadcastRegistrationTask(CellEndpoint cellEndpoint, Class<?> eventClass, CellPath target)
    {
        _cellEndpoint = cellEndpoint;
        _eventClass = eventClass.getName();
        _target = target;
    }

    @Override
    public void run()
    {
        register();
    }

    public void setExpires(long millis)
    {
        _expires = millis;
    }

    public void setCancelOnFailure(boolean isCancelOnFailure)
    {
        _isCancelOnFailure = isCancelOnFailure;
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
            message.setExpires(_expires);
            message.setCancelOnFailure(_isCancelOnFailure);
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
            _logger.debug("Failed to unregister with broadcast cell: No route to cell");
        }
    }
}

