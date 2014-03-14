package org.dcache.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.services.multicaster.BroadcastRegisterMessage;
import dmg.cells.services.multicaster.BroadcastUnregisterMessage;

import org.dcache.cells.CellStub;

/**
 * TimerTask to periodically register a broadcast subscription.
 */
public class BroadcastRegistrationTask implements Runnable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BroadcastRegistrationTask.class);

    /** Name of the message class to register. */
    private Class<?> _eventClass;

    /** Target cell to which to send broadcasts. */
    private CellPath _target;

    private CellStub _broadcast;

    private long _expires;
    private boolean _isCancelOnFailure;

    public BroadcastRegistrationTask()
    {
    }

    public void setEventClass(Class<?> eventClass)
    {
        _eventClass = eventClass;
    }

    public Class<?> getEventClass()
    {
        return _eventClass;
    }

    public void setBroadcastStub(CellStub stub)
    {
        _broadcast = stub;
    }

    public void setTarget(CellPath target)
    {
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
                new BroadcastRegisterMessage(_eventClass.getName(), _target);
            message.setExpires(_expires);
            message.setCancelOnFailure(_isCancelOnFailure);
            _broadcast.notify(message);
        } catch (NoRouteToCellException e) {
            LOGGER.error("Failed to register with broadcast cell: No route to cell {}", _broadcast.getDestinationPath());
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
                new BroadcastUnregisterMessage(_eventClass.getName(), _target);
            _broadcast.notify(message);
        } catch (NoRouteToCellException e) {
            LOGGER.info("Failed to unregister with broadcast cell: No route to cell {}", _broadcast.getDestinationPath());
        }
    }
}

