package org.dcache.services.pinmanager1;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.MessageReply;

import diskCacheV111.vehicles.PinManagerMessage;
import diskCacheV111.vehicles.PinManagerPinMessage;
import diskCacheV111.vehicles.PinManagerUnpinMessage;
import diskCacheV111.vehicles.PinManagerExtendLifetimeMessage;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;
import diskCacheV111.util.CacheException;

public class PinManagerMessageReceiver
    implements CellMessageReceiver
{
    private PinManager _pinManager;

    public void setPinManager(PinManager pinManager)
    {
        _pinManager = pinManager;
    }

    public MessageReply messageArrived(PinManagerPinMessage message)
        throws CacheException
    {
        return _pinManager.pin(message);
    }

    public MessageReply messageArrived(PinManagerExtendLifetimeMessage message)
        throws CacheException
    {
        return _pinManager.extendLifetime(message);
    }

    public void messageArrived(PoolRemoveFilesMessage message)
    {
        _pinManager.removeFiles(message);
    }

    public MessageReply messageArrived(PinManagerMovePinMessage message)
        throws CacheException
    {
        return _pinManager.movePin(message);
    }
}