package org.dcache.services.pinmanager1;

import org.dcache.cells.CellMessageReceiver;
import org.dcache.cells.MessageReply;

import org.dcache.pinmanager.PinManagerPinMessage;
import org.dcache.pinmanager.PinManagerUnpinMessage;
import org.dcache.pinmanager.PinManagerExtendPinMessage;
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

    public MessageReply<PinManagerPinMessage> messageArrived(PinManagerPinMessage message)
        throws CacheException
    {
        return _pinManager.pin(message);
    }

    public MessageReply<PinManagerExtendPinMessage> messageArrived(PinManagerExtendPinMessage message)
        throws CacheException
    {
        return _pinManager.extendLifetime(message);
    }

    public void messageArrived(PoolRemoveFilesMessage message)
    {
        _pinManager.removeFiles(message);
    }

    public MessageReply<PinManagerMovePinMessage> messageArrived(PinManagerMovePinMessage message)
        throws CacheException
    {
        return _pinManager.movePin(message);
    }
}