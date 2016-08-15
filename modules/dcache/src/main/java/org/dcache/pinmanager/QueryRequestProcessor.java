package org.dcache.pinmanager;


import dmg.cells.nucleus.CellMessageReceiver;

import org.springframework.beans.factory.annotation.Required;

import diskCacheV111.util.CacheException;


/**
 * Process request to get the count of pins.
 *
 */
public class QueryRequestProcessor
        implements CellMessageReceiver {

    private PinDao _dao;

    @Required
    public void setDao(PinDao dao) {
        _dao = dao;
    }


    public PinManagerCountPinsMessage
    messageArrived(PinManagerCountPinsMessage message)
            throws CacheException {
        message.setCount(_dao.count(_dao.where().pnfsId(message.getPnfsId())));
        return message;
    }


}
