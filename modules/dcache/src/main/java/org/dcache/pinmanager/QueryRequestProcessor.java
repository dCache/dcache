package org.dcache.pinmanager;

import org.springframework.beans.factory.annotation.Required;

import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.pinmanager.PinDao.PinCriterion;

import static org.dcache.pinmanager.model.Pin.State.FAILED_TO_UNPIN;
import static org.dcache.pinmanager.model.Pin.State.READY_TO_UNPIN;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;


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
            throws CacheException
    {
        PinCriterion criterion = _dao.where().pnfsId(message.getPnfsId());
        String requestId = message.getRequestId();
        if (requestId != null) {
            criterion.requestId(message.getRequestId());
        }
        message.setCount(_dao.count(criterion
                                            .stateIsNot(UNPINNING)
                                            .stateIsNot(READY_TO_UNPIN)
                                            .stateIsNot(FAILED_TO_UNPIN)));
        return message;
    }
}
