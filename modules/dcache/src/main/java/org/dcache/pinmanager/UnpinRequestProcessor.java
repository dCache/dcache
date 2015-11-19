package org.dcache.pinmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;

import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.pinmanager.model.Pin;

/**
 * Processes unpin requests.
 *
 * When an unpin request is received a pin is put into state
 * UNPINNING. The actual work to unpin a file is performed
 * by the UnpinProcessor.
 */
public class UnpinRequestProcessor
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(UnpinRequestProcessor.class);

    private PinDao _dao;
    private AuthorizationPolicy _pdp;

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setAuthorizationPolicy(AuthorizationPolicy pdp)
    {
        _pdp = pdp;
    }

    @Transactional
    public PinManagerUnpinMessage messageArrived(PinManagerUnpinMessage message)
        throws CacheException
    {
        PnfsId pnfsId = message.getPnfsId();
        if (message.getPinId() != null) {
            unpin(message, _dao.getPin(pnfsId, message.getPinId()));
        } else if (message.getRequestId() != null) {
            unpin(message, _dao.getPin(pnfsId, message.getRequestId()));
        } else {
            for (Pin pin: _dao.getPins(pnfsId)) {
                if (_pdp.canUnpin(message.getSubject(), pin)) {
                    pin.setState(Pin.State.UNPINNING);
                }
                _log.info("Unpinned {} ({})", pin.getPnfsId(), pin.getPinId());
                _dao.storePin(pin);
            }
        }
        return message;
    }

    private void unpin(PinManagerUnpinMessage message, Pin pin)
        throws CacheException
    {
        if (pin != null) {
            if (!_pdp.canUnpin(message.getSubject(), pin)) {
                throw new PermissionDeniedCacheException("Access denied");
            }
            pin.setState(Pin.State.UNPINNING);
            pin = _dao.storePin(pin);

            message.setPinId(pin.getPinId());
            message.setRequestId(pin.getRequestId());

            _log.info("Unpinned {} ({})", pin.getPnfsId(), pin.getPinId());
        }
    }
}
