package org.dcache.pinmanager;

import diskCacheV111.util.CacheException;
import diskCacheV111.util.PermissionDeniedCacheException;
import diskCacheV111.util.PnfsId;
import dmg.cells.nucleus.CellMessageReceiver;
import org.dcache.pinmanager.model.Pin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.transaction.annotation.Transactional;

/**
 * Processes unpin requests.
 * <p>
 * When an unpin request is received a pin is put into state READY_TO_UNPIN. The actual work to
 * unpin a file is performed by the UnpinProcessor.
 */
public class UnpinRequestProcessor
      implements CellMessageReceiver {

    private static final Logger LOGGER =
          LoggerFactory.getLogger(UnpinRequestProcessor.class);

    private PinDao _dao;
    private AuthorizationPolicy _pdp;

    @Required
    public void setDao(PinDao dao) {
        _dao = dao;
    }

    @Required
    public void setAuthorizationPolicy(AuthorizationPolicy pdp) {
        _pdp = pdp;
    }

    @Transactional
    public PinManagerUnpinMessage messageArrived(PinManagerUnpinMessage message)
          throws CacheException {
        PnfsId pnfsId = message.getPnfsId();
        if (message.getPinId() != null) {
            unpin(message, _dao.get(_dao.where().pnfsId(pnfsId).id(message.getPinId())));
        } else if (message.getRequestId() != null) {
            unpin(message, _dao.get(_dao.where().pnfsId(pnfsId).requestId(message.getRequestId())));
        } else {
            for (Pin pin : _dao.get(_dao.where().pnfsId(pnfsId))) {
                if (_pdp.canUnpin(message.getSubject(), pin)) {
                    _dao.update(pin, _dao.set().state(Pin.State.READY_TO_UNPIN));
                }
            }
        }
        return message;
    }

    private void unpin(PinManagerUnpinMessage message, Pin pin)
          throws CacheException {
        if (pin != null) {
            if (!_pdp.canUnpin(message.getSubject(), pin)) {
                throw new PermissionDeniedCacheException("Access denied");
            }
            pin = _dao.update(pin, _dao.set().state(Pin.State.READY_TO_UNPIN));
            if (pin != null) {
                message.setPinId(pin.getPinId());
                message.setRequestId(pin.getRequestId());

                LOGGER.info("Unpinned {} ({})", pin.getPnfsId(), pin.getPinId());
            }
        }
    }
}
