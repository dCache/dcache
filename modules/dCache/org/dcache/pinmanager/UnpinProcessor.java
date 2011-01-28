package org.dcache.pinmanager;

import java.util.concurrent.Semaphore;

import org.dcache.cells.CellStub;
import org.dcache.cells.MessageCallback;
import org.dcache.pinmanager.model.Pin;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import diskCacheV111.util.CacheException;

import dmg.cells.nucleus.CellPath;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.transaction.annotation.Transactional;

/**
 * Performs the work of unpinning files.
 *
 * When an unpin request is received a pin is put into state
 * UNPINNING. The actual work to unpin a file is performed
 * independently of the unpin request.
 */
public class UnpinProcessor implements Runnable
{
    private final static Logger _logger =
        LoggerFactory.getLogger(UnpinProcessor.class);

    private final PinDao _dao;
    private final CellStub _poolStub;

    public UnpinProcessor(PinDao dao, CellStub poolStub)
    {
        _dao = dao;
        _poolStub = poolStub;
    }

    public void run()
    {
        try {
            Semaphore finished = new Semaphore(0);
            int running = unpin(finished);
            finished.acquire(running);
        } catch (InterruptedException e) {
            _logger.debug(e.toString());
        }
    }

    @Transactional
    protected int unpin(Semaphore finished)
    {
        int running = 0;
        for (Pin pin: _dao.getPins(Pin.State.UNPINNING)) {
            /**
             * For purposes of migrating from the previous PinManager
             * we need to deal with sticky flags shared by multiple
             * pins. We will not remove a sticky flag as long as there
             * are other pins using it.
             */
            if (pin.getPool() == null || _dao.hasSharedSticky(pin)) {
                _dao.deletePin(pin);
                continue;
            }

            clearStickyFlag(finished, pin);
            running++;
        }
        return running;
    }

    private void clearStickyFlag(final Semaphore finished, final Pin pin)
    {
        PoolSetStickyMessage msg =
            new PoolSetStickyMessage(pin.getPool(),
                                     pin.getPnfsId(),
                                     false,
                                     pin.getSticky(),
                                     0);
        _poolStub.send(new CellPath(pin.getPool()), msg,
                       PoolSetStickyMessage.class,
                       new MessageCallback<PoolSetStickyMessage>() {
                           @Override
                           public void success(PoolSetStickyMessage msg)
                           {
                               finished.release();
                               _dao.deletePin(pin);
                           }

                           @Override
                           public void failure(int rc, Object error)
                           {
                               finished.release();
                               switch (rc) {
                               case CacheException.FILE_NOT_IN_REPOSITORY:
                                   _dao.deletePin(pin);
                                   break;
                               default:
                                   _logger.warn("Failed to clear sticky flag: {} [{}]", error, rc);
                                   break;
                               }
                           }

                           @Override
                           public void noroute()
                           {
                               finished.release();
                               _logger.warn("No route to {}", pin.getPool());
                           }

                           @Override
                           public void timeout()
                           {
                               finished.release();
                               _logger.warn("Timeout while clearing sticky flag");
                           }
                       });
    }
}