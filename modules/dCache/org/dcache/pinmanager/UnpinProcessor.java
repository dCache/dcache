package org.dcache.pinmanager;

import javax.jdo.JDOException;
import java.util.concurrent.Semaphore;

import org.dcache.cells.CellStub;
import org.dcache.cells.AbstractMessageCallback;
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

    private final static int MAX_RUNNING = 1000;

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
            Semaphore idle = new Semaphore(MAX_RUNNING);
            Semaphore finished = new Semaphore(0);
            int running = unpin(idle, finished);
            finished.acquire(running);
        } catch (InterruptedException e) {
            _logger.debug(e.toString());
        } catch (JDOException e) {
            _logger.error("Database failure while unpinning: {}",
                          e.getMessage());
        } catch (RuntimeException e) {
            _logger.error("Unexpected failure while unpinning", e);
        }
    }

    @Transactional
    protected int unpin(Semaphore idle, Semaphore finished)
        throws InterruptedException
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

            clearStickyFlag(idle, finished, pin);
            running++;
        }
        return running;
    }

    private void clearStickyFlag(final Semaphore idle,
                                 final Semaphore finished, final Pin pin)
        throws InterruptedException
    {
        idle.acquire();
        PoolSetStickyMessage msg =
            new PoolSetStickyMessage(pin.getPool(),
                                     pin.getPnfsId(),
                                     false,
                                     pin.getSticky(),
                                     0);
        _poolStub.send(new CellPath(pin.getPool()), msg,
                       PoolSetStickyMessage.class,
                       new AbstractMessageCallback<PoolSetStickyMessage>() {
                           @Override
                           public void success(PoolSetStickyMessage msg)
                           {
                               idle.release();
                               finished.release();
                               _dao.deletePin(pin);
                           }

                           @Override
                           public void failure(int rc, Object error)
                           {
                               idle.release();
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
                       });
    }
}