package org.dcache.pinmanager;

import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolSetStickyMessage;
import dmg.cells.nucleus.CellPath;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jdo.JDOException;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.pinmanager.model.Pin;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.CDCExecutorServiceDecorator;
import org.dcache.util.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.remoting.RemoteConnectFailureException;
import org.springframework.transaction.annotation.Transactional;

/**
 * Performs the work of unpinning files.
 * <p>
 * When an unpin request is received a pin is put into state UNPINNING. The actual work to unpin a
 * file is performed independently of the unpin request.
 */
public class UnpinProcessor implements Runnable {

    private static final Logger _logger =
          LoggerFactory.getLogger(UnpinProcessor.class);

    private static final int MAX_RUNNING = 1000;

    private final PinDao _dao;
    private final CellStub _poolStub;
    private final PoolMonitor _poolMonitor;
    private final AtomicInteger _count = new AtomicInteger();

    public UnpinProcessor(PinDao dao, CellStub poolStub,
          PoolMonitor poolMonitor) {
        _dao = dao;
        _poolStub = poolStub;
        _poolMonitor = poolMonitor;
    }

    @Override
    public void run() {
        final ExecutorService executor = new CDCExecutorServiceDecorator(
              Executors.newSingleThreadExecutor());
        NDC.push("BackgroundUnpinner-" + _count.incrementAndGet());
        try {
            Semaphore idle = new Semaphore(MAX_RUNNING);
            unpin(idle, executor);
            idle.acquire(MAX_RUNNING);
        } catch (InterruptedException e) {
            _logger.debug(e.toString());
        } catch (JDOException | DataAccessException e) {
            _logger.error("Database failure while unpinning: {}",
                  e.getMessage());
        } catch (RemoteConnectFailureException e) {
            _logger.error("Remote connection failure while unpinning: {}", e.getMessage());
        } catch (RuntimeException e) {
            _logger.error("Unexpected failure while unpinning", e);
        } finally {
            executor.shutdown();
            NDC.pop();
        }
    }

    @Transactional
    protected void unpin(final Semaphore idle, final Executor executor)
          throws InterruptedException {
        _dao.foreach(_dao.where().state(Pin.State.UNPINNING), pin -> upin(idle, executor, pin));
    }

    private void upin(Semaphore idle, Executor executor, Pin pin) throws InterruptedException {
        if (pin.getPool() == null) {
            _logger.debug("No pool found for pin {}, pnfsid {}; no sticky flags to clear",
                  pin.getPinId(), pin.getPnfsId());
            _dao.delete(pin);
        } else {
            _logger.debug("Clearing sticky flag for pin {}, pnfsid {} on pool {}", pin.getPinId(),
                  pin.getPnfsId(), pin.getPool());
            clearStickyFlag(idle, pin, executor);
        }
    }

    private void clearStickyFlag(final Semaphore idle, final Pin pin, Executor executor)
          throws InterruptedException {
        PoolSelectionUnit.SelectionPool pool = _poolMonitor.getPoolSelectionUnit()
              .getPool(pin.getPool());
        if (pool == null || !pool.isActive()) {
            _logger.warn(
                  "Unable to clear sticky flag for pin {} on pnfsid {} because pool {} is unavailable",
                  pin.getPinId(), pin.getPnfsId(), pin.getPool());
            return;
        }

        idle.acquire();
        PoolSetStickyMessage msg =
              new PoolSetStickyMessage(pin.getPool(),
                    pin.getPnfsId(),
                    false,
                    pin.getSticky(),
                    0);
        CellStub.addCallback(_poolStub.send(new CellPath(pool.getAddress()), msg),
              new AbstractMessageCallback<PoolSetStickyMessage>() {
                  @Override
                  public void success(PoolSetStickyMessage msg) {
                      idle.release();
                      _dao.delete(pin);
                  }

                  @Override
                  public void failure(int rc, Object error) {
                      idle.release();
                      switch (rc) {
                          case CacheException.FILE_NOT_IN_REPOSITORY:
                              _dao.delete(pin);
                              break;
                          default:
                              _logger.warn("Failed to clear sticky flag: {} [{}]", error, rc);
                              break;
                      }
                  }
              }, executor);
    }
}
