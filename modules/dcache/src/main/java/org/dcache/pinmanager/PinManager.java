package org.dcache.pinmanager;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;

import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jdo.JDOException;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.FireAndForgetTask;
import org.dcache.util.NDC;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

public class PinManager
      implements CellMessageReceiver, LeaderLatchListener {

    private static final Logger _log =
          LoggerFactory.getLogger(PinManager.class);
    private static final long INITIAL_EXPIRATION_DELAY = SECONDS.toMillis(15);
    private static final long INITIAL_UNPIN_DELAY = SECONDS.toMillis(30);

    private ScheduledExecutorService executor;
    private PinDao dao;
    private CellStub poolStub;
    private long expirationPeriod;
    private TimeUnit expirationPeriodUnit;
    private PoolMonitor poolMonitor;

    @Required
    public void setExecutor(ScheduledExecutorService executor) {
        this.executor = executor;
    }

    @Required
    public void setDao(PinDao dao) {
        this.dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub) {
        poolStub = stub;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor) {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setExpirationPeriod(long period) {
        expirationPeriod = period;
    }

    public long getExpirationPeriod() {
        return expirationPeriod;
    }

    @Required
    public void setExpirationPeriodUnit(TimeUnit unit) {
        expirationPeriodUnit = unit;
    }

    public TimeUnit getExpirationPeriodUnit() {
        return expirationPeriodUnit;
    }

    public PnfsDeleteEntryNotificationMessage messageArrived(
          PnfsDeleteEntryNotificationMessage message) {
        dao.delete(dao.where().pnfsId(message.getPnfsId()));
        return message;
    }

    private class ExpirationTask implements Runnable {

        private AtomicInteger count = new AtomicInteger();

        @Override
        public void run() {
            NDC.push("BackgroundExpiration-" + count.incrementAndGet());
            try {
                dao.update(dao.where()
                            .expirationTimeBefore(new Date())
                            .stateIsNot(UNPINNING),
                      dao.set().
                            state(UNPINNING));
            } catch (JDOException | DataAccessException e) {
                _log.error("Database failure while expiring pins: {}",
                      e.getMessage());
            } catch (RuntimeException e) {
                _log.error("Unexpected failure while expiring pins", e);
            } finally {
                NDC.pop();
            }
        }
    }

    private FireAndForgetTask unpinTask;
    private final ExpirationTask expirationTask = new ExpirationTask();
    private ScheduledFuture<?> unpinFuture;
    private ScheduledFuture<?> expirationFuture;

    public void init() {
        // Needs to be assigned after dao has been initialized
        unpinTask = new FireAndForgetTask(new UnpinProcessor(dao, poolStub, poolMonitor));
    }

    @Override
    public void isLeader() {
        _log.info("Scheduling Expiration and Unpin tasks.");
        expirationFuture = executor.scheduleWithFixedDelay(
              new FireAndForgetTask(expirationTask),
              INITIAL_EXPIRATION_DELAY,
              expirationPeriodUnit.toMillis(expirationPeriod),
              MILLISECONDS);
        unpinFuture = executor.scheduleWithFixedDelay(
              unpinTask,
              INITIAL_UNPIN_DELAY,
              expirationPeriodUnit.toMillis(expirationPeriod),
              MILLISECONDS);
    }

    @Override
    public void notLeader() {
        _log.info("Cancelling Expiration and Unpin tasks.");
        expirationFuture.cancel(false);
        unpinFuture.cancel(true);
    }
}