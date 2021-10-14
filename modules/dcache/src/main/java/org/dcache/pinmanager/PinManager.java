package org.dcache.pinmanager;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.pinmanager.model.Pin.State.FAILED_TO_UNPIN;
import static org.dcache.pinmanager.model.Pin.State.PINNED;
import static org.dcache.pinmanager.model.Pin.State.PINNING;
import static org.dcache.pinmanager.model.Pin.State.READY_TO_UNPIN;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;

import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;
import dmg.cells.nucleus.CellInfoProvider;
import dmg.cells.nucleus.CellMessageReceiver;
import java.io.PrintWriter;
import java.time.Duration;
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
import org.dcache.util.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

public class PinManager implements CellMessageReceiver, LeaderLatchListener, CellInfoProvider {

    private static final Logger _log = LoggerFactory.getLogger(PinManager.class);
    private static final long INITIAL_EXPIRATION_DELAY = SECONDS.toMillis(15);
    private static final long INITIAL_UNPIN_DELAY = SECONDS.toMillis(30);

    private ScheduledExecutorService executor;
    private PinDao dao;
    private CellStub poolStub;
    private PoolMonitor poolMonitor;

    private long expirationPeriod;
    private TimeUnit expirationPeriodUnit;
    // Period in which to reset all pins that failed to be unpinned from state FAILED_TO_UNPIN to READY_TO_UNPIN
    private Duration resetFailedUnpinsPeriod;
    private int maxUnpinsPerRun = -1;

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

    @Required
    public void setExpirationPeriodUnit(TimeUnit unit) {
        expirationPeriodUnit = unit;
    }

    public void setResetFailedUnpinsPeriod(Duration period) {
        resetFailedUnpinsPeriod = period;
    }

    public void setMaxUnpinsPerRun(int value) {
        checkArgument(value > 0 || value == -1,
              "The number of unpins per run should either be -1 or greater than 0.");
        maxUnpinsPerRun = value;
    }

    public PnfsDeleteEntryNotificationMessage messageArrived(
          PnfsDeleteEntryNotificationMessage message) {
        dao.delete(dao.where().pnfsId(message.getPnfsId()));
        return message;
    }

    /**
     * Resets all pins in state UNPINNING and FAILED_TO_UNPIN to READY_TO_UNPIN.
     */
    private void markAllExpiredPinsReadyToUnpin() {
        dao.update(dao.where()
                    .stateIsNot(PINNING)
                    .stateIsNot(PINNED)
                    .stateIsNot(READY_TO_UNPIN),
              dao.set().
                    state(READY_TO_UNPIN));
    }

    /**
     * This task transitions all pins that have exceeded their lifetime and are in state PINNING or
     * PINNED to state READY_TO_UNPIN.
     */
    private class ExpirationTask implements Runnable {

        private AtomicInteger count = new AtomicInteger();

        @Override
        public void run() {
            NDC.push("BackgroundExpiration-" + count.incrementAndGet());
            try {
                dao.update(dao.where()
                            .expirationTimeBefore(new Date())
                            .stateIsNot(READY_TO_UNPIN)
                            .stateIsNot(UNPINNING)
                            .stateIsNot(FAILED_TO_UNPIN),
                      dao.set().
                            state(READY_TO_UNPIN));
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

    /**
     * This task transitions all pins that are in state FAILED_TO_UNPIN back to READY_TO_UNPIN to
     * allow retrying their unpinning. This task should not be run very frequently.
     */
    private class ResetFailedUnpinsTask implements Runnable {

        private AtomicInteger count = new AtomicInteger();

        @Override
        public void run() {
            NDC.push("BackgroundResetFailedUnpins-" + count.incrementAndGet());
            try {
                dao.update(dao.where()
                            .state(FAILED_TO_UNPIN),
                      dao.set().
                            state(READY_TO_UNPIN));
            } catch (JDOException | DataAccessException e) {
                _log.error(
                      "Database failure while resetting pins that previously failed to be unpinned: {}",
                      e.getMessage());
            } catch (RuntimeException e) {
                _log.error("Unexpected failure while pins that previously failed to be unpinned",
                      e);
            } finally {
                NDC.pop();
            }
        }
    }

    private UnpinProcessor unpinTask;
    private final ExpirationTask expirationTask = new ExpirationTask();
    private final ResetFailedUnpinsTask resetFailedUnpinsTask = new ResetFailedUnpinsTask();

    private ScheduledFuture<?> unpinFuture;
    private ScheduledFuture<?> expirationFuture;
    private ScheduledFuture<?> resetFailedUnpinsFuture;

    public void init() {
        // Needs to be assigned after dao has been initialized
        unpinTask = new UnpinProcessor(dao, poolStub, poolMonitor, maxUnpinsPerRun);
    }

    @Override
    public void isLeader() {
        _log.info("Resetting existing intermediate pin states.");
        markAllExpiredPinsReadyToUnpin();

        _log.info("Scheduling Expiration and Unpin tasks.");
        expirationFuture = executor.scheduleWithFixedDelay(
              new FireAndForgetTask(expirationTask),
              INITIAL_EXPIRATION_DELAY,
              expirationPeriodUnit.toMillis(expirationPeriod),
              MILLISECONDS);
        unpinFuture = executor.scheduleWithFixedDelay(
              new FireAndForgetTask(unpinTask),
              INITIAL_UNPIN_DELAY,
              expirationPeriodUnit.toMillis(expirationPeriod),
              MILLISECONDS);
        resetFailedUnpinsFuture = executor.scheduleWithFixedDelay(
              new FireAndForgetTask(resetFailedUnpinsTask),
              INITIAL_UNPIN_DELAY,
              resetFailedUnpinsPeriod.toMillis(),
              MILLISECONDS);
    }

    @Override
    public void notLeader() {
        _log.info("Cancelling Expiration, ResetFailedUnpins and Unpin tasks.");
        expirationFuture.cancel(false);
        unpinFuture.cancel(true);
        resetFailedUnpinsFuture.cancel(true);
    }

    @Override
    public void getInfo(PrintWriter pw) {
        pw.printf("Expiration and unpin period:            %s %s\n", expirationPeriod,
              expirationPeriodUnit);
        pw.printf("Reset pins that failed to unpin period: %s\n",
              TimeUtils.describe(resetFailedUnpinsPeriod).orElse("-"));
        pw.printf("Max unpin operations per run:           %s\n", maxUnpinsPerRun);
    }
}