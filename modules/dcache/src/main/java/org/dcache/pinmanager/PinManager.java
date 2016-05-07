package org.dcache.pinmanager;

import com.google.common.base.Throwables;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import javax.jdo.JDOException;

import java.util.Date;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.FireAndForgetTask;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.dcache.pinmanager.model.Pin.State.UNPINNING;

public class PinManager
    implements CellMessageReceiver, CuratorFrameworkAware, CellIdentityAware, CellLifeCycleAware
{
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
    private CuratorFramework client;
    private CellAddressCore address;
    private LeaderLatch leaderLatch;
    private String zkPath;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
    }

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        this.executor = executor;
    }

    @Required
    public void setDao(PinDao dao)
    {
        this.dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        poolStub = stub;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        this.poolMonitor = poolMonitor;
    }

    @Required
    public void setExpirationPeriod(long period)
    {
        expirationPeriod = period;
    }

    public long getExpirationPeriod()
    {
        return expirationPeriod;
    }

    @Required
    public void setExpirationPeriodUnit(TimeUnit unit)
    {
        expirationPeriodUnit = unit;
    }

    public TimeUnit getExpirationPeriodUnit()
    {
        return expirationPeriodUnit;
    }

    @Required
    public void setServiceName(String serviceName)
    {
        zkPath = getZooKeeperLeaderPath(serviceName);
    }

    @Override
    public void afterStart()
    {
        try {
            leaderLatch = new LeaderLatch(client, zkPath, address.toString());
            leaderLatch.addListener(new LeaderListener());
            leaderLatch.start();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void beforeStop()
    {
        if (leaderLatch != null) {
            CloseableUtils.closeQuietly(leaderLatch);
        }
    }

    public PnfsDeleteEntryNotificationMessage messageArrived(PnfsDeleteEntryNotificationMessage message)
    {
        dao.delete(dao.where().pnfsId(message.getPnfsId()));
        return message;
    }

    public static String getZooKeeperLeaderPath(String serviceName)
    {
        return ZKPaths.makePath("/dcache/pinmanager", serviceName, "leader");
    }

    private class ExpirationTask implements Runnable
    {
        @Override
        public void run()
        {
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
            }
        }
    }

    private class LeaderListener implements LeaderLatchListener
    {
        private final FireAndForgetTask unpinTask =
                new FireAndForgetTask(new UnpinProcessor(dao, poolStub, poolMonitor));
        private final ExpirationTask expirationTask =
                new ExpirationTask();
        private ScheduledFuture<?> unpinFuture;
        private ScheduledFuture<?> expirationFuture;

        @Override
        public void isLeader()
        {
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
        public void notLeader()
        {
            expirationFuture.cancel(false);
            unpinFuture.cancel(true);
        }
    }
}
