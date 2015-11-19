package org.dcache.pinmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import javax.jdo.JDOException;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.vehicles.PnfsDeleteEntryNotificationMessage;

import dmg.cells.nucleus.CellMessageReceiver;

import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.util.FireAndForgetTask;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PinManager
    implements CellMessageReceiver
{
    private static final Logger _log =
        LoggerFactory.getLogger(PinManager.class);
    private static final long INITIAL_EXPIRATION_DELAY = SECONDS.toMillis(15);
    private static final long INITIAL_UNPIN_DELAY = SECONDS.toMillis(30);

    private ScheduledExecutorService _executor;
    private PinDao _dao;
    private CellStub _poolStub;
    private long _expirationPeriod;
    private TimeUnit _expirationPeriodUnit;
    private PoolMonitor _poolMonitor;

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    @Required
    public void setDao(PinDao dao)
    {
        _dao = dao;
    }

    @Required
    public void setPoolStub(CellStub stub)
    {
        _poolStub = stub;
    }

    @Required
    public void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
    }

    @Required
    public void setExpirationPeriod(long period)
    {
        _expirationPeriod = period;
    }

    public long getExpirationPeriod()
    {
        return _expirationPeriod;
    }

    @Required
    public void setExpirationPeriodUnit(TimeUnit unit)
    {
        _expirationPeriodUnit = unit;
    }

    public TimeUnit getExpirationPeriodUnit()
    {
        return _expirationPeriodUnit;
    }

    public void init()
    {
        Runnable expirationTask =
            new FireAndForgetTask(new ExpirationTask());
        _executor.scheduleWithFixedDelay(
                expirationTask,
                INITIAL_EXPIRATION_DELAY,
                _expirationPeriodUnit.toMillis(_expirationPeriod),
                MILLISECONDS);
        Runnable unpinProcessor =
            new FireAndForgetTask(new UnpinProcessor(_dao, _poolStub, _poolMonitor));
        _executor.scheduleWithFixedDelay(
                unpinProcessor,
                INITIAL_UNPIN_DELAY,
                _expirationPeriodUnit.toMillis(_expirationPeriod),
                MILLISECONDS);
    }

    public PnfsDeleteEntryNotificationMessage messageArrived(PnfsDeleteEntryNotificationMessage message)
    {
        _dao.deletePin(message.getPnfsId());
        return message;
    }

    private class ExpirationTask implements Runnable
    {
        @Override
        public void run()
        {
            try {
                _dao.expirePins();
            } catch (JDOException | DataAccessException e) {
                _log.error("Database failure while expiring pins: {}",
                           e.getMessage());
            } catch (RuntimeException e) {
                _log.error("Unexpected failure while expiring pins", e);
            }
        }
    }
}
