package org.dcache.pinmanager;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static java.util.concurrent.TimeUnit.*;

import org.dcache.cells.CellStub;
import org.dcache.cells.CellMessageReceiver;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolRemoveFilesMessage;

import org.springframework.beans.factory.annotation.Required;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinManager
    implements CellMessageReceiver
{
    private final static Logger _log =
        LoggerFactory.getLogger(PinManager.class);
    private final static long INITIAL_EXPIRATION_DELAY = SECONDS.toMillis(15);
    private final static long INITIAL_UNPIN_DELAY = SECONDS.toMillis(30);

    private ScheduledExecutorService _executor;
    private PinDao _dao;
    private CellStub _poolStub;
    private long _expirationPeriod;

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
    public void setExpirationPeriod(long period)
    {
        _expirationPeriod = period;
    }

    public long getExpirationPeriod()
    {
        return _expirationPeriod;
    }

    public void init()
    {
        _executor.scheduleWithFixedDelay(new ExpirationTask(),
                                         INITIAL_EXPIRATION_DELAY,
                                         _expirationPeriod,
                                         TimeUnit.MILLISECONDS);
        _executor.scheduleWithFixedDelay(new UnpinProcessor(_dao, _poolStub),
                                         INITIAL_UNPIN_DELAY,
                                         _expirationPeriod,
                                         TimeUnit.MILLISECONDS);
    }

    public void messageArrived(PoolRemoveFilesMessage message)
    {
        _dao.deletePins(message.getFiles());
    }

    private class ExpirationTask implements Runnable
    {
        public void run()
        {
            try {
                _dao.expirePins();
            } catch (RuntimeException e) {
                _log.error(e.toString());
            }
        }
    }
}