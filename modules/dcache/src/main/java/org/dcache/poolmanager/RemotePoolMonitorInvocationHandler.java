package org.dcache.poolmanager;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * InvocationHandler to access a cached PoolMonitor that is periodically imported from pool manager.
 */
public class RemotePoolMonitorInvocationHandler implements InvocationHandler
{
    private final static Logger _log = LoggerFactory
            .getLogger(RemotePoolMonitorInvocationHandler.class);

    /**
     * The delay we use after transient failures during initialization.
     * Adding a small delay prevents tight retry loops.
     */
    private final static long INIT_DELAY = MILLISECONDS.toMillis(10);

    private long _refreshDelay = SECONDS.toMillis(20);
    private CellStub _poolManagerStub;
    private ScheduledExecutorService _executor;

    private PoolMonitor _poolMonitor;

    @Required
    public void setPoolManagerStub(CellStub stub)
    {
        _poolManagerStub = stub;
    }

    @Required
    public void setExecutor(ScheduledExecutorService executor)
    {
        _executor = executor;
    }

    public long getRefreshDelay()
    {
        return _refreshDelay;
    }

    /**
     * Sets the delay in milliseconds between two consecutive refreshes
     * of the cached PoolMonitor.
     */
    public void setRefreshDelay(long refreshDelay)
    {
        _refreshDelay = refreshDelay;
    }

    private synchronized void setPoolMonitor(PoolMonitor poolMonitor)
    {
        _poolMonitor = poolMonitor;
        notify();
    }

    private synchronized PoolMonitor getPoolMonitor()
            throws InterruptedException
    {
        while (_poolMonitor == null) {
            wait();
        }
        return _poolMonitor;
    }

    public void init()
            throws InterruptedException
    {
        initPoolMonitor();
        scheduleUpdateTask();
    }

    private void initPoolMonitor() throws InterruptedException
    {
        while (true) {
            try {
                setPoolMonitor(_poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor());
                break;
            } catch (CacheException e) {
                _log.error(e.toString());
                Thread.sleep(INIT_DELAY);
            }
        }
    }

    private void scheduleUpdateTask()
    {
        _executor.scheduleWithFixedDelay(new UpdatePoolMonitorTask(),
                _refreshDelay / 2, _refreshDelay,
                TimeUnit.MILLISECONDS);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        return method.invoke(getPoolMonitor(), args);
    }

    private class UpdatePoolMonitorTask
            extends AbstractMessageCallback<PoolManagerGetPoolMonitor>
            implements Runnable
    {
        @Override
        public void run()
        {
            _poolManagerStub.send(new PoolManagerGetPoolMonitor(),
                    PoolManagerGetPoolMonitor.class, this);
        }

        @Override
        public void success(PoolManagerGetPoolMonitor message)
        {
            setPoolMonitor(message.getPoolMonitor());
        }

        @Override
        public void failure(int rc, Object error)
        {
            _log.warn("Failed to update pool monitor: {} [{}]", error, rc);
        }
    }
}
