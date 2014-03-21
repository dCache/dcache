package org.dcache.poolmanager;

import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.target.dynamic.Refreshable;
import org.springframework.beans.factory.annotation.Required;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.util.FireAndForgetTask;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * InvocationHandler to access a cached PoolMonitor that is periodically imported from pool manager.
 */
public class RemotePoolMonitorInvocationHandler implements InvocationHandler, Refreshable
{
    private static final Logger _log = LoggerFactory
            .getLogger(RemotePoolMonitorInvocationHandler.class);

    /**
     * The delay we use after transient failures during initialization.
     * Adding a small delay prevents tight retry loops.
     */
    private static final long INIT_DELAY = MILLISECONDS.toMillis(100);

    private long _refreshDelay = SECONDS.toMillis(20);
    private long _refreshCount;
    private long _lastRefreshTime;
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
        _refreshCount++;
        _lastRefreshTime = System.currentTimeMillis();
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

    @Override
    public synchronized void refresh()
    {
        _poolMonitor = null;
        new UpdatePoolMonitorTask().run();
    }

    @Override
    public synchronized long getRefreshCount()
    {
        return _refreshCount;
    }

    @Override
    public synchronized long getLastRefreshTime()
    {
        return _lastRefreshTime;
    }

    public void init()
    {
        _executor.submit(new FireAndForgetTask(new Runnable()
        {
            private static final double ERRORS_PER_SECOND = 1.0 / 60.0;
            private final RateLimiter rate = RateLimiter.create(ERRORS_PER_SECOND);

            @Override
            public void run()
            {
                try {
                    setPoolMonitor(_poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor());
                    _executor.scheduleWithFixedDelay(
                            new UpdatePoolMonitorTask(), _refreshDelay / 2, _refreshDelay, TimeUnit.MILLISECONDS);
                } catch (CacheException e) {
                    if (rate.tryAcquire()) {
                        _log.error(e.toString());
                    }
                    _executor.schedule(this, INIT_DELAY, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ignored) {
                }
            }
        }));
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
            throws Throwable
    {
        if (method.getDeclaringClass() == Refreshable.class) {
            return method.invoke(this, args);
        }
        return method.invoke(getPoolMonitor(), args);
    }

    private class UpdatePoolMonitorTask
            extends AbstractMessageCallback<PoolManagerGetPoolMonitor>
            implements Runnable
    {
        @Override
        public void run()
        {
            CellStub.addCallback(_poolManagerStub.send(new PoolManagerGetPoolMonitor()), this,
                                 MoreExecutors.sameThreadExecutor());
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
