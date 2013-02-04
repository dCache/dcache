package org.dcache.poolmanager;

import org.dcache.cells.CellStub;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import java.lang.reflect.Proxy;
import java.util.concurrent.ScheduledExecutorService;

/**
 * FactoryBean for PoolMonitor Proxy instances that provide access to
 * cached PoolMonitors periodically imported from pool manager.
 *
 * @see org.dcache.poolmanager.RemotePoolMonitorInvocationHandler
 */
public class RemotePoolMonitorFactoryBean implements FactoryBean<PoolMonitor>
{
    private final RemotePoolMonitorInvocationHandler handler =
            new RemotePoolMonitorInvocationHandler();

    @Override
    public PoolMonitor getObject() throws Exception
    {
        return (PoolMonitor) Proxy.newProxyInstance(PoolMonitor.class.getClassLoader(),
                new Class[] { PoolMonitor.class}, handler);
    }

    @Override
    public Class<?> getObjectType()
    {
        return PoolMonitor.class;
    }

    @Override
    public boolean isSingleton()
    {
        return true;
    }

    @Required
    public void setPoolManagerStub(CellStub stub)
    {
        handler.setPoolManagerStub(stub);
    }

    @Required
    public void setExecutor(
            ScheduledExecutorService executor)
    {
        handler.setExecutor(executor);
    }

    public long getRefreshDelay()
    {
        return handler.getRefreshDelay();
    }

    /**
     * Sets the delay in milliseconds between two consecutive refreshes
     * of the cached PoolMonitor.
     */
    public void setRefreshDelay(long refreshDelay)
    {
        handler.setRefreshDelay(refreshDelay);
    }

    public void init() throws InterruptedException
    {
        handler.init();
    }
}
