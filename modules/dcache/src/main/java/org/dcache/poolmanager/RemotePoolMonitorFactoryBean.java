package org.dcache.poolmanager;

import org.springframework.aop.target.dynamic.Refreshable;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;

import java.lang.reflect.Proxy;

import org.dcache.cells.CellStub;

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
                new Class[] { PoolMonitor.class, Refreshable.class }, handler);
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

    @PostConstruct
    public void init() throws InterruptedException
    {
        handler.init();
    }
}
