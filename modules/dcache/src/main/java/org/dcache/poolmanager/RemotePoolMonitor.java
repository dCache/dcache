/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.dcache.poolmanager;

import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.remoting.RemoteConnectFailureException;
import org.springframework.remoting.RemoteProxyFailureException;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.util.CacheException;
import diskCacheV111.util.FileLocality;
import diskCacheV111.vehicles.PoolManagerGetPoolMonitor;
import diskCacheV111.vehicles.ProtocolInfo;

import dmg.cells.nucleus.CellEvent;
import dmg.cells.nucleus.CellEventListener;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.CellRoute;

import org.dcache.cells.AbstractMessageCallback;
import org.dcache.cells.CellStub;
import org.dcache.vehicles.FileAttributes;

import static org.dcache.util.MathUtils.addWithInfinity;
import static org.dcache.util.MathUtils.subWithInfinity;

/**
 * PoolMonitor that delegates to a PoolMonitor obtained from pool manager.
 */
public class RemotePoolMonitor
        implements PoolMonitor, CellLifeCycleAware, CellMessageReceiver, CellEventListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger(RemotePoolMonitor.class);

    private enum LastEvent
    {
        NONE, REQUEST_SUBMITTED, ROUTE_ADDED, NOROUTE
    }

    private long lastRefreshTime;
    private CellStub poolManagerStub;
    private PoolMonitor poolMonitor;
    private long refreshCount;
    private LastEvent lastEvent = LastEvent.NONE;

    @Required
    public void setPoolManagerStub(CellStub stub)
    {
        poolManagerStub = stub;
    }

    @Override
    public synchronized void routeAdded(CellEvent ce)
    {
        if (((CellRoute) ce.getSource()).getRouteType() == CellRoute.DOMAIN) {
            if (lastEvent == LastEvent.NOROUTE) {
                afterStart();
            } else {
                lastEvent = LastEvent.ROUTE_ADDED;
            }
        }
    }

    @Override
    public void cellCreated(CellEvent ce)
    {
    }

    @Override
    public void cellDied(CellEvent ce)
    {
    }

    @Override
    public void routeDeleted(CellEvent ce)
    {
    }

    @Override
    public synchronized void afterStart()
    {
        lastEvent = LastEvent.REQUEST_SUBMITTED;
        CellStub.addCallback(poolManagerStub.send(new PoolManagerGetPoolMonitor()),
                             new AbstractMessageCallback<PoolManagerGetPoolMonitor>()
                             {
                                 @Override
                                 public void success(PoolManagerGetPoolMonitor message)
                                 {
                                     messageArrived(message.getPoolMonitor());
                                 }

                                 @Override
                                 public void timeout(String message)
                                 {
                                     synchronized (RemotePoolMonitor.this) {
                                         if (lastEvent == LastEvent.ROUTE_ADDED) {
                                             afterStart();
                                         } else {
                                             lastEvent = LastEvent.NOROUTE;
                                         }
                                     }
                                 }

                                 @Override
                                 public void failure(int rc, Object error)
                                 {
                                 }
                             },
                             MoreExecutors.directExecutor());
    }

    @Override
    public void beforeStop()
    {
    }

    @Override
    public PoolSelectionUnit getPoolSelectionUnit()
    {
        return getPoolMonitor().getPoolSelectionUnit();
    }

    @Override
    public CostModule getCostModule()
    {
        return getPoolMonitor().getCostModule();
    }

    @Override
    public PartitionManager getPartitionManager()
    {
        return getPoolMonitor().getPartitionManager();
    }

    @Override
    public PoolSelector getPoolSelector(FileAttributes fileAttributes, ProtocolInfo protocolInfo, String linkGroup)
    {
        return getPoolMonitor().getPoolSelector(fileAttributes, protocolInfo, linkGroup);
    }

    @Override
    public Collection<PoolCostInfo> queryPoolsByLinkName(String linkName)
    {
        return getPoolMonitor().queryPoolsByLinkName(linkName);
    }

    @Override
    public FileLocality getFileLocality(FileAttributes attributes, String hostName)
    {
        return getPoolMonitor().getFileLocality(attributes, hostName);
    }

    public void refresh() throws CacheException, InterruptedException
    {
        messageArrived(poolManagerStub.sendAndWait(new PoolManagerGetPoolMonitor()).getPoolMonitor());
    }

    public synchronized long getRefreshCount()
    {
        return refreshCount;
    }

    public synchronized long getLastRefreshTime()
    {
        return lastRefreshTime;
    }

    public synchronized void messageArrived(SerializablePoolMonitor monitor)
    {
        poolMonitor = monitor;
        lastRefreshTime = System.currentTimeMillis();
        refreshCount++;
        notifyAll();
    }

    private synchronized PoolMonitor getPoolMonitor()
    {
        try {
            if (poolMonitor == null) {
                long deadline = addWithInfinity(System.currentTimeMillis(), poolManagerStub.getTimeoutInMillis());
                do {
                    wait(subWithInfinity(deadline, System.currentTimeMillis()));
                } while (poolMonitor == null || deadline <= System.currentTimeMillis());
                if (poolMonitor == null) {
                    throw new RemoteConnectFailureException("Cached pool information is not yet available.", null);
                }
            }
            if (lastRefreshTime < System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5)) {
                LOGGER.warn("Cached pool information is older than 5 minutes. Please check pool manager.");
            }
            return poolMonitor;
        } catch (InterruptedException e) {
            throw new RemoteProxyFailureException("Failed to fetch pool monitor: " + e.getMessage(), e);
        }
    }
}
