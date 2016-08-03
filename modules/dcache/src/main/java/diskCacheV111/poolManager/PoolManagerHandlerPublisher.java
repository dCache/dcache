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
package diskCacheV111.poolManager;

import com.google.common.collect.Sets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.nodes.PersistentNode;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.springframework.beans.factory.annotation.Required;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.DelayedReply;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.zookeeper.PathChildrenCache;

import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.poolmanager.PoolMgrGetHandler;
import org.dcache.poolmanager.PoolMgrGetUpdatedHandler;
import org.dcache.poolmanager.RemotePoolManagerHandler;
import org.dcache.poolmanager.RendezvousPoolManagerHandler;
import org.dcache.poolmanager.SerializablePoolManagerHandler;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.stream.Collectors.toList;

/**
 * This class responds to requests to provide a PoolManagerHandler.
 *
 * Synchronizes with other instances of this class using ZooKeeper to provide a
 * RendezvousPoolManagerHandler that directs requests to the appropriate backend.
 */
public class PoolManagerHandlerPublisher
        implements CellLifeCycleAware, CellIdentityAware, CuratorFrameworkAware, CellMessageReceiver, PathChildrenCacheListener
{
    /**
     * Our cell address.
     */
    private CellAddressCore address;

    /**
     * Common service name of the backends.
     */
    private String serviceName;

    /**
     * Our znode announcing our availability.
     */
    private PersistentNode node;

    /**
     * A cache of znodes tracking available backends.
     */
    private PathChildrenCache cache;

    /**
     * Interface to ZooKeeper.
     */
    private CuratorFramework client;

    /**
     * Current pool manager handler given to services that ask for one.
     */
    private volatile SerializablePoolManagerHandler handler;

    /**
     * Tracks blocked update requests. If the list of backends changes, these requests
     * are processed.
     */
    private final Set<UpdateRequest> requests = Sets.newConcurrentHashSet();

    /**
     * Tracks expiration of update requests. Requests are dropped lazily as new
     * requests are added.
     */
    private final DelayQueue<UpdateRequest> delays = new DelayQueue<>();

    @Override
    public void setCellAddress(CellAddressCore address)
    {
        this.address = address;
    }

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
    }

    @Required
    public void setServiceName(String serviceName)
    {
        this.serviceName = serviceName;
    }

    @PostConstruct
    public void start() throws Exception
    {
        handler = new RemotePoolManagerHandler(new CellAddressCore(serviceName));
        cache = new PathChildrenCache(client, getZooKeeperPath(), true);
        cache.getListenable().addListener(this);
        cache.start();
    }

    @Override
    public void afterStart()
    {
        String path = ZKPaths.makePath(getZooKeeperPath(), address.toString());
        byte[] data =  address.toString().getBytes(US_ASCII);
        node = new PersistentNode(client, CreateMode.EPHEMERAL, false, path, data);
        node.start();
    }

    @Override
    public void beforeStop()
    {
        if (node != null) {
            CloseableUtils.closeQuietly(node);
        }
        requests.stream().filter(requests::remove).forEach(UpdateRequest::shutdown);
    }

    @PreDestroy
    public void stop() throws IOException
    {
        if (cache != null) {
            cache.close();
        }
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception
    {
        switch (event.getType()) {
        case CHILD_ADDED:
        case CHILD_UPDATED:
        case CHILD_REMOVED:
            rebuildHandler();
            break;
        default:
            break;
        }
    }

    private void rebuildHandler()
    {
        List<CellAddressCore> backends =
                cache.getCurrentData().stream()
                        .map(ChildData::getPath)
                        .map(ZKPaths::getNodeFromPath)
                        .map(CellAddressCore::new)
                        .collect(toList());
        SerializablePoolManagerHandler handler;
        if (backends.isEmpty()) {
            handler = new RemotePoolManagerHandler(new CellAddressCore(serviceName));
        } else if (backends.size() == 1) {
            handler = new RemotePoolManagerHandler(backends.get(0));
        } else {
            handler = new RendezvousPoolManagerHandler(new CellAddressCore(serviceName), backends);
        }
        this.handler = handler;
        requests.stream().filter(requests::remove).forEach(r -> r.send(handler));
    }

    public PoolMgrGetHandler messageArrived(PoolMgrGetHandler message)
    {
        message.setHandler(handler);
        return message;
    }

    public DelayedReply messageArrived(CellMessage envelope, PoolMgrGetUpdatedHandler message)
    {
        SerializablePoolManagerHandler handler = this.handler;
        UpdateRequest request = new UpdateRequest(envelope, message);
        requests.add(request);
        if (message.getVersion().equals(handler.getVersion())) {
            delays.put(request);
            UpdateRequest expired;
            while ((expired = delays.poll()) != null) {
                requests.remove(expired);
            }
        } else if (requests.remove(request)) {
            request.send(handler);
        }
        return request;
    }

    public String getZooKeeperPath()
    {
        return ZKPaths.makePath("/dcache/poolmanager", serviceName, "backends");
    }

    private static class UpdateRequest extends DelayedReply implements Delayed
    {
        private final PoolMgrGetUpdatedHandler message;

        private final CellMessage envelope;

        public UpdateRequest(CellMessage envelope, PoolMgrGetUpdatedHandler message)
        {
            this.envelope = envelope;
            this.message = message;
        }

        public void send(SerializablePoolManagerHandler handler)
        {
            message.setHandler(handler);
            reply(message);
        }

        public void shutdown()
        {
            reply(new NoRouteToCellException(envelope, "Pool manager is shutting down."));
        }

        @Override
        public long getDelay(TimeUnit unit)
        {
            return unit.convert(envelope.getTtl() - envelope.getLocalAge(), TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed o)
        {
            return Long.compare(getDelay(TimeUnit.MILLISECONDS), o.getDelay(TimeUnit.MILLISECONDS));
        }
    }
}
