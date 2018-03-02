/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * CHANGES:
 *
 * - Modified by Gerd Behrmann, May 2016.
 */

package dmg.cells.zookeeper;

import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.listen.ListenerContainer;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.utils.PathUtils;
import org.apache.curator.utils.ThreadUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * <p>A utility that attempts to keep all data from all children of a ZK path locally cached. This class
 * will watch the ZK path, respond to update/create/delete events, pull down the data, etc. You can
 * register a listener that will get notified when changes occur.</p>
 * <p></p>
 * <p><b>IMPORTANT</b> - it's not possible to stay transactionally in sync. Users of this class must
 * be prepared for false-positives and false-negatives. Additionally, always use the version number
 * when updating data to avoid overwriting another process' change.</p>
 * <p>Similar to the Curator recipe of the same name, but simpler and with fewer features. Works around
 * issues in Curator 2.10 (see CURATOR-326 and CURATOR-328 in Curator JIRA). We should probably move
 * back to the upstream version once a fix is released and maybe work with upstream to improve the
 * original recipe further (e.g. non-blocking path creation). </p>
 */
public class PathChildrenCache implements Closeable
{
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final CuratorFramework client;
    private final String path;
    private final boolean cacheData;
    private final boolean dataIsCompressed;
    private final ListenerContainer<PathChildrenCacheListener> listeners = new ListenerContainer<>();
    private final ConcurrentMap<String, ChildData> currentData = new ConcurrentHashMap<>();
    private final AtomicReference<State> state = new AtomicReference<>(State.LATENT);

    private final Watcher childrenWatcher = event -> {
        try {
            refresh(RefreshMode.NORMAL);
        } catch (Exception e) {
            handleException(e);
        }
    };

    private final Watcher dataWatcher = event -> {
        try {
            if (event.getType() == Watcher.Event.EventType.NodeDeleted) {
                remove(event.getPath());
            } else if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
                getDataAndStat(event.getPath());
            } else {
                log.debug("Data watcher ignored {}", event);
            }
        } catch (Exception e) {
            handleException(e);
        }
    };

    private final ConnectionStateListener connectionStateListener =
            (client, newState) -> handleStateChange(newState);

    private enum State
    {
        LATENT,
        STARTED,
        CLOSED
    }

    private enum RefreshMode
    {
        NORMAL,
        REBUILD
    }

    /**
     * @param client        the client
     * @param path          path to watch
     * @param cacheData     if true, node contents are cached in addition to the stat
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData)
    {
        this(client, path, cacheData, false);
    }

    /**
     * @param client           the client
     * @param path             path to watch
     * @param cacheData        if true, node contents are cached in addition to the stat
     * @param dataIsCompressed if true, data in the path is compressed
     */
    public PathChildrenCache(CuratorFramework client, String path, boolean cacheData, boolean dataIsCompressed)
    {
        this.client = client;
        this.path = PathUtils.validatePath(path);
        this.cacheData = cacheData;
        this.dataIsCompressed = dataIsCompressed;
    }

    /**
     * Start the cache. The cache is not started automatically. You must call this method.
     *
     * @throws Exception errors
     */
    public void start() throws Exception
    {
        Preconditions.checkState(state.compareAndSet(State.LATENT, State.STARTED), "already started");

        client.getConnectionStateListenable().addListener(connectionStateListener);

        refresh(RefreshMode.NORMAL);
    }

    /**
     * Close/end the cache
     *
     * @throws IOException errors
     */
    @Override
    public void close() throws IOException
    {
        if (state.compareAndSet(State.STARTED, State.CLOSED)) {
            client.getConnectionStateListenable().removeListener(connectionStateListener);
            listeners.clear();
            client.clearWatcherReferences(childrenWatcher);
            client.clearWatcherReferences(dataWatcher);
        }
    }

    /**
     * Return the cache listenable
     *
     * @return listenable
     */
    public ListenerContainer<PathChildrenCacheListener> getListenable()
    {
        return listeners;
    }

    /**
     * Return the current data. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. The data is returned in sorted order.
     *
     * @return list of children and data
     */
    public List<ChildData> getCurrentData()
    {
        return Ordering.natural().immutableSortedCopy(currentData.values());
    }

    /**
     * Return the current data for the given path. There are no guarantees of accuracy. This is
     * merely the most recent view of the data. If there is no child with that path, <code>null</code>
     * is returned.
     *
     * @param fullPath full path to the node to check
     * @return data or null
     */
    public ChildData getCurrentData(String fullPath)
    {
        return currentData.get(fullPath);
    }

    void refresh(RefreshMode mode) throws Exception
    {
        final BackgroundCallback callback = (client, event) -> {
            if (PathChildrenCache.this.state.get() != State.CLOSED) {
                if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                    processChildren(event.getChildren(), mode);
                } else if (event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                    ensurePathAndThenRefresh(mode);
                } else if (event.getResultCode() == KeeperException.Code.CONNECTIONLOSS.intValue() ||
                        event.getResultCode() == KeeperException.Code.SESSIONEXPIRED.intValue()) {
                    log.debug("Refresh callback ignored {}", event);
                } else {
                    handleException(KeeperException.create(event.getResultCode()));
                }
            }
        };
        client.getChildren().usingWatcher(childrenWatcher).inBackground(callback).forPath(path);
    }

    void ensurePathAndThenRefresh(RefreshMode mode) throws Exception
    {
        BackgroundCallback callback =
                (client, event) -> {
                    if (event.getResultCode() == KeeperException.Code.OK.intValue() ||
                        event.getResultCode() == KeeperException.Code.NONODE.intValue()) {
                        refresh(mode);
                    }
                };
        client.checkExists().creatingParentContainersIfNeeded().inBackground(callback).forPath(ZKPaths.makePath(path, "ignored"));
    }

    void callListeners(final PathChildrenCacheEvent event)
    {
        listeners.forEach (
                listener -> {
                    try {
                        listener.childEvent(client, event);
                    } catch (Exception e) {
                        handleException(e);
                    }
                    return null;
                });
    }

    void getDataAndStat(String fullPath) throws Exception
    {
        BackgroundCallback callback = (client, event) -> {
            if (event.getResultCode() == KeeperException.Code.OK.intValue()) {
                updateCache(fullPath, event.getStat(), cacheData ? event.getData() : null);
            }
        };

        if (dataIsCompressed && cacheData) {
            client.getData().decompressed().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
        } else {
            client.getData().usingWatcher(dataWatcher).inBackground(callback).forPath(fullPath);
        }
    }

    /**
     * Default behavior is just to log the exception
     *
     * @param e the exception
     */
    protected void handleException(Throwable e)
    {
        if (e instanceof RuntimeException) {
            log.error("", e);
        } else {
            log.error(e.getMessage());
        }
        ThreadUtils.checkInterrupted(e);
    }

    protected void remove(String fullPath)
    {
        ChildData data = currentData.remove(fullPath);
        if (data != null) {
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_REMOVED, data));
        }
    }

    private void handleStateChange(ConnectionState newState)
    {
        switch ( newState ) {
        case SUSPENDED:
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_SUSPENDED, null));
            break;

        case LOST:
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_LOST, null));
            break;

        case CONNECTED:
        case RECONNECTED:
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CONNECTION_RECONNECTED, null));
            try {
                refresh(RefreshMode.REBUILD);
            } catch (Exception e) {
                handleException(e);
            }
            break;
        }
    }

    private void processChildren(List<String> children, RefreshMode mode) throws Exception
    {
        /* Although we got watchers on the nodes and these watchers will remove the cached data
         * when the node is removed, the watchers can be lost when the ZooKeeper session expires.
         */
        Set<String> removedNodes = new HashSet<>(currentData.keySet());
        for (String child : children) {
            removedNodes.remove(ZKPaths.makePath(path, child));
        }
        for (String fullPath : removedNodes) {
            remove(fullPath);
        }

        for (String child : children) {
            String fullPath = ZKPaths.makePath(path, child);
            if (mode == RefreshMode.REBUILD || !currentData.containsKey(fullPath)) {
                getDataAndStat(fullPath);
            }
        }
    }

    private void updateCache(String fullPath, Stat stat, byte[] bytes)
    {
        ChildData data = new ChildData(fullPath, stat, bytes);
        ChildData previousData = currentData.put(fullPath, data);
        if (previousData == null) {
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_ADDED, data));
        } else if (previousData.getStat().getVersion() != stat.getVersion()) {
            callListeners(new PathChildrenCacheEvent(PathChildrenCacheEvent.Type.CHILD_UPDATED, data));
        }
    }
}
