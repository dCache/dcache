/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2026 Deutsches Elektronen-Synchrotron
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
package org.dcache.http;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.dcache.cells.CuratorFrameworkAware;
import org.eclipse.jetty.session.AbstractSessionDataStore;
import org.eclipse.jetty.session.SessionData;
import org.eclipse.jetty.util.ClassLoadingObjectInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jetty SessionDataStore backed by ZooKeeper for persistent, cluster-safe sessions. Implements
 * CuratorFrameworkAware for automatic injection in dCache.
 */
public class ZooKeeperSessionDataStore extends AbstractSessionDataStore
      implements CuratorFrameworkAware {

    private CuratorFramework curator;
    private String root = "/dcache/auth/sessions";

    private final static Logger LOGGER = LoggerFactory.getLogger(ZooKeeperSessionDataStore.class);


    // -------------------------------
    // Curator injection
    // -------------------------------
    @Override
    public void setCuratorFramework(CuratorFramework client) {
        this.curator = client;
    }

    public void setRoot(String root) {
        this.root = root;
    }

    // -------------------------------
    // Lifecycle
    // -------------------------------

    @Override
    public SessionData doLoad(String id) throws Exception {
        LOGGER.debug("Loading session: {}", id);
        byte[] bytes;
        try {
            bytes = curator.getData().forPath(path(id));
        } catch (KeeperException.NoNodeException e) {
            return null;
        }

        try {
            return deserialize(bytes);
        } catch (Exception e) {
            // e.g. stored by an older Jetty version; treat as missing
            LOGGER.info("Dropping unreadable session {}: {}", id, e.toString());
            delete(id);
            return null;
        }
    }

    @Override
    public Set<String> doCheckExpired(Set<String> candidates, long time) {
        Set<String> expired = new HashSet<>();
        for (String id : candidates) {
            try {
                SessionData data = doLoad(id);
                if (data == null || data.isExpiredAt(time)) {
                    expired.add(id);
                }
            } catch (Exception e) {
                // If there’s any problem loading, consider it expired
                expired.add(id);
            }
        }
        return expired;
    }

    @Override
    public Set<String> doGetExpired(long before) {
        Set<String> expired = new HashSet<>();
        for (String id : sessionIds()) {
            try {
                SessionData data = doLoad(id);
                if (data != null && data.isExpiredAt(before)
                      && data.getContextPath().equals(_context.getCanonicalContextPath())) {
                    expired.add(id);
                }
            } catch (Exception e) {
                LOGGER.debug("Failed to check session {}: {}", id, e.toString());
            }
        }
        return expired;
    }

    @Override
    public void doCleanOrphans(long time) {
        for (String id : sessionIds()) {
            try {
                SessionData data = doLoad(id);
                if (data != null && data.isExpiredAt(time)) {
                    delete(id);
                }
            } catch (Exception e) {
                LOGGER.debug("Failed to clean session {}: {}", id, e.toString());
            }
        }
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        LOGGER.debug("Starting ZooKeeperSessionDataStore with root: {}", root);

        if (curator == null) {
            LOGGER.error("CuratorFramework not injected, cannot start ZooKeeperSessionDataStore");
        }
        LOGGER.warn("Ensuring root path exists in ZooKeeper: " + root);
        if (curator.checkExists().forPath(root) == null) {
            curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
                  .forPath(root);
            LOGGER.info("Created root path in ZooKeeper: {}", root);
        }
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }


    // ------------------------
    // Helpers
    // ------------------------
    private String path(String id) {
        return root + "/" + id;
    }

    private List<String> sessionIds() {
        try {
            return curator.getChildren().forPath(root);
        } catch (Exception e) {
            LOGGER.warn("Failed to list sessions in {}: {}", root, e.toString());
            return List.of();
        }
    }

    // -------------------------------
    // Required methods
    // -------------------------------
    @Override
    public boolean doExists(String id) throws Exception {
        return curator.checkExists().forPath(path(id)) != null;
    }

    @Override
    public boolean delete(String id) throws Exception {
        LOGGER.debug("Deleting session: {}", id);
        try {
            curator.delete().forPath(path(id));
        } catch (KeeperException.NoNodeException e) {
            LOGGER.debug("Session not found, nothing to delete: {}", id);
            return false;
        }
        LOGGER.debug("Successfully deleted session: {}", id);
        return true;
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        LOGGER.debug("Storing session: {}", id);

        byte[] bytes = serialize(data);
        String fullPath = path(id);

        if (doExists(id)) {
            curator.setData().forPath(fullPath, bytes);
            LOGGER.debug("Updated existing session: {}, size: {} bytes", id, bytes.length);
        } else {
            curator.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.EPHEMERAL)
                  .forPath(fullPath, bytes);
            LOGGER.debug("Created new session: {}, size: {} bytes", id, bytes.length);
        }
    }

    @Override
    public boolean isPassivating() {
        return false;
    }

    private byte[] serialize(SessionData data) throws Exception {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();

        try (ObjectOutputStream object = new ObjectOutputStream(baos)) {
            object.writeObject(data);
        }

        return baos.toByteArray();
    }

    private SessionData deserialize(byte[] data) throws Exception {

        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
              ClassLoadingObjectInputStream object =
                    new ClassLoadingObjectInputStream(bais)) {

            return (SessionData) object.readObject();
        }
    }

}
