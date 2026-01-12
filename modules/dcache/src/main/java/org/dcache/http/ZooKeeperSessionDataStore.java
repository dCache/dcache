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
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.dcache.cells.CuratorFrameworkAware;
import org.eclipse.jetty.server.session.AbstractSessionDataStore;
import org.eclipse.jetty.server.session.SessionData;
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
        byte[] bytes = curator.getData().forPath(path(id));
        return deserialize(bytes);
    }

    @Override
    public Set<String> doGetExpired(Set<String> candidates) {
        Set<String> expired = new HashSet<>();
        long now = System.currentTimeMillis();

        for (String id : candidates) {
            try {
                SessionData data = doLoad(id);
                if (data == null || data.getExpiry() >= now) {
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

    // -------------------------------
    // Required methods
    // -------------------------------
    @Override
    public boolean exists(String id) throws Exception {
        return curator.checkExists().forPath(path(id)) != null;
    }

    @Override
    public SessionData load(String id) throws Exception {
        if (!exists(id)) {
            return null;
        }
        byte[] bytes = curator.getData().forPath(path(id));
        return deserialize(bytes);
    }

    @Override
    public boolean delete(String id) throws Exception {
        LOGGER.debug("Deleting session: {}", id);
        if (!exists(id)) {
            LOGGER.error("Session not found, nothing to delete: {}", id);
            return false;
        }

        curator.delete().forPath(path(id));
        LOGGER.debug("Successfully deleted session: {}", id);
        return true;
    }

    @Override
    public void doStore(String id, SessionData data, long lastSaveTime) throws Exception {
        LOGGER.debug("Storing session: {}", id);

        byte[] bytes = serialize(data);
        String fullPath = path(id);

        if (exists(id)) {
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
    public Set<String> getExpired(Set<String> candidates) {
        Set<String> expired = new HashSet<>();
        long now = System.currentTimeMillis();

        for (String id : candidates) {
            try {
                SessionData data = load(id);
                if (data == null || data.getExpiry() <= now) {
                    expired.add(id);
                }
            } catch (Exception e) {
                expired.add(id);
            }
        }
        return expired;
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
