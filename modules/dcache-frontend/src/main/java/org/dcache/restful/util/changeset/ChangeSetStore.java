package org.dcache.restful.util.changeset;

import java.util.Optional;
import javax.inject.Inject;
import javax.ws.rs.ClientErrorException;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.core.Response;

import diskCacheV111.poolManager.PoolSelectionUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.dcache.cells.CuratorFrameworkAware;
import org.dcache.poolmanager.PoolMonitor;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ChangeSetStore implements CuratorFrameworkAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeSetStore.class);

    private static final String ROOT = "/dcache/frontend/changesets";
    private static final String PREFIX = "changeset-";

    private CuratorFramework curator;

    @Override
    public void setCuratorFramework(CuratorFramework client) {
        curator = client;
    }

    /**
     * Stores a new change set.
     *
     * @return the id of the change set
     */
    public long create(JSONObject changeSet) {
        try {
            String path = curator.create()
                  .creatingParentsIfNeeded()
                  .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                  .forPath(ZKPaths.makePath(ROOT, PREFIX), changeSet.toString().getBytes(UTF_8));
            return Long.parseLong(ZKPaths.getNodeFromPath(path).substring(PREFIX.length()));
        } catch (Exception e) {
            LOGGER.warn("Failed to create change set: {}", e.toString());
            throw new InternalServerErrorException("Failed to create change set.", e);
        }
    }

    /**
     * A change set together with the version of its node, which a later {@link #update}
     * must match.
     */
    public record Stored(JSONObject changeSet, int version) {
    }

    /**
     * Returns the stored change set, or nothing if there is no change set with this id.
     */
    public Optional<Stored> read(long id) {
        try {
            Stat stat = new Stat();
            byte[] data = curator.getData().storingStatIn(stat).forPath(path(id));
            return Optional.of(new Stored(new JSONObject(new String(data, UTF_8)),
                  stat.getVersion()));
        } catch (KeeperException.NoNodeException e) {
            return Optional.empty();
        } catch (Exception e) {
            LOGGER.warn("Failed to read change set {}: {}", id, e.toString());
            throw new InternalServerErrorException("Failed to read change set " + id + ".", e);
        }
    }

    /**
     * Replaces a stored change set, provided nobody else changed it since it was read.
     *
     * @param version the version the change set was read with
     * @return the new version
     */
    public int update(long id, JSONObject changeSet, int version) {
        try {
            return curator.setData().withVersion(version)
                  .forPath(path(id), changeSet.toString().getBytes(UTF_8))
                  .getVersion();
        } catch (KeeperException.BadVersionException e) {
            throw new ClientErrorException("Change set " + id + " was changed concurrently",
                  Response.Status.CONFLICT);
        } catch (Exception e) {
            LOGGER.warn("Failed to update change set {}: {}", id, e.toString());
            throw new InternalServerErrorException("Failed to update change set " + id + ".", e);
        }
    }

    public void delete(long id) {
        try {
            curator.delete().forPath(path(id));
        } catch (KeeperException.NoNodeException e) {
            // ignore
        } catch (Exception e) {
            LOGGER.warn("Failed to delete change set {}: {}", id, e.toString());
            throw new InternalServerErrorException("Failed to delete change set " + id + ".", e);
        }
    }

    private static String path(long id) {
        return ZKPaths.makePath(ROOT, PREFIX + String.format("%010d", id));
    }
}
