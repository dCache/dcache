/* dCache - http://www.dcache.org/
 *
 * Copyright (C) 2017 Deutsches Elektronen-Synchrotron
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
package org.dcache.macaroons;

import com.google.common.base.Throwables;
import com.google.common.io.BaseEncoding;
import com.google.common.io.ByteSource;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Optional;

import dmg.cells.nucleus.CellLifeCycleAware;

import org.dcache.cells.CuratorFrameworkAware;

import static org.dcache.macaroons.ZookeeperSecretHandler.ZK_MACAROONS;

/**
 * A Zookeeper-backed storage for IdentifiedSecrets.  This class also maintains
 * an in-memory storage for improved performance when searching for suitable
 * existing secrets, or when handling expired secrets.
 */
public class ZookeeperSecretStorage implements PathChildrenCacheListener, CuratorFrameworkAware, CellLifeCycleAware
{
    private static final Logger LOG = LoggerFactory.getLogger(ZookeeperSecretStorage.class);
    private static final String ZK_MACAROONS_SECRETS = ZKPaths.makePath(ZK_MACAROONS, "secrets");
    private static final String IDENTITY_KEY = "id:";
    private static final String SECRET_KEY = "secret:";

    private final InMemorySecretStorage storage = new InMemorySecretStorage();

    private PathChildrenCache cache;
    private CuratorFramework client;

    @Override
    public void setCuratorFramework(CuratorFramework client)
    {
        this.client = client;
        cache = new PathChildrenCache(client, ZK_MACAROONS_SECRETS, true);
    }

    @Override
    public void afterStart()
    {
        cache.getListenable().addListener(this);
        try {
            cache.start();
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void beforeStop()
    {
        CloseableUtils.closeQuietly(cache);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws IOException
    {
        LOG.debug("Recieved event {}", event);

        ChildData child = event.getData();

        switch (event.getType()) {
        case CHILD_REMOVED:
            storage.remove(expiryFromPath(child.getPath()));
            break;

        case CHILD_UPDATED:
            LOG.error("Secret unexpectedly updated: {}", event);
            break;

        case CHILD_ADDED:
            storage.put(expiryFromPath(child.getPath()), decodeSecret(child.getData()));
            break;
        }
    }

    private Instant expiryFromPath(String path) throws DateTimeParseException
    {
        String expiry = ZKPaths.getNodeFromPath(path);
        return Instant.parse(expiry);
    }

    private String pathFromExpiry(Instant expiry)
    {
        return ZKPaths.makePath(ZK_MACAROONS_SECRETS, expiry.toString());
    }

    private IdentifiedSecret decodeSecret(byte[] data) throws IOException
    {
        String id = null;
        String encodedSecret = null;
        for (String line : ByteSource.wrap(data).asCharSource(StandardCharsets.US_ASCII).readLines()) {
            if (line.startsWith(IDENTITY_KEY)) {
                id = line.substring(IDENTITY_KEY.length());
            } else if (line.startsWith(SECRET_KEY)) {
                encodedSecret = line.substring(SECRET_KEY.length());
            }
        }
        if (id == null) {
            throw new IOException("Missing '" + IDENTITY_KEY + "' line");
        }
        if (encodedSecret == null) {
            throw new IOException("Missing '" + SECRET_KEY + "' line");
        }
        try {
            return new IdentifiedSecret(id, BaseEncoding.base64().decode(encodedSecret));
        } catch (IllegalArgumentException e) {
            throw new IOException("Bad encoded secret '" + encodedSecret + "': " + e.getMessage());
        }
    }

    private byte[] encodeSecret(IdentifiedSecret secret)
    {
        String encodedSecret = BaseEncoding.base64().omitPadding().encode(secret.getSecret());

        StringBuilder sb = new StringBuilder();
        sb.append(IDENTITY_KEY).append(secret.getIdentifier()).append('\n');
        sb.append(SECRET_KEY).append(encodedSecret).append('\n');
        return sb.toString().getBytes(StandardCharsets.US_ASCII);
    }

    public void removeExpiredSecrets()
    {
        storage.expiringBefore(Instant.now()).forEach(this::remove);
    }

    public byte[] get(String identifier)
    {
        return storage.get(identifier);
    }

    public Optional<IdentifiedSecret> firstExpiringAfter(Instant earliestExpiry)
    {
        return storage.firstExpiringAfter(earliestExpiry);
    }

    public IdentifiedSecret put(Instant expiry, IdentifiedSecret secret) throws Exception
    {
        LOG.debug("Adding secret {} into ZK with expire after {}", secret.getIdentifier(), expiry);

        try {
            client.create().creatingParentsIfNeeded().forPath(pathFromExpiry(expiry), encodeSecret(secret));
            storage.put(expiry, secret);
            return secret;
        } catch (KeeperException.NodeExistsException e) {
            LOG.debug("Lost put race, returning winner");
            Optional<IdentifiedSecret> winner = storage.get(expiry);
            return winner.orElseThrow(() -> e);
        }
    }

    private void remove(Instant expiry)
    {
        LOG.debug("Removing secret expiring at {} from ZK", expiry);

        String path = pathFromExpiry(expiry);
        try {
            client.delete().forPath(path);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            LOG.error("Failed to delete path {} from ZK: {}", path, e.getMessage());
        }
    }
}
