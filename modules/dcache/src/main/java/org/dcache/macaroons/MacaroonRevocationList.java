/* dCache - http://www.dcache.org/
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
package org.dcache.macaroons;

import static org.dcache.macaroons.ZookeeperSecretHandler.ZK_MACAROONS;

import com.github.nitram509.jmacaroons.CaveatPacket;
import com.github.nitram509.jmacaroons.Macaroon;
import com.github.nitram509.jmacaroons.MacaroonsBuilder;
import com.google.common.annotations.VisibleForTesting;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellCommandListener;
import dmg.cells.nucleus.CellIdentityAware;
import dmg.cells.nucleus.CellLifeCycleAware;
import dmg.util.command.Argument;
import dmg.util.command.Command;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.KeeperException;
import org.dcache.cells.CuratorFrameworkAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A list of macaroons that have been revoked.  A revoked macaroon is rejected by {@link
 * MacaroonProcessor#expandMacaroon} even if it is cryptographically valid and has not expired.
 * <p>
 * Revoked macaroons are identified by their (full) final signature.  This is a one-way
 * calculation over the macaroon itself, so whoever presents a (leaked) macaroon can have it
 * revoked without knowing anything beyond the token, and two copies of the same macaroon always
 * map to the same revocation key.  The signature is not the signing secret: deriving a working
 * macaroon from the stored values would require a SHA-256 second-preimage, so the list leaks no
 * usable credentials.
 * <p>
 * Revoking a macaroon revokes exactly that macaroon: a macaroon that was created by adding
 * further caveats to a revoked macaroon has a different final signature and is therefore not
 * revoked by that entry.
 * <p>
 * If a CuratorFramework is injected (via the cells Spring wiring, as for {@link
 * ZookeeperSecretStorage}) then revoked macaroons are stored below {@code
 * /dcache/macaroons/revoked}, are visible to all domains immediately, and survive a restart;
 * otherwise the list is per-domain and in-memory only (mainly useful for testing).
 */
public class MacaroonRevocationList implements PathChildrenCacheListener,
      CuratorFrameworkAware, CellCommandListener, CellLifeCycleAware, CellIdentityAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(MacaroonRevocationList.class);

    public static final String ZK_MACAROONS_REVOKED = ZKPaths.makePath(ZK_MACAROONS, "revoked");

    private static final String IDENTITY_KEY = "identity:";
    private static final String SIGNATURE_KEY = "signature:";
    private static final String REVOKED_AT_KEY = "revokedAt:";
    private static final String EXPIRES_KEY = "expires:";

    /**
     * Entries older than 'pruneAfter' plus this margin are pruned.  The margin
     * covers clock skew between the minting door and the pruning domain.
     */
    private static final long PRUNE_SAFETY_MARGIN = java.util.concurrent.TimeUnit.HOURS.toMillis(1);
    private static final String ISSUE_ID_PREFIX = CaveatType.ISSUE_ID.getLabel() + ":";
    private static final String BEFORE_PREFIX = CaveatType.BEFORE.getLabel() + ":";

    /** How many revoked macaroons are accepted before revocation is refused. */
    private static final int DEFAULT_MAX_REVOKED = 100;

    /** Revocation key (lowercase hex signature) -> public identity for display. */
    private final Map<String,String> revoked = new ConcurrentHashMap<>();
    /** Wall-clock revocation time (epoch millis) per revoked signature; 0 if unknown. */
    private final Map<String,Long> revokedAt = new ConcurrentHashMap<>();
    /** Claimed macaroon expiry (epoch millis) per revoked signature; 0 if unknown. */
    private final Map<String,Long> expiresAt = new ConcurrentHashMap<>();
    /** Age beyond which revocation entries are pruned; 0 disables pruning. */
    private long pruneAfter;

    private CuratorFramework client;
    private PathChildrenCache cache;
    private int maxRevoked = DEFAULT_MAX_REVOKED;

    @Override
    public void setCuratorFramework(CuratorFramework client) {
        this.client = client;
    }

    @Override
    public void setCellAddress(CellAddressCore address) {
        // required by the cells Spring wiring; no per-address behaviour needed here.
    }

    @Override
    public void afterStart() {
        if (client != null) {
            cache = new PathChildrenCache(client, ZK_MACAROONS_REVOKED, true);
            cache.getListenable().addListener(this);
            try {
                cache.start();
            } catch (Exception e) {
                throw new RuntimeException("Failed to start macaroon revocation cache: "
                      + e.getMessage(), e);
            }
        }
    }

    @Override
    public void beforeStop() {
        CloseableUtils.closeQuietly(cache);
    }

    @Override
    public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
          throws IOException {
        ChildData child = event.getData();
        if (child == null) {
            return;
        }
        switch (event.getType()) {
            case CHILD_ADDED:
                String signature = signatureFromData(child);
                revoked.put(signature, identityFromData(child));
                revokedAt.put(signature, timestampFromData(child, REVOKED_AT_KEY));
                expiresAt.put(signature, timestampFromData(child, EXPIRES_KEY));
                break;
            case CHILD_REMOVED:
                drop(ZKPaths.getNodeFromPath(child.getPath()).toLowerCase());
                break;
            case CHILD_UPDATED:
                LOGGER.error("Revoked macaroon unexpectedly updated: {}", event);
                break;
            default:
                break;
        }
    }

    public void setMaxRevoked(int max) {
        this.maxRevoked = max;
    }

    /**
     * Prune revocation entries older than this (milliseconds) since revocation.
     * A macaroon revoked at time T was minted no later than T, and doors cap
     * macaroon lifetimes at mint time, so entries older than the maximum token
     * lifetime (plus a margin) can only refer to tokens that can no longer
     * authenticate.  Setting 0 (the default) disables pruning.
     */
    public void setPruneAfter(long millis) {
        this.pruneAfter = millis;
    }

    public int getMaxRevoked() {
        return maxRevoked;
    }

    public int size() {
        return revoked.size();
    }

    private void pruneExpired(long now) {
        if (pruneAfter <= 0) {
            return;
        }
        for (Map.Entry<String,Long> e : revokedAt.entrySet()) {
            long revokedTime = e.getValue();
            if (revokedTime <= 0) {
                continue; // age unknown: never pruned
            }
            String signature = e.getKey();
            long outerWindow = revokedTime + pruneAfter + PRUNE_SAFETY_MARGIN;
            long deadline = outerWindow;
            Long expiry = expiresAt.get(signature);
            if (expiry != null && expiry > 0) {
                // The entry dies with the token itself; the outer window ensures a
                // doctored (never verifiable) far-future expiry cannot bloat the list.
                deadline = Math.min(outerWindow, expiry + PRUNE_SAFETY_MARGIN);
            }
            if (now <= deadline) {
                continue;
            }
            if (client != null) {
                try {
                    client.delete().forPath(
                          ZKPaths.makePath(ZK_MACAROONS_REVOKED, signature));
                    // the cache listener removes the in-memory entries
                    continue;
                } catch (KeeperException.NoNodeException ignored) {
                    // already gone; drop the memory entries below
                } catch (Exception ex) {
                    LOGGER.warn("Failed to prune revoked macaroon {}: {}",
                          signature, ex.toString());
                    continue;
                }
            }
            drop(signature);
            LOGGER.info("Pruned revocation entry for macaroon {} (revoked {}) as the "
                  + "macaroon must have expired", signature, revokedTime);
        }
    }

    private void drop(String signature) {
        revoked.remove(signature);
        revokedAt.remove(signature);
        expiresAt.remove(signature);
    }

    /**
     * Return whether the macaroon with this (hex-encoded) final signature has been revoked.
     */
    public boolean isRevoked(String signature) {
        return revoked.containsKey(normalise(signature));
    }

    /**
     * The revocation key of a serialised macaroon: its full final signature, lowercase hex.
     */
    public static String signatureOf(String serialisedMacaroon) throws InvalidMacaroonException {
        return normalise(deserialise(serialisedMacaroon).signature);
    }

    /**
     * The human-readable (public) identity of a serialised macaroon: the issue-id caveat plus a
     * truncated signature, exactly as reported when the macaroon was minted.
     */
    public static String identityOf(String serialisedMacaroon) throws InvalidMacaroonException {
        Macaroon m = deserialise(serialisedMacaroon);
        String issueId = null;
        CaveatPacket[] packets = m.caveatPackets;
        if (packets != null) {
            for (CaveatPacket p : packets) {
                String value = p.getValueAsText();
                if (value != null && value.startsWith(ISSUE_ID_PREFIX)) {
                    issueId = value.substring(ISSUE_ID_PREFIX.length());
                }
            }
        }
        return MacaroonContext.buildId(m.signature, issueId);
    }

    /**
     * The expiry claimed by a serialised macaroon (epoch millis), or 0 if it carries
     * no parsable 'before' caveat.  For a macaroon that passes verification this value
     * is trustworthy: caveat packets are signed, so an attacker cannot extend or remove
     * the expiry without invalidating the signature.  For a pasted token that would
     * never verify it is meaningless, which is why pruning still honours the outer window.
     */
    public static long expiryOf(String serialisedMacaroon) {
        try {
            CaveatPacket[] packets = deserialise(serialisedMacaroon).caveatPackets;
            if (packets != null) {
                for (CaveatPacket p : packets) {
                    String value = p.getValueAsText();
                    if (value != null && value.startsWith(BEFORE_PREFIX)) {
                        return java.time.Instant
                              .parse(value.substring(BEFORE_PREFIX.length()))
                              .toEpochMilli();
                    }
                }
            }
        } catch (Exception ignored) {
            // unparsable or absent: treated as unknown expiry
        }
        return 0;
    }

    /**
     * Revoke the macaroon represented by the supplied serialised token.
     *
     * @return the macaroon's public (issue-id based) identity
     */
    public synchronized String revoke(String serialisedMacaroon) throws InvalidMacaroonException {
        long now = System.currentTimeMillis();
        pruneExpired(now);
        String signature = signatureOf(serialisedMacaroon);
        String identity = identityOf(serialisedMacaroon);
        long expiry = expiryOf(serialisedMacaroon);

        if (revoked.containsKey(signature)) {
            LOGGER.debug("Macaroon {} was already revoked", identity);
            return identity;
        }
        if (revoked.size() >= maxRevoked) {
            // A bounded list avoids unbounded growth from a denial-of-service attack via
            // spurious revocations.  Escalation (removing the signing secret, revoking whole
            // batches) is deliberately left to the admin for now.
            throw new InvalidMacaroonException("The revocation list is full (" + maxRevoked
                  + " entries).  Restore entries or remove the macaroon signing secret instead.");
        }

        if (client != null) {
            try {
                client.create().creatingParentsIfNeeded()
                      .forPath(ZKPaths.makePath(ZK_MACAROONS_REVOKED, signature),
                            encodeEntry(identity, signature, now, expiry));
                // the cache listener populates 'revoked'
            } catch (KeeperException.NodeExistsException e) {
                LOGGER.debug("Revocation of {} was created by another domain", identity);
            } catch (Exception e) {
                throw new InvalidMacaroonException("Failed to update revocation store: "
                      + e.getMessage());
            }
        } else {
            revoked.put(signature, identity);
            revokedAt.put(signature, now);
            expiresAt.put(signature, expiry);
        }
        LOGGER.info("Revoked macaroon {}", identity);
        return identity;
    }

    /**
     * Remove the revocation of the supplied serialised macaroon.
     *
     * @return the macaroon's public identity, or null if it was not revoked
     */
    public synchronized String restore(String serialisedMacaroon) throws InvalidMacaroonException {
        pruneExpired(System.currentTimeMillis());
        String signature = signatureOf(serialisedMacaroon);
        String identity = revoked.remove(signature);
        drop(signature);
        if (client != null) {
            try {
                client.delete().forPath(ZKPaths.makePath(ZK_MACAROONS_REVOKED, signature));
            } catch (KeeperException.NoNodeException e) {
                identity = null;
            } catch (Exception e) {
                throw new InvalidMacaroonException("Failed to update revocation store: "
                      + e.getMessage());
            }
        }
        return identity;
    }

    public synchronized Map<String,String> asMap() {
        pruneExpired(System.currentTimeMillis());
        return new TreeMap<>(revoked);
    }

    @VisibleForTesting
    void injectRevokedEntry(String signature, String identity) {
        injectRevokedEntry(signature, identity, 0, 0);
    }

    @VisibleForTesting
    void injectRevokedEntry(String signature, String identity, long revokedAt) {
        injectRevokedEntry(signature, identity, revokedAt, 0);
    }

    @VisibleForTesting
    void injectRevokedEntry(String signature, String identity, long revokedAt, long expiresAt) {
        String sig = normalise(signature);
        revoked.put(sig, identity);
        this.revokedAt.put(sig, revokedAt);
        this.expiresAt.put(sig, expiresAt);
    }

    private static Macaroon deserialise(String serialisedMacaroon)
          throws InvalidMacaroonException {
        try {
            return MacaroonsBuilder.deserialize(serialisedMacaroon);
        } catch (Exception e) {
            throw new InvalidMacaroonException("Not a valid macaroon: " + e.getMessage());
        }
    }

    private static byte[] encodeEntry(String identity, String signature, long revokedAt,
          long expiresAt) {
        String data = IDENTITY_KEY + (identity == null ? "?" : identity) + '\n'
              + SIGNATURE_KEY + signature + '\n'
              + REVOKED_AT_KEY + revokedAt + '\n'
              + EXPIRES_KEY + expiresAt + '\n';
        return data.getBytes(StandardCharsets.US_ASCII);
    }

    private static long timestampFromData(ChildData data, String key) {
        for (String line : decode(data).split("\n")) {
            if (line.startsWith(key)) {
                try {
                    return Long.parseLong(line.substring(key.length()).trim());
                } catch (NumberFormatException e) {
                    return 0; // unknown: treated conservatively
                }
            }
        }
        return 0; // written by an older version: unknown
    }

    private static String identityFromData(ChildData data) {
        for (String line : decode(data).split("\n")) {
            if (line.startsWith(IDENTITY_KEY)) {
                return line.substring(IDENTITY_KEY.length());
            }
        }
        return null;
    }

    private static String signatureFromData(ChildData data) throws IOException {
        for (String line : decode(data).split("\n")) {
            if (line.startsWith(SIGNATURE_KEY)) {
                return normalise(line.substring(SIGNATURE_KEY.length()));
            }
        }
        throw new IOException("Revocation entry is missing a '" + SIGNATURE_KEY + "' line.");
    }

    private static String decode(ChildData data) {
        return new String(data.getData(), StandardCharsets.US_ASCII);
    }

    private static String normalise(String signature) {
        return signature.toLowerCase();
    }

    /* ------------------------------------------------------------ shell */

    @Command(name = "revoke", hint = "revoke a specific macaroon",
          description = "Revoke a specific macaroon.  The argument is the full serialised "
                + "macaroon (the token itself, as presented by the client), not the short id. "
                + "From now on dCache will reject this macaroon, even though it remains "
                + "cryptographically valid.  Any macaroon derived from this one by adding "
                + "caveats must be revoked separately.")
    public class RevokeCommand implements Callable<Serializable> {

        @Argument(usage = "The serialised macaroon to revoke")
        String macaroon;

        @Override
        public Serializable call() throws InvalidMacaroonException {
            return "Revoked macaroon " + revoke(macaroon);
        }
    }

    @Command(name = "restore", hint = "remove an entry from the revocation list",
          description = "Stop revoking a macaroon.  The argument is the full serialised "
                + "macaroon.")
    public class RestoreCommand implements Callable<Serializable> {

        @Argument(usage = "The serialised macaroon to restore")
        String macaroon;

        @Override
        public Serializable call() throws InvalidMacaroonException {
            String identity = restore(macaroon);
            return identity == null
                  ? "That macaroon was not revoked."
                  : "Restored macaroon " + identity;
        }
    }

    @Command(name = "revoked", hint = "list revoked macaroons",
          description = "List all revoked macaroons by their public id and revocation key.")
    public class RevokedCommand implements Callable<Serializable> {

        @Override
        public Serializable call() {
            if (revoked.isEmpty()) {
                return "No macaroons have been revoked.";
            }
            StringBuilder sb = new StringBuilder();
            asMap().forEach((signature, identity) -> sb
                  .append(identity == null ? signature : identity)
                  .append("  [").append(signature).append(']')
                  .append('\n'));
            return (Serializable) (sb + "\n" + revoked.size() + " of " + maxRevoked
                  + " revocation slots used.");
        }
    }
}
