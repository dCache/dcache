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
package org.dcache.space;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.LoadingCache;
import diskCacheV111.poolManager.CostModule;
import diskCacheV111.poolManager.PoolPreferenceLevel;
import diskCacheV111.poolManager.PoolSelectionUnit;
import diskCacheV111.poolManager.PoolSelectionUnit.DirectionType;
import diskCacheV111.pools.PoolCostInfo;
import diskCacheV111.pools.PoolCostInfo.PoolSpaceInfo;
import diskCacheV111.services.space.Space;
import diskCacheV111.services.space.message.GetLinkGroupsMessage;
import diskCacheV111.util.CacheException;
import dmg.cells.nucleus.NoRouteToCellException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.dcache.cells.CellStub;
import org.dcache.poolmanager.PoolMonitor;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.remoting.RemoteAccessException;

/**
 * Total and free space reported by doors to clients (e.g., to df on an NFS or xrootdfs mount), as
 * configured by the {@code <door>.capacity.source} and {@code nfs.capacity.reservation}
 * properties.
 */
public final class Capacity {

    private static final Logger LOGGER = LoggerFactory.getLogger(Capacity.class);

    /**
     * Reported if the capacity is unknown: the same fixed size that Chimera reports, all free.
     */
    public static final Capacity UNKNOWN = new Capacity(1L << 60, 1L << 60, null);

    public enum Source {NAMESPACE, POOLS, RESERVATION}

    private final long total;
    private final long free;
    private final String reservation;

    private Capacity(long total, long free, String reservation) {
        this.total = total;
        this.free = Math.min(Math.max(0L, free), total);
        this.reservation = reservation;
    }

    public long getTotalSpace() {
        return total;
    }

    public long getFreeSpace() {
        return free;
    }

    public long getUsedSpace() {
        return total - free;
    }

    /**
     * Returns the token of the space reservation this capacity was taken from, if any.
     */
    public Optional<String> getReservation() {
        return Optional.ofNullable(reservation);
    }

    /**
     * Determines the capacity a door reports.
     */
    public static class Provider {

        private final Source source;
        private final String reservation;
        private final PoolMonitor poolMonitor;
        private final LoadingCache<String, Optional<Space>> spaceLookupCache;
        private final Set<String> failing = ConcurrentHashMap.newKeySet();
        private final Cache<Long, String> linkGroupNames = CacheBuilder.newBuilder()
              .expireAfterWrite(10, TimeUnit.MINUTES).build();
        private boolean spaceReservation;
        private CellStub spaceManager;

        public Provider(String source, String reservation, PoolMonitor poolMonitor,
              LoadingCache<String, Optional<Space>> spaceLookupCache) {
            try {
                this.source = Source.valueOf(source.strip().toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("Invalid capacity source: " + source);
            }
            this.reservation = reservation.isBlank() ? null : reservation.strip();
            if (this.reservation != null && !this.reservation.matches("\\d+")) {
                throw new IllegalArgumentException("Invalid space reservation: " + reservation);
            }
            this.poolMonitor = poolMonitor;
            this.spaceLookupCache = spaceLookupCache;
        }

        /**
         * Sets whether writes go through the spacemanager, which may select a link group.
         */
        public void setSpaceReservation(boolean spaceReservation) {
            this.spaceReservation = spaceReservation;
        }

        public void setSpaceManager(CellStub spaceManager) {
            this.spaceManager = spaceManager;
        }

        public Source getSource() {
            return source;
        }

        /**
         * Whether the space reservation is given by the WriteToken tag of the queried path.
         */
        public boolean usesWriteToken() {
            return source == Source.RESERVATION && reservation == null;
        }

        /**
         * Returns the capacity to report without a queried path: all pools or the configured
         * space reservation.
         */
        public Capacity get() throws CacheException {
            return get(null, null, null);
        }

        /**
         * Returns the capacity to report. A failure is logged once, rather than for every query,
         * until the capacity can be determined again.
         *
         * @param path     the storage info of the queried path, or null
         * @param host     the client address, to select the pools a file in the path is written to
         * @param protocol the protocol (name/major version), to select the pools likewise
         */
        public Capacity get(FileAttributes path, String host, String protocol)
              throws CacheException {
            if (source == Source.NAMESPACE) {
                return UNKNOWN;
            }
            String token = reservation != null || path == null ? reservation
                  : path.getStorageInfo().getMap().get("writeToken");
            boolean pools = source == Source.POOLS || token == null;
            String what = pools ? "pools" : "space reservation " + token;
            try {
                Capacity capacity = pools ? ofPools(path, host, protocol) : ofReservation(token);
                if (failing.remove(what)) {
                    LOGGER.info("Capacity of {} can be determined again.", what);
                }
                return capacity;
            } catch (CacheException e) {
                if (failing.add(what)) {
                    LOGGER.warn("Cannot determine capacity of {}: {} (logged once until it"
                          + " recovers)", what, e.getMessage());
                }
                throw e;
            }
        }

        private Capacity ofReservation(String token) throws CacheException {
            Space space = space(token);
            return new Capacity(space.getSizeInBytes(), space.getAvailableSpaceInBytes(), token);
        }

        private Space space(String token) throws CacheException {
            try {
                return spaceLookupCache.get(token).orElseThrow(
                      () -> new CacheException("Space reservation " + token + " not found"));
            } catch (ExecutionException e) {
                throw new CacheException("Failed to look up space reservation " + token + ": "
                      + e.getCause().getMessage(), e.getCause());
            }
        }

        private String linkGroupOf(String token) throws CacheException {
            long id = space(token).getLinkGroupId();
            String name = linkGroupNames.getIfPresent(id);
            if (name == null) {
                try {
                    spaceManager.sendAndWait(new GetLinkGroupsMessage()).getLinkGroups()
                          .forEach(group -> linkGroupNames.put(group.getId(), group.getName()));
                } catch (NoRouteToCellException | InterruptedException e) {
                    throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                          "Failed to look up link groups: " + e.getMessage(), e);
                }
                name = linkGroupNames.getIfPresent(id);
                if (name == null) {
                    throw new CacheException("Link group of space reservation " + token
                          + " not found");
                }
            }
            return name;
        }

        /**
         * Returns the pools a file in the path is first written to. With space reservation, these
         * are the pools of the link group of the WriteToken or, without one, of a matching link
         * group, as the spacemanager selects them if unreserved uploads to link groups are allowed
         * (a setting doors cannot see), and otherwise of the links outside link groups.
         */
        private List<String> writePools(FileAttributes path, String host, String protocol)
              throws CacheException {
            PoolSelectionUnit psu = poolMonitor.getPoolSelectionUnit();
            String token = path.getStorageInfo().getMap().get("writeToken");
            List<String> linkGroups = new ArrayList<>();
            if (spaceReservation && token != null) {
                linkGroups.add(linkGroupOf(token));
            } else {
                if (spaceReservation) {
                    linkGroups.addAll(psu.getLinkGroups().keySet());
                }
                linkGroups.add(null);
            }
            for (String linkGroup : linkGroups) {
                PoolPreferenceLevel[] levels = psu.match(DirectionType.WRITE, host, protocol, path,
                      linkGroup, pool -> false);
                if (levels.length > 0 && !levels[0].getPoolList().isEmpty()) {
                    return levels[0].getPoolList();
                }
            }
            return List.of();
        }

        /**
         * Sums the space of the online pools a file in the path is written to first, or of all
         * online pools without a path, counting removable (cached) replicas as free, as the
         * frontend does for storage resource reporting. If no pool may be written to, the capacity
         * is unknown.
         */
        private Capacity ofPools(FileAttributes path, String host, String protocol)
              throws CacheException {
            List<PoolSpaceInfo> pools;
            try {
                CostModule costModule = poolMonitor.getCostModule();
                if (path == null) {
                    pools = costModule.getPoolCostInfos().stream()
                          .map(PoolCostInfo::getSpaceInfo).collect(Collectors.toList());
                } else {
                    List<String> names = writePools(path, host, protocol);
                    if (names.isEmpty()) {
                        return UNKNOWN;
                    }
                    pools = names.stream().map(costModule::getPoolCostInfo)
                          .filter(Objects::nonNull).map(PoolCostInfo::getSpaceInfo)
                          .collect(Collectors.toList());
                }
            } catch (IllegalArgumentException e) {
                return UNKNOWN; // no storage or net unit matches
            } catch (RemoteAccessException e) {
                pools = List.of();
            }
            if (pools.isEmpty()) {
                throw new CacheException(CacheException.SERVICE_UNAVAILABLE,
                      "Pool information is not available");
            }
            return new Capacity(pools.stream().mapToLong(PoolSpaceInfo::getTotalSpace).sum(),
                  pools.stream().mapToLong(p -> p.getFreeSpace() + p.getRemovableSpace()).sum(),
                  null);
        }
    }
}
