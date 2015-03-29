package org.dcache.services.info.secondaryInfoProviders;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.SetMultimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.LinkInfo;
import org.dcache.services.info.stateInfo.LinkInfoVisitor;
import org.dcache.services.info.stateInfo.PoolSpaceVisitor;
import org.dcache.services.info.stateInfo.SpaceInfo;

/**
 * The NormalisaedAccessSpace (NAS) maintainer updates the <code>nas</code>
 * branch of the dCache state based on changes to pool space usage or
 * pool-link membership.
 *
 * A NAS is a set of pools. They are somewhat similar to a poolgroup except
 * that dCache PSU has no knowledge of them. Each NAS has the following
 * properties:
 * <ol>
 * <li>all pools are a member of precisely one NAS,
 * <li>all NAS have at least one member pool,
 * <li>all pools within a NAS have the same "access", where access is the
 * set of links that point to the pool (either directly or indirectly through
 * a pool group),
 * <li>two pools in two different NAS do not have the same access, meaning
 * that one of them is in a link the other is not in.
 * </ol>
 *
 * NAS association with a link is not exclusive. Pools in a NAS that is
 * accessible from one link may be accessible from a different link. Those
 * pools accessible in a NAS accessible through a link may share that link
 * with other pools.
 */
public class NormalisedAccessSpaceMaintainer extends AbstractStateWatcher
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(NormalisedAccessSpaceMaintainer.class);

    /**
     * How we want to represent the different LinkInfo.UNIT_TYPE values as
     * path elements in the resulting metrics
     */
    @SuppressWarnings("serial")
    private static final Map<LinkInfo.UNIT_TYPE, String> UNIT_TYPE_STORAGE_NAME =
            ImmutableMap.of(LinkInfo.UNIT_TYPE.DCACHE, "dcache",
                            LinkInfo.UNIT_TYPE.STORE, "store");

    /**
     * How we want to represent the different LinkInfo.OPERATION values as
     * path elements in resulting metrics
     */
    @SuppressWarnings("serial")
    private static final Map<LinkInfo.OPERATION, String> OPERATION_STORAGE_NAME =
            ImmutableMap.of(LinkInfo.OPERATION.READ, "read",
                            LinkInfo.OPERATION.WRITE, "write",
                            LinkInfo.OPERATION.CACHE, "stage");

    private static final String PREDICATE_PATHS[] =
            { "links.*.pools.*", "links.*.units.store.*",
             "links.*.units.dcache.*", "pools.*.space.*" };

    private static final StatePath NAS_PATH = new StatePath("nas");

    /**
     * A class describing the "paint" information for a pool. Each pool has a
     * PaintInfo object that describes the different access methods for this
     * pool. Pools with the same PaintInfo are members of the same NAS.
     */
    static protected class PaintInfo
    {
        public static final String NAS_NAME_INACCESSIBLE = "inaccessible";
        public static final String NAS_NAME_TOO_LONG_PREFIX = "complex-";
        public static final int NAS_NAME_MAX_LENGTH = 100;

        /**
         * The Set of LinkInfo.OPERATIONS that we paint a pool on.
         */
        private static final Set<LinkInfo.OPERATION> CONSIDERED_OPERATIONS =
                EnumSet.of(LinkInfo.OPERATION.READ, LinkInfo.OPERATION.WRITE, LinkInfo.OPERATION.CACHE);

        /**
         * The Set of LinkInfo.UNIT_TYPES that we paint a pool on.
         */
        private static final Set<LinkInfo.UNIT_TYPE> CONSIDERED_UNIT_TYPES =
                EnumSet.of(LinkInfo.UNIT_TYPE.DCACHE, LinkInfo.UNIT_TYPE.STORE);

        private final String _poolId;
        private final Set<String> _links = new HashSet<>();

        /** The cached copy of the NAS' */
        private String _nasName;

        /** Store all units by unit-type and operation */
        private final Map<LinkInfo.UNIT_TYPE, Multimap<LinkInfo.OPERATION, String>> _storedUnits;

        public PaintInfo(String poolId)
        {
            _poolId = poolId;

            Map<LinkInfo.UNIT_TYPE, Multimap<LinkInfo.OPERATION, String>> storedUnits =
                    new EnumMap<>(LinkInfo.UNIT_TYPE.class);
            for (LinkInfo.UNIT_TYPE unitType : CONSIDERED_UNIT_TYPES) {
                SetMultimap<LinkInfo.OPERATION, String> map =
                        MultimapBuilder.enumKeys(LinkInfo.OPERATION.class).hashSetValues().build();
                storedUnits.put(unitType, map);
            }

            _storedUnits = Collections.unmodifiableMap(storedUnits);
        }

        /**
         * Add paint information for a link. Pools accessible through
         * the same set of links are part of the same NAS.
         *
         * @param link The LinkInfo object that describes the link
         */
        synchronized void addLink(LinkInfo link)
        {
            invalidateNasNameCache();

            _links.add(link.getId());

            for (LinkInfo.OPERATION operation : CONSIDERED_OPERATIONS) {
                if (link.isAccessableFor(operation)) {
                    for (LinkInfo.UNIT_TYPE unitType : CONSIDERED_UNIT_TYPES) {
                        _storedUnits.get(unitType).putAll(operation, link.getUnits(unitType));
                    }
                }
            }
        }

        /**
         * Calculating the NAS name is a relatively heavy-weight
         * operation, so we cache the result.  However, sometimes we
         * need to invalidate this cache so calls to getNasName() will
         * generate the name afresh.
         */
        private void invalidateNasNameCache()
        {
            _nasName = null;
        }

        /**
         * Check whether the nasName cache is currently valid.
         */
        private boolean isNasNameCacheValid()
        {
            return _nasName != null;
        }

        /**
         *  Rebuild the nasName cached value.
         */
        private void buildNasNameCache()
        {
            String name = Joiner.on(",").join(Ordering.natural().sortedCopy(_links));

            if (name.length() > NAS_NAME_MAX_LENGTH) {
                _nasName = NAS_NAME_TOO_LONG_PREFIX + Integer.toHexString(name.hashCode());
            } else if (!name.isEmpty()) {
                _nasName = name;
            } else {
                _nasName = NAS_NAME_INACCESSIBLE;
            }
        }


        /**
         * Return the name of the NAS this painted pool should be
         * within.
         *
         * @return a unique name for the NAS this PaintInfo is
         *         representative of.
         */
        synchronized String getNasName()
        {
            if (!isNasNameCacheValid()) {
                buildNasNameCache();
            }

            return _nasName;
        }

        protected Set<String> getLinks()
        {
            return _links;
        }

        String getPoolId()
        {
            return _poolId;
        }

        /**
         * Obtain a Multimap between operations (such as read, write, ...) and the
         * units that select those operations for a given unitType
         * (such as dcache, store, ..)
         *
         * @param unitType a considered unit type.
         * @return the corresponding mapping or null if unit type isn't
         *         considered.
         */
        public Multimap<LinkInfo.OPERATION, String> getUnits(LinkInfo.UNIT_TYPE unitType)
        {
            if (!_storedUnits.containsKey(unitType)) {
                return null;
            }

            return Multimaps.unmodifiableMultimap(_storedUnits.get(unitType));
        }

        @Override
        public int hashCode()
        {
            return _links.hashCode();
        }

        @Override
        public boolean equals(Object otherObject)
        {
            if (this == otherObject) {
                return true;
            }

            if (!(otherObject instanceof PaintInfo)) {
                return false;
            }

            PaintInfo otherPI = (PaintInfo) otherObject;

            if (!_links.equals(otherPI._links)) {
                return false;
            }

            return true;
        }
    }

    /**
     * Information about a particular NAS. A NAS is something like a
     * poolgroup, but with the restriction that every pool is a member of
     * precisely one NAS.
     */
    static private class NasInfo
    {
        private final SpaceInfo _spaceInfo = new SpaceInfo();
        private final Set<String> _pools = new HashSet<>();
        private PaintInfo _representativePaintInfo;

        /**
         * Add a pool to this NAS. If this is the first pool then the set of
         * links store unit and dCache units is set. It is anticipated that
         * any subsequent pools added to this NAS will have the same set of
         * links (so, same set of store and dCache units). This is checked.
         *
         * @param poolId
         * @param spaceInfo
         * @param pInfo
         */
        void addPool(String poolId, SpaceInfo spaceInfo, PaintInfo pInfo)
        {
            _pools.add(poolId);
            _spaceInfo.add(spaceInfo);

            if (_representativePaintInfo == null) {
                _representativePaintInfo = pInfo;
            } else if (!_representativePaintInfo.equals(pInfo)) {
                throw new RuntimeException("Adding pool " + poolId +
                        " with differeing paintInfo from first pool " +
                        _representativePaintInfo.getPoolId());
            }
        }

        /**
         * Discover whether any of the pools given in the Set of poolIDs has
         * been registered as part of this NAS.
         *
         * @param pools the Set of PoolIDs
         * @return true if at least one member of the provided Set is a
         *         member of this NAS.
         */
        boolean havePoolInSet(Set<String> pools)
        {
            return !Collections.disjoint(pools, _pools);
        }

        /**
         * Add a set of metrics for this NAS
         *
         * @param update
         */
        void addMetrics(StateUpdate update, String nasName)
        {
            StatePath thisNasPath = NAS_PATH.newChild(nasName);

            // Add a list of pools in this NAS
            StatePath thisNasPoolsPath = thisNasPath.newChild("pools");
            update.appendUpdateCollection(thisNasPoolsPath, _pools, true);

            // Add the space information
            _spaceInfo.addMetrics(update, thisNasPath.newChild("space"), true);

            // Add the list of links
            StatePath thisNasLinksPath = thisNasPath.newChild("links");
            update.appendUpdateCollection(thisNasLinksPath,
                   _representativePaintInfo.getLinks(),
                   true);

            // Add the units information
            StatePath thisNasUnitsPath = thisNasPath.newChild("units");

            for (LinkInfo.UNIT_TYPE type : PaintInfo.CONSIDERED_UNIT_TYPES) {
                Multimap<LinkInfo.OPERATION, String> unitsMap =
                        _representativePaintInfo.getUnits(type);

                if (unitsMap == null) {
                    LOGGER.error("A considered unit-type query to getUnits() gave null reply.  This is unexpected.");
                    continue;
                }

                if (!UNIT_TYPE_STORAGE_NAME.containsKey(type)) {
                    LOGGER.error("Unmapped unit type {}", type);
                    continue;
                }

                StatePath thisNasUnitTypePath =
                        thisNasUnitsPath.newChild(UNIT_TYPE_STORAGE_NAME.get(type));

                for (Map.Entry<LinkInfo.OPERATION, Collection<String>> entry : unitsMap.asMap().entrySet()) {
                    LinkInfo.OPERATION operation = entry.getKey();
                    Collection<String> units = entry.getValue();

                    if (!OPERATION_STORAGE_NAME.containsKey(operation)) {
                        LOGGER.error("Unmapped operation {}", operation);
                        continue;
                    }

                    update.appendUpdateCollection(
                                                   thisNasUnitTypePath.newChild(OPERATION_STORAGE_NAME.get(operation)),
                                                   units, true);
                }
            }
        }
    }

    @Override
    protected String[] getPredicates()
    {
        return PREDICATE_PATHS;
    }

    @Override
    public void trigger(StateUpdate update, StateExhibitor currentState, StateExhibitor futureState)
    {
        Map<String, LinkInfo> currentLinks =
                LinkInfoVisitor.getDetails(currentState);
        Map<String, LinkInfo> futureLinks =
                LinkInfoVisitor.getDetails(futureState);

        Map<String, SpaceInfo> currentPools =
                PoolSpaceVisitor.getDetails(currentState);
        Map<String, SpaceInfo> futurePools =
                PoolSpaceVisitor.getDetails(futureState);

        buildUpdate(update, currentPools, futurePools, currentLinks,
                     futureLinks);
    }

    /**
     * Build a mapping of NasInfo objects.
     *
     * @param links
     */
    private Map<String, NasInfo> buildNas(Map<String, SpaceInfo> poolSpaceInfo,
            Map<String, LinkInfo> links)
    {
        // Build initially "white" (unpainted) set of paint info.
        Map<String, PaintInfo> paintedPools = new HashMap<>();
        for (String poolId : poolSpaceInfo.keySet()) {
            paintedPools.put(poolId, new PaintInfo(poolId));
        }

        // For each link in dCache and for each pool accessible via this link
        // build the paint info.
        for (LinkInfo linkInfo : links.values()) {
            for (String linkPool : linkInfo.getPools()) {
                PaintInfo poolPaintInfo = paintedPools.get(linkPool);

                /*
                 * It is possible that a pool is accessible from a link yet
                 * no such pool is known; for example, as the info service is
                 * "booting up".  We work-around this issue by creating a new
                 * PaintInfo for for this pool.
                 */
                if (poolPaintInfo == null) {
                    LOGGER.debug("Inconsistency in information: pool " +
                               linkPool + " accessible via link " +
                               linkInfo.getId() +
                               " but not present as a pool");
                    poolPaintInfo = new PaintInfo(linkPool);
                    paintedPools.put(linkPool, poolPaintInfo);
                }

                poolPaintInfo.addLink(linkInfo);
            }
        }

        // Build the set of NAS by iterating over all paint information (so,
        // iterating over all pools)
        Map<String, NasInfo> nas = new HashMap<>();
        for (Map.Entry<String, PaintInfo> paintEntry : paintedPools.entrySet()) {

            PaintInfo poolPaintInfo = paintEntry.getValue();
            String poolId = paintEntry.getKey();

            String nasName = poolPaintInfo.getNasName();
            NasInfo _thisNas;

            if (!nas.containsKey(nasName)) {
                _thisNas = new NasInfo();
                nas.put(nasName, _thisNas);
            } else {
                _thisNas = nas.get(nasName);
            }

            _thisNas.addPool(poolId, poolSpaceInfo.get(poolId), poolPaintInfo);
        }

        return nas;
    }

    /**
     * Build a StateUpdate with the metrics that need to be updated.
     *
     * @param update
     * @param existingLinks
     * @param futureLinks
     */
    private StateUpdate buildUpdate(StateUpdate update, Map<String, SpaceInfo> currentPools,
            Map<String, SpaceInfo> futurePools, Map<String, LinkInfo> currentLinks,
            Map<String, LinkInfo> futureLinks)
    {
        boolean buildAll = false;
        Set<String> alteredPools = null;
        Map<String, NasInfo> nas = buildNas(futurePools, futureLinks);

        /**
         * If the link structure has changed then we know that there may be
         * NAS that are no longer valid. To keep things simple, we invalidate
         * all stored NAS information and repopulate everything.
         */
        if (!currentLinks.equals(futureLinks)) {
            update.purgeUnder(NAS_PATH);
            buildAll = true;
        } else {
            /**
             * If the structure is the same, then we only need to update NAS
             * that contain a pool that has changed a space metric
             */
            alteredPools = identifyPoolsThatHaveChanged(currentPools, futurePools);
        }

        // Add those NAS that have changed, or all of them if buildAll is set
        for (Map.Entry<String, NasInfo> e : nas.entrySet()) {
            String nasName = e.getKey();
            NasInfo nasInfo = e.getValue();

            if (buildAll || nasInfo.havePoolInSet(alteredPools)) {
                nasInfo.addMetrics(update, nasName);
            }
        }

        return update;
    }

    /**
     * Build up a Set of pools that have altered; either pools that have been
     * added, that have been removed or have changed their details. More
     * succinctly, this is:
     *
     * <pre>
     * (currentPools \ futurePools) U (futurePools \ currentPools)
     * </pre>
     *
     * where the sets here are each Map's Map.EntrySet.
     *
     * @param currentPools Map between poolID and corresponding SpaceInfo for
     *            current pools
     * @param futurePools Map between poolID and corresponding SpaceInfo for
     *            future pools
     * @return a Set of poolIDs for pools that have, in some way, changed.
     */
    private Set<String> identifyPoolsThatHaveChanged(Map<String, SpaceInfo> currentPools,
            Map<String, SpaceInfo> futurePools)
    {
        Set<String> alteredPools = new HashSet<>();

        Set<Map.Entry<String, SpaceInfo>> d1 =
                new HashSet<>(currentPools.entrySet());
        Set<Map.Entry<String, SpaceInfo>> d2 =
                new HashSet<>(futurePools.entrySet());

        d1.removeAll(futurePools.entrySet());
        d2.removeAll(currentPools.entrySet());

        for (Map.Entry<String, SpaceInfo> e : d1) {
            alteredPools.add(e.getKey());
        }

        for (Map.Entry<String, SpaceInfo> e : d2) {
            alteredPools.add(e.getKey());
        }

        return alteredPools;
    }
}
