package org.dcache.services.info.secondaryInfoProviders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.PoolSpaceVisitor;
import org.dcache.services.info.stateInfo.SetMapVisitor;
import org.dcache.services.info.stateInfo.SpaceInfo;


/**
 * The PoolgroupSpaceWatcher Class implements StateWatcher.  It is responsible for maintaining
 * the space information of poolgroups.  It is triggered whenever pool space information or
 * a pool's membership of a poolgroup changes.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class PoolgroupSpaceWatcher extends AbstractStateWatcher {

    private static Logger _log = LoggerFactory.getLogger(PoolgroupSpaceWatcher.class);
    private static final String PREDICATE_PATHS[] = { "pools.*.space.*",
            "poolgroups.*",
            "poolgroups.*.pools.*"};
    private static final StatePath POOLGROUPS_PATH = new StatePath("poolgroups");
    private static final StatePath POOL_MEMBERSHIP_REL_PATH = new StatePath("pools");

    @Override
    protected String[] getPredicates() {
        return PREDICATE_PATHS;
    }


    @Override
    public void trigger(StateUpdate update, StateExhibitor currentState, StateExhibitor futureState) {
        super.trigger(update, currentState, futureState);

        Set<String> recalcPoolgroup = new HashSet<>();
        if (_log.isInfoEnabled()) {
            _log.info("Watcher " + this.getClass()
                    .getSimpleName() + " triggered");
        }

        _log.debug("Gathering state:");
        _log.debug("  building current poolgroup membership.");
        Map <String,Set<String>> currentPoolgroupMembership = SetMapVisitor.getDetails(currentState, POOLGROUPS_PATH, POOL_MEMBERSHIP_REL_PATH);

        _log.debug("  building future poolgroup membership.");
        Map <String,Set<String>> futurePoolgroupMembership = SetMapVisitor.getDetails(futureState, POOLGROUPS_PATH, POOL_MEMBERSHIP_REL_PATH);

        _log.debug("  establishing current pool space mapping.");
        Map<String, SpaceInfo> poolSpaceInfoPre = PoolSpaceVisitor.getDetails(currentState);

        _log.debug("  establishing future pool space mapping.");
        Map<String, SpaceInfo> poolSpaceInfoPost = PoolSpaceVisitor.getDetails(futureState);

        _log.debug("Looking for changes in poolgroup membership.");
        updateTodoBasedOnMembership(recalcPoolgroup, currentPoolgroupMembership, futurePoolgroupMembership);

        _log.debug("Looking for changes in pool space information.");
        updateTodoBasedOnPoolSpace(recalcPoolgroup, futurePoolgroupMembership, poolSpaceInfoPre, poolSpaceInfoPost);

        if (recalcPoolgroup.size() == 0) {
            if (_log.isDebugEnabled()) {
                _log.debug("No poolgroups need updating");
            }
            return;
        }

        for (String thisPoolgroup : recalcPoolgroup) {
            if (_log.isDebugEnabled()) {
                _log.debug("Updating poolgroup " + thisPoolgroup);
            }

            StatePath thisPgPath = POOLGROUPS_PATH.newChild(thisPoolgroup);

            buildNewMetrics(update, thisPgPath.newChild("space"), futurePoolgroupMembership.get(thisPoolgroup), poolSpaceInfoPost);
        }
    }


    /**
     * Create new metrics that update information about the named poolgroup.
     * @param update the StateUpdate to which the new metric values will be appended.
     * @param path the StatePath under which the space information will be added.
     * @param pools the Set of pools this poolgroup has within its membership.
     * @param poolsSpaceInfo the mapping between pools and their SpaceInfo.
     */
    private void buildNewMetrics(StateUpdate update, StatePath path, Set<String> pools, Map<String, SpaceInfo> poolsSpaceInfo) {
        SpaceInfo pgSpaceInfo = new SpaceInfo();

        // Create an Ephemeral StateComposite for our data.
        update.appendUpdate(path, new StateComposite());

        if (pools != null) {
            for (String thisPool : pools) {
                SpaceInfo poolSpace = poolsSpaceInfo.get(thisPool);

                if (poolSpace != null) {
                    pgSpaceInfo.add(poolSpace);
                }
            }
        }

        pgSpaceInfo.addMetrics(update, path, false);

        if (_log.isDebugEnabled()) {
            _log.debug("  new info: " + pgSpaceInfo.toString());
        }
    }



    /**
     * Add poolgroups to the to-be-recalculated Set if the pool membership of a poolgroup has
     * changed, or the poolgroup is new.
     * @param recalcPoolgroup
     * @param transition
     */
    private void updateTodoBasedOnMembership(Set<String> recalcPoolgroup, Map <String,Set<String>> currentPoolgroupMembership, Map <String,Set<String>> futurePoolgroupMembership)
    {
        // Scan (future) poolgroup membership..
        for (Map.Entry<String, Set<String>> pgEntry : futurePoolgroupMembership.entrySet()) {
            String thisPoolgroup = pgEntry.getKey();
            Set<String> thisPoolgroupFuturePoolset = pgEntry.getValue();
            if (_log.isDebugEnabled()) {
                _log.debug("  examining poolgroup: " + thisPoolgroup);
            }

            Set<String> thisPoolgroupCurrentPoolset = currentPoolgroupMembership.get(thisPoolgroup);

            // If poolgroup is new or membership isn't the same as it was...
            if ((thisPoolgroupCurrentPoolset == null) || !thisPoolgroupCurrentPoolset.equals(thisPoolgroupFuturePoolset)) {
                recalcPoolgroup.add(thisPoolgroup);

                if (_log.isDebugEnabled()) {
                    _log.debug("    poolgroup "+ thisPoolgroup + " is new or has altered membership");

                    StringBuilder wasSb = new StringBuilder();
                    if (thisPoolgroupCurrentPoolset != null) {
                        for (String pool : thisPoolgroupCurrentPoolset) {
                            if (wasSb.length() > 0) {
                                wasSb.append(", ");
                            }
                            wasSb.append(pool);
                        }
                        if (wasSb.length() == 0) {
                            wasSb.append("<empty>");
                        }
                    } else {
                        wasSb.append("<unknown>");
                    }

                    _log.debug("      was: " + wasSb.toString());

                    StringBuilder nowSb = new StringBuilder();
                    if (thisPoolgroupFuturePoolset != null) {
                        for (String pool : thisPoolgroupFuturePoolset) {
                            if (nowSb.length() > 0) {
                                nowSb.append(", ");
                            }
                            nowSb.append(pool);
                        }
                        if (nowSb.length() == 0) {
                            nowSb.append("<empty>");
                        }
                    } else {
                        nowSb.append("<unknown>");
                    }
                    _log.debug("      now: " + nowSb.toString());
                }
            }
        }
    }


    /**
     * Look for changes in the pool size.  Update any poolgroup this pool is a member of.
     * @param recalcPoolgroup
     * @param transition
     */
    private void updateTodoBasedOnPoolSpace(Set<String> recalcPoolgroup,
            Map <String,Set<String>> futurePoolgroupMembership,
            Map<String, SpaceInfo> currentPoolSpaceInfo, Map<String, SpaceInfo> futurePoolSpaceInfo)
    {
        Set<String> changedPools = new HashSet<>();

        // 1. Build list of pools that have changed:

        // 1. a) disappeared or changed value.
        for (Map.Entry<String, SpaceInfo> preInfoEntry : currentPoolSpaceInfo.entrySet()) {
            String thisPool = preInfoEntry.getKey();

            SpaceInfo thisPoolPreInfo = preInfoEntry.getValue();
            SpaceInfo thisPoolPostInfo = futurePoolSpaceInfo.get(thisPool);

            if (_log.isDebugEnabled()) {
                _log.debug("  examining pool: " + thisPool);
            }

            // Pool has disappeared or size has changed
            if (thisPoolPostInfo == null || !thisPoolPostInfo.equals(thisPoolPreInfo)) {
                changedPools.add(thisPool);
                if (_log.isDebugEnabled()) {
                    _log.debug("    pool " + thisPool + " has changed or disappeared");
                }
            }
        }

        // 1. b) new pools have appeared.
        for (String thisPool : futurePoolSpaceInfo.keySet()) {
            if (!currentPoolSpaceInfo.containsKey(thisPool)) {
                changedPools.add(thisPool);

                if (_log.isDebugEnabled()) {
                    _log.debug("    pool " + thisPool + " has appeared");
                }
            }
        }

        // 2.  Nothing doing?, do nothing more.
        if (changedPools.size() == 0) {
            if (_log.isDebugEnabled()) {
                _log.debug("  no pools have changed");
            }
            return;
        }

        // 3.  Build the (future) reverse map: pool to Set of Poolgroups.
        Map<String,Set<String>> poolToPoolgroups = new HashMap<>();

        for (Map.Entry<String, Set<String>> pgPoolEntry : futurePoolgroupMembership.entrySet()) {
            String thisPoolgroup = pgPoolEntry.getKey();

            for (String thisPool : pgPoolEntry.getValue()) {

                Set<String> thisPoolPg = poolToPoolgroups.get(thisPool);
                if (thisPoolPg == null) {
                    thisPoolPg = new HashSet<>();
                    poolToPoolgroups.put(thisPool, thisPoolPg);
                }

                thisPoolPg.add(thisPoolgroup);
            }
        }

        // 4.  Mark each Poolgroup (that has a changed pool as a member) as to-be-updated.
        for (String pool : changedPools) {
            Set<String> poolgroupSet = poolToPoolgroups.get(pool);

            if (poolgroupSet == null) {
                continue; // skip if pool is not a member of any poolgroup
            }

            for (String poolgroup : poolgroupSet) {
                recalcPoolgroup.add(poolgroup);
                if (changedPools.size() == 0) {
                    if (_log.isDebugEnabled()) {
                        _log.debug("  poolgroup " + poolgroup + " is marked as to be recalculated.");
                    }

                    return;
                }
            }
        }
    }
}
