package org.dcache.services.info.secondaryInfoProviders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.SimpleIntegerMapVisitor;
import org.dcache.services.info.stateInfo.SimpleStringMapVisitor;

/**
 * The SpaceManager records some capacity information about a linkgroup; specifically,
 * just the Free and Reserved spaces.   The Total and Used sizes are not maintained, so
 * must be calculated from other information.
 * <p>
 * Used space for a linkgroup is the sum of all used space within corresponding
 * SRM space reservations.  Total space is the sum of Free- and Used- space in the linkgroup.
 * <p>
 * This StateWatcher maintains a linkgroup's Total and Used space metrics by triggering
 * when these values change.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkgroupTotalSpaceMaintainer extends AbstractStateWatcher {

    private static Logger _log = LoggerFactory.getLogger(LinkgroupTotalSpaceMaintainer.class);

    private static final StatePath RESERVATIONS = new StatePath("reservations");
    private static final StatePath LINKGROUPS = new StatePath("linkgroups");
    private static final StatePath LINKGROUPREF = new StatePath("linkgroupref");
    private static final StatePath SPACE_USED = StatePath.parsePath("space.used");
    private static final StatePath SPACE_FREE = StatePath.parsePath("space.free");

    private static final String PREDICATE_PATHS[] = { "reservations.*.space.used",
            "linkgroups.*.space.free",
            "linkgroups.*.reservations.*"};

    /**
     * Provide a list of the paths we're interested in.
     */
    @Override
    protected String[] getPredicates() {
        return PREDICATE_PATHS;
    }

    @Override
    public void trigger(StateUpdate update, StateExhibitor currentState, StateExhibitor futureState) {
        super.trigger(update, currentState, futureState);

        if (_log.isInfoEnabled()) {
            _log.info("Watcher " + this.getClass()
                    .getSimpleName() + " triggered");
        }

        // Build a mapping of how linkgroup-IDs map to the corresponding space.free metric
        Map<String,Long> freeSpaceAfter = SimpleIntegerMapVisitor.buildMap(futureState, LINKGROUPS, SPACE_FREE);

        // Build a mapping of how reservation-IDs map to space.used metric
        Map<String,Long> usedSpaceAfter = SimpleIntegerMapVisitor.buildMap(futureState, RESERVATIONS, SPACE_USED);

        // Build a mapping of reservation-IDs to linkgroup-IDs
        Map<String,String> reservationToLinkgroup = SimpleStringMapVisitor.buildMap(futureState, RESERVATIONS, LINKGROUPREF);

        Set<String> linkgroupsToUpdate = new HashSet<>();

        // Update our list of linkgroups to update
        addLinkgroupsWhereUsedSpaceChanged(currentState, linkgroupsToUpdate, usedSpaceAfter, reservationToLinkgroup);
        addLinkgroupsWhereFreeChanged(currentState, linkgroupsToUpdate, freeSpaceAfter);

        if (linkgroupsToUpdate.isEmpty()) {
            // This should never happen!
            _log.warn(this.getClass().getSimpleName()+" triggered, but apparently nothing needs doing.");
            return;
        }

        addLinkgroupChanges(update, linkgroupsToUpdate, freeSpaceAfter, usedSpaceAfter, reservationToLinkgroup);
    }


    /**
     * Provide a the set of linkgroup-IDs where at least one reservation has changed its Used
     * space metric.
     */
    private void addLinkgroupsWhereUsedSpaceChanged(StateExhibitor currentState,
            Set<String> linkgroupsToUpdate,
            Map<String,Long> usedSpaceAfter,
            Map<String,String> reservationToLinkgroup) {
        // Build map of the current reservation-ID to space.used metric
        Map<String,Long> usedSpaceNow = SimpleIntegerMapVisitor.buildMap(currentState, RESERVATIONS, SPACE_USED);

        if (usedSpaceNow.equals(usedSpaceAfter)) {
            if (_log.isDebugEnabled()) {
                _log.debug("No update needed for reservation used space changing.");
            }
            return;
        }

        if (_log.isDebugEnabled()) {
            _log.debug("Updates due to reservation used space changing:");
        }

        // Add the corresponding linkgroup for each space that has changed.
        for (String reservationId : usedSpaceAfter.keySet()) {
            if (!usedSpaceAfter.get(reservationId).equals(usedSpaceNow.get(reservationId))) {
                String linkgroupId = reservationToLinkgroup.get(reservationId);

                if (_log.isDebugEnabled()) {
                    _log.debug("    linkgroup: " + linkgroupId);
                }

                linkgroupsToUpdate.add(linkgroupId);
            }
        }
    }


    /**
     * Provide the Set of linkgroup-IDs where the linkgroup's Free space metric will change.
     * @param transition the StateTransition under consideration.
     * @return the Set of linkgroup-IDs where the Free space metric will change.
     */
    private void addLinkgroupsWhereFreeChanged(StateExhibitor currentState,
            Set<String> linkgroupsToUpdate,
            Map<String, Long> freeSpaceAfter) {

        // Build map from linkgroup-ID to used metric, both now and after.
        Map<String,Long> freeSpaceNow = SimpleIntegerMapVisitor.buildMap(currentState, LINKGROUPS, SPACE_USED);

        if (freeSpaceNow.equals(freeSpaceAfter)) {
            if (_log.isDebugEnabled()) {
                _log.debug("No update needed for linkgroup free space changing.");
            }

            return;
        }

        if (_log.isDebugEnabled()) {
            _log.debug("Updates due to linkgroup free space changing:");
        }

        for (String linkgroupId : freeSpaceAfter.keySet()) {
            if (!freeSpaceAfter.get(linkgroupId).equals(freeSpaceNow.get(linkgroupId))) {
                if (_log.isDebugEnabled()) {
                    _log.debug("    linkgroup: " + linkgroupId);
                }
                linkgroupsToUpdate.add(linkgroupId);
            }
        }
    }

    private void addLinkgroupChanges(StateUpdate update, Set<String> linkgroupsToUpdate,
            Map<String,Long> freeStateAfter, Map<String,Long> usedSpaceAfter,
            Map<String,String> reservationToLinkgroup) {

        for (String linkgroupId : linkgroupsToUpdate) {

            if (_log.isDebugEnabled()) {
                _log.debug("Building update for linkgroup " + linkgroupId);
            }

            StatePath thisLinkgroupSpace = LINKGROUPS.newChild(linkgroupId).newChild("space");

            /**
             *  Update the new space.used metric for this linkgroup.
             */
            long used = 0;

            for (Map.Entry<String, Long> entry : usedSpaceAfter.entrySet()) {
                if (linkgroupId.equals(reservationToLinkgroup.get(entry.getKey()))) {
                    used += entry.getValue();
                }
            }

            update.appendUpdate(thisLinkgroupSpace.newChild("used"), new IntegerStateValue(used));

            /**
             *  Try to update the space.total metric for this linkgroup, if we don't yet know this
             *  linkgroup's free metric then this will not be possible.
             */
            Long freeLong = freeStateAfter.get(linkgroupId);

            if (freeLong != null) {
                update.appendUpdate(thisLinkgroupSpace.newChild("total"),
                        new IntegerStateValue(used + freeLong));
            } else {
                if (_log.isDebugEnabled()) {
                    _log.debug("failed to find linkgroup " + linkgroupId + " in freeStateAfter");
                }
            }
        }
    }
}
