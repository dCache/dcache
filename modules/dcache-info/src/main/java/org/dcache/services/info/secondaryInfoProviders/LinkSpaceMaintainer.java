package org.dcache.services.info.secondaryInfoProviders;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.stateInfo.PoolSpaceVisitor;
import org.dcache.services.info.stateInfo.SetMapVisitor;
import org.dcache.services.info.stateInfo.SpaceInfo;

/**
 * This StateWatcher maintains the sizes for a link by adding together the
 * different spaces for a link's pools.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkSpaceMaintainer extends AbstractStateWatcher
{
    private static final String PREDICATE_PATHS[] = { "links.*",
            "links.*.pools.*",
            "pools.*.space.*"};

    private static final StatePath LINKS_PATH = new StatePath("links");
    private static final StatePath POOL_MEMBERSHIP_REL_PATH = new StatePath("pools");

    @Override
    protected String[] getPredicates()
    {
        return PREDICATE_PATHS;
    }

    @Override
    public void trigger(StateUpdate update, StateExhibitor currentState,
            StateExhibitor futureState)
    {
        super.trigger(update, currentState, futureState);

        Map<String,Set<String>> futureLinksToPools = SetMapVisitor.getDetails(futureState, LINKS_PATH, POOL_MEMBERSHIP_REL_PATH);
        Map<String,SpaceInfo> futurePoolSize = PoolSpaceVisitor.getDetails(futureState);

        Set<String> linksToUpdate = new HashSet<>();

        addLinksWhereLinkHasChanged(currentState, linksToUpdate, futureLinksToPools);
        addLinksWherePoolsHaveChanged(currentState, linksToUpdate, futureLinksToPools, futurePoolSize);

        if (linksToUpdate.isEmpty()) {
            return;
        }

        for (String linkName : linksToUpdate) {
            addUpdateInfo(update, linkName, futureLinksToPools
                    .get(linkName), futurePoolSize);
        }
    }


    /**
     * Check whether we should recalculate a link's available space based on information within the links
     * branch of the dCache state tree.
     * @param linksToUpdate the Set of links to recalculate: link names will be added here.
     * @param futureLinksToPools the mapping between a link's name and the Set of pools that are associated with that link after the transition.
     */
    private void addLinksWhereLinkHasChanged(StateExhibitor currentState,
            Set<String> linksToUpdate, Map<String,Set<String>> futureLinksToPools)
    {
        Map<String,Set<String>> currentLinksToPools = SetMapVisitor.getDetails(currentState, LINKS_PATH, POOL_MEMBERSHIP_REL_PATH);

        if (currentLinksToPools.equals(futureLinksToPools)) {
            return;
        }

        // Iterate over all future links.
        for (Map.Entry<String, Set<String>> linkEntry : futureLinksToPools.entrySet()) {

            String thisLinkName = linkEntry.getKey();

            // If this link already exists ...
            if (currentLinksToPools.containsKey(thisLinkName)) {

                Set<String> thisLinkPools = linkEntry.getValue();

                // If the pool membership has changed, add it.
                if (!thisLinkPools.equals(currentLinksToPools.get(thisLinkName))) {
                    linksToUpdate.add(thisLinkName);
                }
            } else {
                // Otherwise, it's a new link, so add it.
                linksToUpdate.add(thisLinkName);
            }
        }
    }


    /**
     * Check whether any of the pool spaces for pools associated with a link have changed.
     * If they have, we should make sure we update our link's summary view.
     * @param linksToUpdate the Set of link names of which links to update.
     * @param futureLinksToPools the future mapping between a link's name and the set of pools associated with that link
     * @param futurePoolSize the future mapping between a pool's name and its size information.
     */
    private void addLinksWherePoolsHaveChanged(StateExhibitor currentState,
            Set<String> linksToUpdate, Map<String, Set<String>> futureLinksToPools,
            Map<String,SpaceInfo> futurePoolSize)
    {
        // Get the current pool space information
        Map<String,SpaceInfo> currentPoolSize = PoolSpaceVisitor.getDetails(currentState);

        if (futurePoolSize.equals(currentPoolSize)) {
            return;
        }

        // Iterate over all links, looking to see if any of the pools have changed.
        for (Map.Entry<String, Set<String>> linkEntry : futureLinksToPools.entrySet()) {

            String thisLinkName = linkEntry.getKey();
            Set<String> thisLinkPools = linkEntry.getValue();

            // Now, for each pool associated with this link ...
            for (String poolName : thisLinkPools) {

                // If the pool currently exists in dCache state, check whether the space has changed.
                if (currentPoolSize.containsKey(poolName)) {

                    // If the space information has changed, add the link.
                    if (!currentPoolSize.get(poolName).equals(futurePoolSize.get(poolName))) {
                        linksToUpdate.add(thisLinkName);
                    }

                } else {
                    // Otherwise, the pool is new with this transition, so we should recalculate.
                    linksToUpdate.add(thisLinkName);
                }
            }
        }
    }


    /**
     * Add new metrics to a StateUpdate for freshly calculated updates.
     * @param update the StateUpdate the new metrics will be appended to.
     * @param linkName the name of the link.
     * @param futureLinksToPools the Set of poolNames for the pools associated with this link
     * @param futurePoolSize the future mapping between a pool's name and its pool size information.
     */
    private void addUpdateInfo(StateUpdate update, String linkName,
            Set<String> futureLinkPools, Map<String,SpaceInfo> futurePoolSize)
    {
        SpaceInfo linkSpaceInfo = new SpaceInfo();
        StatePath linkSpacePath = LINKS_PATH.newChild(linkName).newChild("space");

        // For each pool, add up the space information.
        for (String poolName : futureLinkPools) {
            linkSpaceInfo.add(futurePoolSize.get(poolName));
        }

        // Add the metrics to the StateUpdate.
        linkSpaceInfo.addMetrics(update, linkSpacePath, false);
    }
}
