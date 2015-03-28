package org.dcache.services.info.stateInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.services.info.base.StateExhibitor;

/**
 * Scan through the current list of pools and calculate aggregated statistics.
 */
public class PoolSummaryVisitor extends AbstractPoolSpaceVisitor
{
    private static final Logger _log = LoggerFactory.getLogger(PoolSummaryVisitor.class);

    /**
     * Obtain some summary statistics about all available pools.
     * @return the aggregated information about the pools.
     */
    static public SpaceInfo getDetails(StateExhibitor exhibitor)
    {
        if (_log.isDebugEnabled()) {
            _log.debug("Gathering summary information.");
        }

        PoolSummaryVisitor visitor = new PoolSummaryVisitor();
        exhibitor.visitState(visitor);
        return visitor._summaryInfo;
    }

    private final SpaceInfo _summaryInfo = new SpaceInfo();

    @Override
    protected void newPool(String poolName, SpaceInfo space)
    {
        _summaryInfo.add(space);
    }
}
