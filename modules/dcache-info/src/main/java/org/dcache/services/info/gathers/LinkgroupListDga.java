package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.services.space.message.GetLinkGroupNamesMessage;

import dmg.cells.nucleus.CellPath;

/**
 * This class should really be folded into SingleMessageDga, but unfortunately
 * the Message class doesn't have a clone() method.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkgroupListDga extends SkelPeriodicActivity
{
    private static final Logger LOGGER = LoggerFactory.getLogger(LinkgroupListDga.class);
    private static final String SRM_CELL_NAME = "SpaceManager";

    /** Assume that a message might be lost and allow for 50% jitter */
    private static final double SAFETY_FACTOR = 2.5;

    private CellPath _cp = new CellPath(SRM_CELL_NAME);
    private final MessageHandlerChain _mhc;

    /** The period between successive requests for data, in seconds */
    long _metricLifetime;

    /**
     * Create new DGA for maintaining a list of LinkGroups.
     * @param interval how often the list of linkgroups should be updated, in seconds.
     */
    public LinkgroupListDga(int interval, MessageHandlerChain mhc)
    {
        super(interval);

        _mhc = mhc;
        _metricLifetime = Math.round(interval * SAFETY_FACTOR);
    }

    /**
     * When triggered, send a message.
     */
    @Override
    public void trigger()
    {
        super.trigger();
        LOGGER.trace("Sending linkgroup list request message");
        _mhc.sendMessage(_metricLifetime, _cp, new GetLinkGroupNamesMessage());
    }


    @Override
    public String toString()
    {
        return this.getClass().getSimpleName();
    }
}
