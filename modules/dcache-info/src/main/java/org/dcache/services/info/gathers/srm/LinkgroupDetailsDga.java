package org.dcache.services.info.gathers.srm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import diskCacheV111.services.space.message.GetLinkGroupsMessage;

import dmg.cells.nucleus.CellPath;

import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.SkelPeriodicActivity;

/**
 * Class to send off requests for detailed information.
 * <p>
 * Ideally, this would request information about a single, specific LinkGroup (or perhaps a
 * few linkgroups) in the Message.  However, that functionality hasn't been implemented yet.
 * <p>
 * Until it has, we query all information at the same time.  Since this might be a heavy-weight
 * operation (due to serialiastion), we try not to do it too often.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class LinkgroupDetailsDga extends SkelPeriodicActivity {

    private static Logger _log = LoggerFactory.getLogger(LinkgroupDetailsDga.class);

    private static final String SRM_CELL_NAME = "SpaceManager";

    /** Assume that a message might be lost and allow for 50% jitter */
    private static final double SAFETY_FACTOR = 2.5;

    private CellPath _cp = new CellPath(SRM_CELL_NAME);
    private final MessageSender _sender;

    /** The period between successive requests for data, in seconds */
    final long _metricLifetime;

    /**
     * Create new DGA for maintaining a list of LinkGroups.
     * @param interval how often the list of linkgroups should be updated, in seconds.
     */
    public LinkgroupDetailsDga(MessageSender sender, int interval) {
        super(interval);
        _sender = sender;
        _metricLifetime = Math.round(interval * SAFETY_FACTOR);
    }

    /**
     * When triggered, send a message.
     */
    @Override
    public void trigger() {
        super.trigger();

        if (_log.isInfoEnabled()) {
            _log.info("Sending linkgroup details request message");
        }

        _sender.sendMessage(_metricLifetime, _cp, new GetLinkGroupsMessage());
    }


    @Override
    public String toString()
    {
        return this.getClass().getSimpleName();
    }
}
