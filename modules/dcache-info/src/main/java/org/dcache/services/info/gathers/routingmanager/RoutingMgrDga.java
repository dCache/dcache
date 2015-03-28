package org.dcache.services.info.gathers.routingmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.SkelListBasedActivity;

/**
 * The RoutingMgrDga queries the RoutingMgr cell running on each domain that
 * the dCache info knows about.  This is achieved by iterating over the list
 * located at StatePath: "domains".
 * <p>
 * For each list item, it issues the "ls -x" command.  This requests that the
 * RoutingMgr replies with its current knowledge of routing as a three-item
 * array.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class RoutingMgrDga extends SkelListBasedActivity
{
    private static final Logger _log = LoggerFactory.getLogger(RoutingMgrDga.class);

    private final MessageSender _sender;

    /**
     *  Use our own list timings.  Enforce a minimum delay of 5 minutes between successive
     *  "ls -x" requests to the *same* domain, and a delay of at least 100 ms between
     *  successive requests of information from any domain.
     */
    private static int MIN_LIST_REFRESH_PERIOD = 300000;
    private static int SUCC_MSG_DELAY = 100;

    private final CellMessageAnswerable _handler;

    public RoutingMgrDga(StateExhibitor exhibitor, MessageSender sender,
            CellMessageAnswerable handler)
    {
        super(exhibitor, new StatePath("domains"), MIN_LIST_REFRESH_PERIOD, SUCC_MSG_DELAY);

        _sender = sender;
        _handler = handler;
    }

    /**
     * Method called periodically when we should send out a message.
     */
    @Override
    public void trigger()
    {
        super.trigger();

        String domainName = getNextItem();

        // This can happen, indicating that there's nothing to do.
        if (domainName == null) {
            return;
        }

        /**
         * In principle, we should check that this domain has a RoutingMgr
         * cell.  However, in practice, all domains do.  If a domain doesn't
         * we'll throw a (relatively harmless) exception.
         */

        CellPath routingMgrCellPath = new CellPath("RoutingMgr", domainName);

        if (_log.isInfoEnabled()) {
            _log.info("sending message \"ls -x\" to RoutingMgr cell on domain " + domainName);
        }

        _sender.sendMessage(getMetricLifetime(), _handler, routingMgrCellPath, "ls -x");
    }

    /**
     * We only expect to have a single instance of this class.
     */
    @Override
    public String toString()
    {
        return this.getClass().getSimpleName();
    }
}
