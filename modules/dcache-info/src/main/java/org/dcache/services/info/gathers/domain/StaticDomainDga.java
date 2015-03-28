package org.dcache.services.info.gathers.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.SkelListBasedActivity;

/**
 * Activity to periodically query the System cell in each known domain by
 * issuing the ASCII command "show context info.static".  The returned message
 * containing the result is parsed by the CellMessageAnswerable supplied.
 */
public class StaticDomainDga extends SkelListBasedActivity
{
    private static final Logger _log = LoggerFactory.getLogger(StaticDomainDga.class);
    private static final StatePath DOMAINS_PATH = StatePath.parsePath("domains");
    private static final String COMMAND = "show context info.static";

    private final MessageSender _sender;
    private final CellMessageAnswerable _handler;

    /**
     * Minimum of two minutes between successive calls to the same domain, and
     * a delay of at least 100 ms between successive requests of
     * information from any domain.
     */
    private static final int MIN_LIST_REFRESH_PERIOD = 120000;
    private static final int SUCC_MSG_DELAY = 100;

    public StaticDomainDga(StateExhibitor exhibitor, MessageSender sender,
            CellMessageAnswerable handler)
    {
        super(exhibitor, DOMAINS_PATH, MIN_LIST_REFRESH_PERIOD, SUCC_MSG_DELAY);
        _sender = sender;
        _handler = handler;
    }

    @Override
    public void trigger()
    {
        super.trigger();

        String domain = getNextItem();

        if (domain != null) {
            CellPath path = new CellPath("System", domain);

            _log.debug("sending message \"{}\" to System cell on domain {}",
                    COMMAND, domain);

            _sender.sendMessage(getMetricLifetime(), _handler, path,
                    COMMAND);
        }
    }


    @Override
    public String toString()
    {
        return this.getClass().getSimpleName();
    }
}
