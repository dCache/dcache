package org.dcache.services.info.gathers.cells;

import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import java.util.concurrent.TimeUnit;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.gathers.MessageSender;
import org.dcache.services.info.gathers.SkelListBasedActivity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CellInfoDga extends SkelListBasedActivity {

    private static final Logger LOGGER = LoggerFactory.getLogger(CellInfoDga.class);

    private final MessageSender _sender;

    /**
     * Use our own list timings.  Enforce a minimum delay of two minutes between successive
     * getcellinfos requests to the *same* domain, and a delay of at least 100 ms between successive
     * requests of information from any domain.
     */
    private static final long MIN_LIST_REFRESH_PERIOD = TimeUnit.MINUTES.toMillis(2);
    private static final long SUCC_MSG_DELAY = TimeUnit.SECONDS.toMillis(2);

    private final CellMessageAnswerable _handler;

    public CellInfoDga(StateExhibitor exhibitor, MessageSender sender,
          CellMessageAnswerable handler) {
        super(exhibitor, new StatePath("domains"), MIN_LIST_REFRESH_PERIOD, SUCC_MSG_DELAY);

        _handler = handler;
        _sender = sender;
    }

    /**
     * Method called periodically when we should send out a message.
     */
    @Override
    public void trigger() {
        super.trigger();

        String domainName = getNextItem();

        // This can happen, indicating that there's nothing to do.
        if (domainName == null) {
            return;
        }

        CellPath systemCellPath = new CellPath("System", domainName);

        LOGGER.info("sending message getcellinfos to System cell on domain {}", domainName);

        _sender.sendMessage(getMetricLifetime(), _handler, systemCellPath,
                listRefreshPeriod(), "getcellinfos");
    }

    /**
     * We only expect to have a single instance of this class.
     */
    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
