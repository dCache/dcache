package org.dcache.services.info.gathers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.NoRouteToCellException;
import dmg.cells.nucleus.UOID;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;
import org.dcache.services.info.base.StateUpdateManager;
import org.dcache.services.info.base.StringStateValue;


/**
 * This Class introduces a number of useful utilities common to all
 * CellMessageHandler parsing implementations.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public abstract class CellMessageHandlerSkel implements CellMessageAnswerable
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CellMessageHandlerSkel.class);

    private static final String SIMPLE_DATE_FORMAT = "MMM d, HH:mm:ss z";
    private static final String ISO_8601_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm'Z'";

    /**
     * Adds a standard set of metrics that represent some point in time.  We add three metrics that
     * are (in essence) the same date to allow dumb clients (e.g., xslt) to select which one makes
     * sense to them.
     * @param update  The StateUpdate the three additional metrics will be added to.
     * @param parentPath the StatePath of the parent branch.
     * @param theTime the Date describing the time to record.
     * @param lifetime how long, in seconds, the metric should last.
     */
    protected static void addTimeMetrics(StateUpdate update, StatePath parentPath,
            Date theTime, long lifetime)
    {
        // Supply time as seconds since 1970
        update.appendUpdate(parentPath.newChild("unix"),
                new IntegerStateValue(theTime.getTime() / 1000, lifetime));

        DateFormat simpleDateFormat = new SimpleDateFormat(SIMPLE_DATE_FORMAT);
        DateFormat iso8601DateFormat = new SimpleDateFormat(ISO_8601_DATE_FORMAT);
        iso8601DateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));

        // Supply the time in a simple format
        update.appendUpdate(parentPath.newChild("simple"),
                new StringStateValue(simpleDateFormat.format(theTime), lifetime));

        // Supply the time in UTC in a standard format
        update.appendUpdate(parentPath.newChild("ISO-8601"),
                new StringStateValue(iso8601DateFormat.format(theTime), lifetime));
    }


    private final MessageMetadataRepository<UOID> _msgMetadataRepo;
    private final StateUpdateManager _sum;

    private String _domain;

    public CellMessageHandlerSkel(StateUpdateManager sum,
            MessageMetadataRepository<UOID> msgHandlerChain)
    {
        _sum = sum;
        _msgMetadataRepo = msgHandlerChain;
    }

    /**
     *  Process the information payload.  The metricLifetime gives how long
     *  the metrics should last, in seconds.
     *
     *  We guarantee that msgPayload is never null and is never instanceof Exception.
     */
    public abstract void process(Object msgPayload, long metricLifetime);


    /**
     * Build a list of items under a specific path.  These are recorded as
     * StateComposites (branch nodes).
     * @param update the StateUpdate to append
     * @param parentPath the StatePath pointing to the parent of these items
     * @param items an array of items.
     * @param metricLifetime how long the metric should last, in seconds.
     */
    protected void addItems(StateUpdate update, StatePath parentPath,
            Object[] items, long metricLifetime)
    {
        LOGGER.trace("appending list-items under {}", parentPath);

        for (Object item : items) {
            String listItem = (String) item;

            LOGGER.trace("    adding item {}", listItem);

            update.appendUpdate(parentPath
                    .newChild(listItem), new StateComposite(metricLifetime));
        }
    }


    /**
     * Send a StateUpdate object to our State singleton.  If we get this wrong, log this
     * fact somewhere.
     * @param update the StateUpdate to apply to the state tree.
     */
    protected void applyUpdates(StateUpdate update)
    {
        LOGGER.trace("adding update to state's to-do stack with {} updates for {}",
                update.count(), getClass().getSimpleName());
        _sum.enqueueUpdate(update);
    }


    /**
     * The following methods are needed for CellMessageAnswerable.
     */

    /**
     * Incoming message: look it up and call the (abstract) process() method.
     */
    @Override
    public void answerArrived(CellMessage request, CellMessage answer)
    {
        Object payload = answer.getMessageObject();

        if (payload == null) {
            LOGGER.warn("ignoring incoming message for {} will null payload",
                    getClass().getSimpleName());
            return;
        }

        LOGGER.trace("incoming CellMessage received from {}", answer.getSourcePath());

        long ttl = _msgMetadataRepo.getMetricTTL(request.getLastUOID());
        _msgMetadataRepo.remove(request.getLastUOID());

        /**
         * If we receive an exception, make a note of it and don't bother the super class.
         */
        if (payload instanceof Exception) {
            Exception e = (Exception) payload;
            LOGGER.info("received exception: {}", e.getMessage());
            return;
        }

        _domain = answer.getSourcePath().getCellDomainName();
        process(payload, ttl);
    }


    /**
     * @return the domain from which the message was sent.
     */
    public String getDomain()
    {
        return _domain;
    }

    /**
     * Exception arrived, record it and carry on.
     */
    @Override
    public void exceptionArrived(CellMessage request, Exception exception)
    {
        if (exception instanceof NoRouteToCellException) {
            LOGGER.info("Sending message to {} failed: {}",
                    ((NoRouteToCellException)exception).getDestinationPath(),
                    exception.getMessage());
        } else {
            LOGGER.error("Received remote exception: {}", exception);
        }
    }

    /**
     * Timeouts we just ignore.
     */
    @Override
    public void answerTimedOut(CellMessage request)
    {
        LOGGER.info("Message timed out");
        _msgMetadataRepo.remove(request.getLastUOID());
    }
}
