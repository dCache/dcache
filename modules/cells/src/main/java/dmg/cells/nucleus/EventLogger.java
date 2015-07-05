package dmg.cells.nucleus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.util.NetLoggerBuilder;

/**
 * Implements cell event logging following the NetLogger format.
 *
 * The log format was originally documented as a CEDPS best practice recommendation,
 * however CEDPS no longer exists. A more current description of the format can
 * be found at https://docs.google.com/document/d/1oeW_l_YgQbR-C_7R2cKl6eYBT5N4WSMbvz0AT6hYDvA
 *
 * The NetLogger project can be found at http://netlogger.lbl.gov
 */
public class EventLogger
{
    private static final Logger deliver =
        LoggerFactory.getLogger("org.dcache.events.cells.deliver");
    private static final Logger send =
        LoggerFactory.getLogger("org.dcache.events.cells.send");
    private static final Logger queue =
        LoggerFactory.getLogger("org.dcache.events.cells.queue");

    private static final String DELIVER_BEGIN =
        "org.dcache.cells.deliver.begin";
    private static final String DELIVER_END =
        "org.dcache.cells.deliver.end";
    private static final String SEND_BEGIN =
        "org.dcache.cells.send.begin";
    private static final String SEND_END =
        "org.dcache.cells.send.end";
    private static final String QUEUE_BEGIN =
        "org.dcache.cells.queue.begin";
    private static final String QUEUE_END =
        "org.dcache.cells.queue.end";

    private static String getMessage(CellMessage envelope)
    {
        Object o = envelope.getMessageObject();
        if (o == null) {
            return "";
        } else if (o instanceof String) {
            return o.toString();
        } else {
            return o.getClass().getSimpleName();
        }
    }

    public static void deliverBegin(CellMessage envelope)
    {
        if (deliver.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(DELIVER_BEGIN);
            log.add("uoid", envelope.getUOID());
            log.add("lastuoid", envelope.getLastUOID());
            log.add("session", envelope.getSession());
            log.add("message", getMessage(envelope));
            log.add("source", envelope.getSourcePath());
            log.add("destination", envelope.getDestinationPath());
            deliver.info(log.toString());
        }
    }

    public static void deliverEnd(Object session, UOID uoid)
    {
        if (deliver.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(DELIVER_END);
            log.add("uoid", uoid);
            log.add("session", session);
            deliver.info(log.toString());
        }
    }

    public static void sendBegin(CellNucleus nucleus, CellMessage envelope, String mode)
    {
        if (send.isInfoEnabled() && !envelope.isStreamMode()) {
            /* The envelope does not have a source yet, so we need to
             * generate it from the information stored in the nucleus.
             */
            CellPath source = envelope.getSourcePath();
            if (source.hops() == 0) {
                source = new CellPath();
                source.add(nucleus.getThisAddress());
            }
            NetLoggerBuilder log = new NetLoggerBuilder(SEND_BEGIN);
            log.add("uoid", envelope.getUOID());
            log.add("lastuoid", envelope.getLastUOID());
            log.add("session", envelope.getSession());
            log.add("mode", mode);
            log.add("message", getMessage(envelope));
            log.add("source", source);
            log.add("destination", envelope.getDestinationPath());
            send.info(log.toString());
        }
    }

    public static void sendEnd(CellMessage envelope)
    {
        if (send.isInfoEnabled() && !envelope.isStreamMode()) {
            NetLoggerBuilder log = new NetLoggerBuilder(SEND_END);
            log.add("uoid", envelope.getUOID());
            log.add("session", envelope.getSession());
            send.info(log.toString());
        }
    }

    public static void queueBegin(CellEvent event)
    {
        if (queue.isInfoEnabled() && event.getClass().equals(MessageEvent.class)) {
            CellMessage envelope = ((MessageEvent) event).getMessage();
            NetLoggerBuilder log = new NetLoggerBuilder(QUEUE_BEGIN);
            log.add("uoid", envelope.getUOID());
            log.add("lastuoid", envelope.getLastUOID());
            log.add("session", envelope.getSession());
            log.add("source", envelope.getSourcePath());
            log.add("destination", envelope.getDestinationPath());
            queue.info(log.toString());
        }
    }

    public static void queueEnd(CellEvent event)
    {
        if (queue.isInfoEnabled() && event.getClass().equals(MessageEvent.class)) {
            CellMessage envelope = ((MessageEvent) event).getMessage();
            NetLoggerBuilder log = new NetLoggerBuilder(QUEUE_END);
            log.add("uoid", envelope.getUOID());
            log.add("session", envelope.getSession());
            queue.info(log.toString());
        }
    }
}
