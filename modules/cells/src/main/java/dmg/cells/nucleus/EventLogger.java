package dmg.cells.nucleus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final static Logger deliver =
        LoggerFactory.getLogger("events.org.dcache.cells.deliver");
    private final static Logger send =
        LoggerFactory.getLogger("events.org.dcache.cells.send");
    private final static Logger queue =
        LoggerFactory.getLogger("events.org.dcache.cells.queue");

    private final static String DELIVER_BEGIN =
        "org.dcache.cells.deliver.begin";
    private final static String DELIVER_END =
        "org.dcache.cells.deliver.end";
    private final static String SEND_BEGIN =
        "org.dcache.cells.send.begin";
    private final static String SEND_END =
        "org.dcache.cells.send.end";
    private final static String QUEUE_BEGIN =
        "org.dcache.cells.queue.begin";
    private final static String QUEUE_END =
        "org.dcache.cells.queue.end";

    static private String getMessage(CellMessage envelope)
    {
        Object o = envelope.getMessageObject();
        if (o == null) {
            return "";
        } else if (o instanceof String) {
            return "\"" + o + "\"";
        } else {
            return o.getClass().getSimpleName();
        }
    }

    static private String toString(Object o)
    {
        return (o == null) ? "" : o.toString();
    }

    static public void deliverBegin(CellMessage envelope)
    {
        if (deliver.isInfoEnabled()) {
            deliver.info(String.format("event=%s uoid=%s lastuoid=%s session=%s message=%s source=%s destination=%s",
                                       DELIVER_BEGIN,
                                       envelope.getUOID(), envelope.getLastUOID(),
                                       toString(envelope.getSession()),
                                       getMessage(envelope),
                                       envelope.getSourcePath(),
                                       envelope.getDestinationPath()));
        }
    }

    static public void deliverEnd(Object session, UOID uoid)
    {
        if (deliver.isInfoEnabled()) {
            deliver.info(String.format("event=%s uoid=%s session=%s",
                                       DELIVER_END,
                                       uoid, toString(session)));
        }
    }

    static public void sendBegin(CellNucleus nucleus, CellMessage envelope, String mode)
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
            send.info(String.format("event=%s uoid=%s lastuoid=%s session=%s mode=%s message=%s source=%s destination=%s",
                                    SEND_BEGIN,
                                    envelope.getUOID(), envelope.getLastUOID(),
                                    toString(envelope.getSession()),
                                    mode,
                                    getMessage(envelope),
                                    source,
                                    envelope.getDestinationPath()));
        }
    }

    static public void sendEnd(CellMessage envelope)
    {
        if (send.isInfoEnabled() && !envelope.isStreamMode()) {
            send.info(String.format("event=%s uoid=%s session=%s",
                                    SEND_END, envelope.getUOID(),
                                    toString(envelope.getSession())));
        }
    }

    static public void queueBegin(CellEvent event)
    {
        if (queue.isInfoEnabled() && event.getClass().equals(MessageEvent.class)) {
            CellMessage envelope = ((MessageEvent) event).getMessage();
            queue.info(String.format("event=%s uoid=%s lastuoid=%s session=%s source=%s destination=%s",
                                     QUEUE_BEGIN,
                                     envelope.getUOID(), envelope.getLastUOID(),
                                     toString(envelope.getSession()),
                                     envelope.getSourcePath(),
                                     envelope.getDestinationPath()));
        }
    }

    static public void queueEnd(CellEvent event)
    {
        if (queue.isInfoEnabled() && event.getClass().equals(MessageEvent.class)) {
            CellMessage envelope = ((MessageEvent) event).getMessage();
            queue.info(String.format("event=%s uoid=%s session=%s",
                                     QUEUE_END,
                                     envelope.getUOID(),
                                     toString(envelope.getSession())));
        }
    }

}
