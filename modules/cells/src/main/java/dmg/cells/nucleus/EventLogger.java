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
    private static final Logger lifecycle =
        LoggerFactory.getLogger("org.dcache.events.cells.lifecycle");

    /* Message events */
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

    /* Cell lifecycle events */
    private static final String PREPARE_SETUP_BEGIN =
        "org.dcache.cells.lifecycle.prepare-setup.begin";
    private static final String PREPARE_SETUP_END =
        "org.dcache.cells.lifecycle.prepare-setup.end";
    private static final String POST_SETUP_BEGIN =
        "org.dcache.cells.lifecycle.post-setup.begin";
    private static final String POST_SETUP_END =
        "org.dcache.cells.lifecycle.post-setup.end";
    private static final String PREPARE_REMOVAL_BEGIN =
        "org.dcache.cells.lifecycle.prepare-removal.begin";
    private static final String PREPARE_REMOVAL_END =
        "org.dcache.cells.lifecycle.prepare-removal.end";
    private static final String POST_REMOVAL_BEGIN =
        "org.dcache.cells.lifecycle.post-removal.begin";
    private static final String POST_REMOVAL_END =
        "org.dcache.cells.lifecycle.post-removal.end";

    /* AbstractCell lifecycle events */
    private static final String STARTING_BEGIN =
        "org.dcache.cells.lifecycle.starting.begin";
    private static final String STARTING_END =
        "org.dcache.cells.lifecycle.starting.end";
    private static final String STARTED_BEGIN =
        "org.dcache.cells.lifecycle.started.begin";
    private static final String STARTED_END =
        "org.dcache.cells.lifecycle.started.end";
    private static final String STOPPING_BEGIN =
        "org.dcache.cells.lifecycle.stopping.begin";
    private static final String STOPPING_END =
        "org.dcache.cells.lifecycle.stopping.end";
    private static final String STOPPED_BEGIN =
        "org.dcache.cells.lifecycle.stopped.begin";
    private static final String STOPPED_END =
        "org.dcache.cells.lifecycle.stopped.end";

    public static void prepareSetupBegin(Cell cell, StartEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(PREPARE_SETUP_BEGIN);
            log.add("cell", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void prepareSetupEnd(Cell cell, StartEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(PREPARE_SETUP_END);
            log.add("cell", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void postStartupBegin(Cell cell, StartEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(POST_SETUP_BEGIN);
            log.add("cell", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void postStartupEnd(Cell cell, StartEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(POST_SETUP_END);
            log.add("cell", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void prepareRemovalBegin(Cell cell, KillEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(PREPARE_REMOVAL_BEGIN);
            log.add("cell", event.getTarget());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("killer", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void prepareRemovalEnd(Cell cell, KillEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(PREPARE_REMOVAL_END);
            log.add("cell", event.getTarget());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("killer", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void postRemovalBegin(Cell cell, KillEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(POST_REMOVAL_BEGIN);
            log.add("cell", event.getTarget());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("killer", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void postRemovalEnd(Cell cell, KillEvent event)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(POST_REMOVAL_END);
            log.add("cell", event.getTarget());
            log.add("class", cell.getClass().getCanonicalName());
            log.add("killer", ((CellPath)event.getSource()).getCurrent().getCellName());
            log.add("timeout", event.getTimeout());
            lifecycle.info(log.toString());
        }
    }

    public static void startingBegin(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STARTING_BEGIN);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void startingEnd(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STARTING_END);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void startedBegin(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STARTED_BEGIN);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void startedEnd(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STARTED_END);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void stoppingBegin(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STOPPING_BEGIN);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void stoppingEnd(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STOPPING_END);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void stoppedBegin(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STOPPED_BEGIN);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

    public static void stoppedEnd(String cell)
    {
        if (lifecycle.isInfoEnabled()) {
            NetLoggerBuilder log = new NetLoggerBuilder(STOPPED_END);
            log.add("cell", cell);
            lifecycle.info(log.toString());
        }
    }

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

    public static void sendBegin(CellMessage envelope, String mode)
    {
        if (send.isInfoEnabled() && !envelope.isStreamMode()) {
            CellPath source = envelope.getSourcePath();
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
