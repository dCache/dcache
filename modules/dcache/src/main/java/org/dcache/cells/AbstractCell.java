package org.dcache.cells;

import diskCacheV111.util.CacheException;
import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellAdapter;
import dmg.cells.nucleus.CellAddressCore;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageReceiver;
import dmg.cells.nucleus.Reply;
import dmg.cells.nucleus.UOID;
import java.io.Serializable;
import java.util.concurrent.Executor;
import org.dcache.util.Args;
import org.dcache.util.Option;
import org.dcache.util.OptionParser;

/**
 * Abstract cell implementation providing features needed by many dCache cells.
 *
 * <h2>Automatic dispatch of dCache messages to message handler</h2>
 * <p>
 * See org.dcache.util.CellMessageDispatcher for details.
 *
 * <h2>Option parsing</h2>
 * <p>
 * AbstractCell supports automatic option parsing based on annotations of fields. A field is
 * annotated with the Option annotation. The annotation supports the following attributes:
 *
 * <dl>
 * <dt>name</dt>
 * <dd>The name of the option.</dd>
 *
 * <dt>description</dt>
 * <dd>A one line description of the option.</dd>
 *
 * <dt>defaultValue</dt>
 * <dd>The default value if the option is not specified,
 * specified as a string.</dd>
 *
 * <dt>unit</dt>
 * <dd>The unit of the value, if any, e.g. seconds.</dd>
 *
 * <dt>required</dt>
 * <dd>Whether this is a mandatory option. Defaults to false.</dd>
 *
 * <dt>log</dt>
 * <dd>Whether to log the value of the option during startup.
 * Defaults to true, but should be disabled for sensitive
 * information.</dd>
 * </dl>
 * <p>
 * Options are automatically converted to the type of the field. In
 * case of non-POD fields, the class must have a one-argument
 * constructor taking a String. The File class is an example of such a
 * class.
 * <p>
 * By defaults options are logged at the info level. The description
 * and unit should be formulated in such a way that the a message can
 * be formed as "<description> set to <value> <unit>".
 * <p>
 * In case a required option is missing, an IllegalArgumentException
 * is thrown during option parsing.
 * <p>
 * It is important that fields used for storing options do not have an
 * initializer. An initializer would overwrite the value retrieved
 * from the option. Empty Strings will become null.
 * <p>
 * Example code:
 *
 * <code>
 *   @Option(
 *       name = "maxPinDuration",
 *       description = "Max. lifetime of a pin",
 *       defaultValue = "86400000", // one day
 *       unit = "ms"
 *   )
 *   protected long _maxPinDuration;
 *
 * @see org.dcache.cells.CellMessageDispatcher
 */
public class AbstractCell extends CellAdapter implements CellMessageReceiver {

    private static final String MSG_UOID_MISMATCH =
          "A reply [%s] was generated by a message listener, but the " +
                "message UOID indicates that another message listener has " +
                "already replied to the message.";
    private static final String MSG_ALREADY_FORWARDED =
          "A result [%s] was generated by a message listener, but the " +
                "message was already forwarded and thus cannot can be sent.";

    @Option(
          name = "monitor",
          description = "Cell message monitoring",
          defaultValue = "false"
    )
    protected boolean _isMonitoringEnabled;

    @Option(
          name = "cellClass",
          description = "Cell classification"
    )
    protected String _cellClass;

    private final OptionParser _optionParser;

    /**
     * Helper object used to dispatch messages to message listeners.
     */
    protected final CellMessageDispatcher _messageDispatcher =
          new CellMessageDispatcher("messageArrived");

    /**
     * Helper object used to dispatch messages to forward to message listeners.
     */
    protected final CellMessageDispatcher _forwardDispatcher =
          new CellMessageDispatcher("messageToForward");

    protected MessageProcessingMonitor _monitor;

    /**
     * Returns the cell type specified as option 'cellType', or "Generic" if the option was not
     * given.
     */
    private static String getCellType(Args args) {
        String type = args.getOpt("cellType");
        return (type == null) ? "Generic" : type;
    }

    public AbstractCell(String cellName, String arguments) {
        this(cellName, new Args(arguments), null);
    }

    public AbstractCell(String cellName, String arguments, Executor executor) {
        this(cellName, new Args(arguments), executor);
    }

    public AbstractCell(String cellName, Args arguments, Executor executor) {
        this(cellName, getCellType(arguments), arguments, executor);
    }

    /**
     * Constructs an AbstractCell.
     *
     * @param cellName  the name of the cell
     * @param cellType  the type of the cell
     * @param arguments the cell arguments
     */
    public AbstractCell(String cellName, String cellType, Args arguments) {
        this(cellName, cellType, arguments, null);
    }

    public AbstractCell(String cellName, String cellType, Args arguments, Executor executor) {
        super(cellName, cellType, arguments, executor);
        _optionParser = new OptionParser(arguments);
    }

    @Override
    protected void starting() throws Exception {
        _optionParser.inject(AbstractCell.this);

        _monitor = new MessageProcessingMonitor();
        _monitor.setCellEndpoint(AbstractCell.this);
        _monitor.setEnabled(_isMonitoringEnabled);

        if (_cellClass != null) {
            getNucleus().setCellClass(_cellClass);
        }

        addMessageListener(AbstractCell.this);
        addCommandListener(_monitor);
    }

    /**
     * Adds a listener for dCache messages.
     *
     * @see CellMessageDispatcher#addMessageListener
     */
    public void addMessageListener(CellMessageReceiver o) {
        _messageDispatcher.addMessageListener(o);
        _forwardDispatcher.addMessageListener(o);
    }

    /**
     * Removes a listener previously added with addMessageListener.
     */
    public void removeMessageListener(CellMessageReceiver o) {
        _messageDispatcher.removeMessageListener(o);
        _forwardDispatcher.removeMessageListener(o);
    }

    /**
     * Delivers message to registered forward listeners.
     * <p>
     * A reply is delivered back to the client if any message listener:
     * <p>
     * - Returns a value.
     * <p>
     * - Throws a declared exception, IllegalStateException or IllegalArgumentException.
     * <p>
     * dCache vehicles (subclasses of Message) are recognized, and a reply is only sent if requested
     * by the client.
     * <p>
     * For dCache vehicles, errors are reported by sending back the vehicle with an error code.
     * CacheException and IllegalArgumentException are recognised and an appropriate error code is
     * used. If the message is already flagged as having failed, that error is not replaced.
     * <p>
     * Return values implementing Reply are recognized and the reply is delivered by calling the
     * deliver method on the Reply object. It is the responsibility of the Reply object to decide
     * whether to forward or return the message.
     * <p>
     * If no listener returns a value or throws an exception, then the message is forwarded to the
     * next destination.
     */
    @Override
    public void messageToForward(CellMessage envelope) {
        CellEndpoint endpoint = _monitor.getReplyCellEndpoint(envelope);
        UOID uoid = envelope.getUOID();
        CellAddressCore address = envelope.getSourcePath().getDestinationAddress();
        boolean isReply = isReply(envelope);
        Object result = _forwardDispatcher.call(envelope);

        if (result != null) {
            if (!uoid.equals(envelope.getUOID())) {
                throw new RuntimeException(String.format(MSG_UOID_MISMATCH, result));
            }
            if (!address.equals(envelope.getSourcePath().getDestinationAddress())) {
                throw new RuntimeException(String.format(MSG_ALREADY_FORWARDED, result));
            }
            Serializable o = envelope.getMessageObject();

            if (result instanceof Reply) {
                Reply reply = (Reply) result;
                reply.deliver(endpoint, envelope);
            } else {
                if (o instanceof Message) {
                    Message msg = (Message) o;

                    /* Don't bother replying if requester isn't interested.
                     */
                    if (!msg.getReplyRequired() && !isReply) {
                        return;
                    }

                    /* dCache vehicles can transport errors back to the
                     * requester, so detect if this is an error reply.
                     */
                    if (result instanceof CacheException) {
                        CacheException e = (CacheException) result;
                        msg.setFailedConditionally(e.getRc(), e.getMessage());
                        result = msg;
                    } else if (result instanceof IllegalArgumentException) {
                        msg.setFailedConditionally(CacheException.INVALID_ARGS, result.toString());
                        result = msg;
                    } else if (result instanceof Exception) {
                        msg.setFailedConditionally(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                              (Exception) result);
                        result = msg;
                    }
                }

                if (!isReply) {
                    envelope.revertDirection();
                }
                envelope.setMessageObject((Serializable) result);
                endpoint.sendMessage(envelope);
            }
        } else {
            endpoint.sendMessage(envelope);
        }
    }

    private boolean isReply(CellMessage envelope) {
        Object message = envelope.getMessageObject();
        return (message instanceof Message) && ((Message) message).isReply();
    }

    /**
     * Delivers messages to registered message listeners.
     * <p>
     * A reply is delivered back to the client if any message listener:
     * <p>
     * - Returns a value
     * <p>
     * - Throws a checked exception, IllegalStateException or IllegalArgumentException.
     * <p>
     * dCache vehicles (subclasses of Message) are recognized, and a reply is only sent if requested
     * by the client.
     * <p>
     * For dCache vehicles, errors are reported by sending back the vehicle with an error code.
     * CacheException and IllegalArgumentException are recognised and an appropriate error code is
     * used.
     * <p>
     * Return values implementing Reply are recognized and the reply is delivered by calling the
     * deliver method on the Reply object.
     */
    @Override
    public void messageArrived(CellMessage envelope) {
        CellEndpoint endpoint = _monitor.getReplyCellEndpoint(envelope);
        UOID uoid = envelope.getUOID();
        boolean isReply = isReply(envelope);
        Object result = _messageDispatcher.call(envelope);

        if (result != null && !isReply) {
            if (!uoid.equals(envelope.getUOID())) {
                throw new RuntimeException(String.format(MSG_UOID_MISMATCH, result));
            }
            Serializable o = envelope.getMessageObject();
            if (result instanceof Reply) {
                Reply reply = (Reply) result;
                reply.deliver(endpoint, envelope);
            } else {
                if (o instanceof Message) {
                    Message msg = (Message) o;

                    /* Don't send reply if not requested. Some vehicles
                     * contain a bug in which the message is marked as not
                     * requiring a reply, while what was intended was
                     * asynchronous processing on the server side. Therefore
                     * we have a special test for Reply results.
                     */
                    if (!msg.getReplyRequired()) {
                        return;
                    }

                    /* dCache vehicles can transport errors back to the
                     * requester, so detect if this is an error reply.
                     */
                    if (result instanceof CacheException) {
                        CacheException e = (CacheException) result;
                        msg.setFailed(e.getRc(), e.getMessage());
                        result = msg;
                    } else if (result instanceof IllegalArgumentException) {
                        msg.setFailed(CacheException.INVALID_ARGS, result.toString());
                        result = msg;
                    } else if (result instanceof Exception) {
                        msg.setFailed(CacheException.UNEXPECTED_SYSTEM_EXCEPTION,
                              (Exception) result);
                        result = msg;
                    }
                }
                envelope.revertDirection();
                envelope.setMessageObject((Serializable) result);
                endpoint.sendMessage(envelope);
            }
        }
    }
}
