package org.dcache.services.info.gathers;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

/**
 * Instances of the SingleMessageDga class will, when triggered, send CellMessages with
 * a specific payload, which is either a Message sub-class or a String.  It
 * does this with a default interval, which must be supplied when constructing the object.
 * <p>
 * If the payload is a Message sub-class, then it is expected that some MessageHandler
 * instance will handle the reply message.  This MessageHandler object must be registered
 * with MessageHandlerChain.
 * <p>
 * If the payload is a String, then an instance of a CellMessageHandler class must also
 * be included.  This will be registered against this CellMessage, ensuring it will be
 * invoked when the reply CellMessage is received.  This is necessary because sending
 * a String will receive a generic object (e.g., of class Object), which requires
 * very special and careful treatment.
 * <p>
 * Supplying a String as a payload is deprecated, vehicles should be used instead.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SingleMessageDga extends SkelPeriodicActivity
{
    private final CellPath _target;
    private final String _requestString;
    private final Message _requestMessage;
    private final CellMessageAnswerable _handler;
    private final MessageSender _sender;

    /**
     * Create a new Single-Message DataGatheringActivity.
     * @param sender component that sends messages
     * @param targets A comma-separated list of cells to contact.
     * @param request the message string,
     * @param interval how often (in seconds) this should be sent.
     */
    public SingleMessageDga(MessageSender sender, String target, String request, CellMessageAnswerable handler, long interval)
    {
        super(interval);

        _target = new CellPath(target);
        _requestMessage = null;
        _requestString = request;
        _handler = handler;
        _sender = sender;
    }

    /**
     * Create a new Single-Message DataGatheringActivity.
     * @param cellName The path to the dCache cell,
     * @param request the Message to send
     * @param interval how often (in seconds) this message should be sent.
     */
    public SingleMessageDga(MessageHandlerChain mhc, String target, Message request, long interval)
    {
        super(interval);

        _target = new CellPath(target);
        _requestMessage = request;
        _requestString = null;
        _handler = null; // reply messages are handled by a MessageHandler chain.
        _sender = mhc;
    }


    /**
     * Send messages to query current list of pools.
     */
    @Override
    public void trigger()
    {
        super.trigger();

        if (_requestMessage != null) {
            _sender.sendMessage(metricLifetime(), null, new CellMessage(_target, _requestMessage));
        } else {
            _sender.sendMessage(metricLifetime(), _handler, _target, _requestString);
        }
    }

    @Override
    public String toString()
    {
        String message = _requestMessage != null ?
                _requestMessage.getClass().getName() : "'" + _requestString + "'";

        return getClass().getSimpleName() + "[" + _target + " " + message + "]";
    }
}
