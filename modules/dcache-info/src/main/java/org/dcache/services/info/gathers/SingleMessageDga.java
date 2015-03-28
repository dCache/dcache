package org.dcache.services.info.gathers;

import diskCacheV111.vehicles.Message;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;

/**
 * Instances of the SingleMessageDga class will, when triggered, send a CellMessage with
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
public class SingleMessageDga extends SkelPeriodicActivity {

    private CellPath _cp;
    private String _requestString;
    private Message _requestMessage;
    private CellMessageAnswerable _handler;
    private final MessageSender _sender;

    /**
     * Create a new Single-Message DataGatheringActivity.
     * @param cellName The path to the dCache cell,
     * @param request the message string,
     * @param interval how often (in seconds) this should be sent.
     */
    public SingleMessageDga(MessageSender sender, String cellName, String request, CellMessageAnswerable handler, long interval)
    {
        super(interval);

        _cp = new CellPath(cellName);
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
    public SingleMessageDga(MessageHandlerChain mhc, String cellName, Message request, long interval)
    {
        super(interval);

        _cp = new CellPath(cellName);
        _requestMessage = request;
        _requestString = null;
        // reply messages are handled by a MessageHandler chain.
        _sender = mhc;
    }


    /**
     * Send messages to query current list of pools.
     */
    @Override
    public void trigger() {
        super.trigger();

        if (_requestMessage != null) {
            CellMessage msg = new CellMessage(_cp, _requestMessage);
            _sender.sendMessage(0, null, msg);
        } else {
            _sender.sendMessage(super
                    .metricLifetime(), _handler, _cp, _requestString);
        }
    }


    @Override
    public String toString()
    {
        String msgName;

        msgName = _requestMessage != null ? _requestMessage.getClass().getName() : "'" + _requestString + "'";

        return this.getClass().getSimpleName() + "[" + _cp.getCellName() + ", " + msgName + "]";
    }
}
