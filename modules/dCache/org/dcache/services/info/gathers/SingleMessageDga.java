package org.dcache.services.info.gathers;

import org.dcache.services.info.InfoProvider;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.*;

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
 * Supplying a String as a payload is depricated, vehicles should be used instead.
 *  
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SingleMessageDga extends SkelPeriodicActivity {
	
	private CellPath _cp;
	private String _requestString;
	private Message _requestMessage;
	private CellMessageHandler _handler;
	private MessageHandlerChain _msgHandlerChain = InfoProvider.getInstance().getMessageHandlerChain();

	/**
	 * Create a new Single-Message DataGatheringActivity. 
	 * @param cellName The path to the dCache cell,
	 * @param request the message string,
	 * @param interval how often (in seconds) this should be sent.
	 */
	public SingleMessageDga( String cellName, String request, CellMessageHandler handler, long interval)
	{
		super( interval);
		
		_cp = new CellPath( cellName);
		_requestMessage = null;
		_requestString = request;
		_handler = handler;
	}
	
	/**
	 * Create a new Single-Message DataGatheringActivity.
	 * @param cellName The path to the dCache cell,
	 * @param request the Message to send
	 * @param interval how often (in seconds) this message should be sent.
	 */
	public SingleMessageDga( String cellName, Message request, long interval)
	{
		super( interval);
		
		_cp = new CellPath( cellName);
		_requestMessage = request;
		_requestString = null;
		// reply messages are handled by a MessageHandler chain.
	}

	
	/**
	 * Send messages to query current list of pools.
	 */
	public void trigger() {
		super.trigger();
		
		if( _requestMessage != null) {
			CellMessage msg = new CellMessage( _cp, _requestMessage);
			InfoProvider.getInstance().sendMessage( msg);
		} else
			_msgHandlerChain.sendCellMsg( _cp, _requestString, _handler, super.metricLifetime());
	}
	
	
	public String toString()
	{
		String msgName;
		
		msgName = _requestMessage != null ? _requestMessage.getClass().getName() : "'" + _requestString + "'";

		return this.getClass().getSimpleName() + "[" + _cp.getCellName() + ", " + msgName + "]";
	}
}
