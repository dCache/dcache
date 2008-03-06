package org.dcache.services.info.gathers;

import java.util.*;

import org.dcache.services.info.InfoProvider;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.*;


/**
 * A MessageHandlerChain allows multiple MessageHandler subclass instances to attempt to 
 * process an incoming Message.  This allows easy addition of extra monitoring by receiving
 * additional messages.
 * 
 * Zero or more MessageHandler subclass instances are registered with the MessageHandlerChain.
 * When passed an incoming Message, the MessageHandlerChain instance will pass the Message to
 * each MessageHandler subclass instance in turn until one succeeds in processing the Message.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class MessageHandlerChain {
	
	private List<MessageHandler> _messageHandler = new LinkedList<MessageHandler>();
	
	/**
	 * Following variable stores association between out-going msg UOID and a handler to process
	 * reply message payload.
	 * @deprecated this is ugly and should be replaced by vehicles (subclasses of Message)
	 */
	private Map<UOID,CellMessageHandler> _cellMsgHandlerLookup= new HashMap<UOID,CellMessageHandler>();
	
	/**
	 * For each message we send, a small amount of metadata is recorded (when it was sent and a long).
	 * The long is so, when the return message is received, we can pass this parameter on
	 * to the message processing plugin.  The time is so we can (every so often) delete stale entries
	 * due to message-loss.  
	 */
	private class MessageMetadata {
		Date _timeSent;
		long _ttl;
		MessageMetadata( long ttl) {
			_timeSent = new Date();
			_ttl = ttl;
		}
	}
	
	private Map<UOID,MessageMetadata> _msgMetadata = new HashMap<UOID,MessageMetadata>();
	
	
	/**
	 * Add a new MessageHandler to the list.
	 * @param handler a new handler to add to the list.
	 */
	public void addMessageHandler( MessageHandler handler) {
		_messageHandler.add(handler);
	}

	/**
	 * @return a simple array of registered MessageHandlers subclass types.
	 */
	public String[] listMessageHandlers() {
		int i=0;
		String[] msgHandlers = new String[_messageHandler.size()];
		
		for( MessageHandler mh : _messageHandler)
			msgHandlers [i++] =  mh.getClass().getSimpleName(); // We're assuming only one instance per Class
		
		return msgHandlers;
	}
	
	
	/**
	 * Common method to send a CellMessage and register a handler for the return message.
	 * This is depricated against using Vehicles and registering MessageHandlers. 
	 * @param path the CellPath to target cell
	 * @param requestString the String, requesting information
	 * @param handler the call-back handler for the return message
	 * @param ttl lifetime of resulting metric, in seconds.
	 */
	protected void sendCellMsg( CellPath path, String requestString, CellMessageHandler handler, long ttl) {
		CellMessage msg = new CellMessage( path, requestString);

		sendMessage( msg, ttl);
		
		/**
		 *  We must register the handle *after* sending the message as the UOID may (in this case,
		 *  will always) be altered.  This introduces a race-condition, but is unavoidable with
		 *  current cell comms.
		 */ 
		_cellMsgHandlerLookup.put( msg.getUOID(), handler);
	}
	
	
	/**
	 * The preferred way of sending requests for information.
	 * @param path the CellPath for the recipient of this message
	 * @param msg the Message payload
	 * @param ttl lifetime of resulting metric, in seconds.
	 */
	protected void sendMessage( CellPath path, Message msg, long ttl) {
		CellMessage envelope = new CellMessage( path, msg);
		sendMessage( envelope, ttl);		
	}
	
	
	/**
	 * Send a message envelope and record metadata against it.
	 * @param envelope the message to send
	 * @param ttl the metadata for the message
	 */
	private void sendMessage( CellMessage envelope, long ttl) {
		InfoProvider.getInstance().sendMessage( envelope);
		_msgMetadata.put( envelope.getUOID(), new MessageMetadata( ttl));
	}
	
	
	/**
	 * Process an incoming message using registered MessageHandlers
	 * @param msg the incoming message's payload
	 * @return true if the message was handled, false otherwise.
	 */
	public boolean handleMessage( CellMessage msg) {
		
		long ttl = 0;
		
		MessageMetadata mm = _msgMetadata.get(msg.getLastUOID());
		if( mm != null) {
			ttl = mm._ttl;
			_msgMetadata.remove(msg.getLastUOID());
		}
		
		Object messagePayload = msg.getMessageObject();
		
		if( messagePayload instanceof Message)
			return handleVehicleMessage( (Message) messagePayload, ttl);
		
		/**
		 * Deal with some damn awkward, ugly, ugly, existing code.
		 * 
		 * We are forced to record our CellMessage UIDs to figure out what to do with the
		 * corresponding reply CellMessage.
		 * 
		 * TODO: replace this with type-safe transports (i.e. subclasses of Message)
		 */
		CellMessageHandler handler = _cellMsgHandlerLookup.get(msg.getLastUOID());
		
		if( handler != null) {
			handler.process( messagePayload, ttl);
			_cellMsgHandlerLookup.remove(msg.getLastUOID());
			return true;
		}
		
		return false;
	}
	
	/**
	 * Handle an incoming message that is of a specific Vehicle.
	 * @param msg  the incoming message Message
	 * @param delay the expected time, in seconds, until next delivery of msg
	 * @return true if the Message was successfully processed. 
	 */
	private boolean handleVehicleMessage( Message msg, long delay) {
		
		for( Iterator<MessageHandler> itr = _messageHandler.iterator(); itr.hasNext();) {
			MessageHandler mh = itr.next();
			
			if( mh.handleMessage( msg, delay))
				return true;
		}
		
		return false;		
	}	
}
