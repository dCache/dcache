package org.dcache.services.info.gathers;

import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.dcache.services.info.InfoProvider;

import diskCacheV111.vehicles.Message;
import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;
import dmg.cells.nucleus.CellPath;
import dmg.cells.nucleus.UOID;


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

	/** The period between successive flushes of ancient metadata, in milliseconds */
	private static final long METADATA_FLUSH_THRESHOLD = 3600000; // 1 hour
	private static final long METADATA_FLUSH_PERIOD = 600000; // 10 minutes
	
	private static final Logger _log = Logger.getLogger( MessageHandlerChain.class);

	private List<MessageHandler> _messageHandler = new LinkedList<MessageHandler>();
	
	/**
	 * For each message we send, a small amount of metadata is recorded (when it was sent and a long).
	 * The long is so, when the return message is received, we can pass this parameter on
	 * to the message processing plug-in.  The time is so we can (every so often) delete stale entries
	 * due to message-loss.  
	 */
	private static class MessageMetadata {
		Date _timeSent;
		final long _ttl;
		MessageMetadata( long ttl) {
			_timeSent = new Date();
			_ttl = ttl;
		}
	}
	
	private Map<UOID,MessageMetadata> _msgMetadata = new HashMap<UOID,MessageMetadata>();
	private Date _nextFlushOldMetadata;
	
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
	 * This is deprecated against using Vehicles and registering MessageHandlers. 
	 * @param path the CellPath to target cell
	 * @param requestString the String, requesting information
	 * @param handler the call-back handler for the return message
	 * @param ttl lifetime of resulting metric, in seconds.
	 */
	protected void sendCellMsg( CellPath path, String requestString, CellMessageAnswerable handler, long ttl) {
		
		if( handler == null) {
			_log.error( "ignoring attempt to send string-based message without call-back");
			return;
		}
		
		CellMessage envelope = new CellMessage( path, requestString);
		sendMessage( envelope, ttl, handler);
	}
	
	
	/**
	 * The preferred way of sending requests for information.
	 * @param path the CellPath for the recipient of this message
	 * @param msg the Message payload
	 * @param ttl lifetime of resulting metric, in seconds.
	 */
	protected void sendMessage( CellPath path, Message msg, long ttl) {
		CellMessage envelope = new CellMessage( path, msg);
		sendMessage( envelope, ttl, null);		
	}
	
	
	/**
	 * Send a message envelope and record metadata against it.
	 * @param envelope the message to send
	 * @param ttl the metadata for the message
	 * @param handler the call-back for this method, or null if none should be used.
	 */
	private void sendMessage( CellMessage envelope, long ttl, CellMessageAnswerable handler) {
		if( handler == null)
			InfoProvider.getInstance().sendMessage( envelope);
		else
			InfoProvider.getInstance().sendMessage( envelope, handler);
		
		_msgMetadata.put( envelope.getUOID(), new MessageMetadata( ttl));
	}
	
	
	/**
	 * Look up a CellMessage to see how long the metrics should last.
	 * @param sentMessage the CellMessage sent
	 * @return the recommended lifetime for generated metrics, or zero if no record could be found.
	 */
	protected long getMetricLifetime( UOID msgOUID) {
		long ttl = 0;

		flushOldMetadata();
		
		MessageMetadata mm = _msgMetadata.get( msgOUID);

		if( mm != null) {
			ttl = mm._ttl;
			_msgMetadata.remove( msgOUID);
		} else {
			if( _log.isDebugEnabled())
				_log.debug( "tried to look up OUID "+msgOUID+" but couldn't find corresponding metadata.");
		}
		
		return ttl;
	}
	
	
	/**
	 * Process an incoming message using registered MessageHandlers.
	 * @param msg the incoming message's payload
	 * @return true if the message was handled, false otherwise.
	 */
	public boolean handleMessage( CellMessage msg) {
		
		long ttl = this.getMetricLifetime( msg.getLastUOID());
						
		Object messagePayload = msg.getMessageObject();
		
		if( !(messagePayload instanceof Message)) {
			if( _log.isDebugEnabled())
				_log.debug( "Received msg where payload is not instanceof Message");

			return false;
		}

		for( MessageHandler mh : _messageHandler)			
			if( mh.handleMessage( (Message) messagePayload, ttl))
				return true;
		
		return false;		
	}

	
	/**
	 * Scan through our recorded Metadata and remove very old entries.
	 * This is only done "every so often" and adds some safety against
	 * lost packets resulting in accumulated memory usage. 
	 */
	private void flushOldMetadata() {
		
		Date now = new Date();
		
		if( _nextFlushOldMetadata != null && now.before( _nextFlushOldMetadata))
			return;
		
		// Flush ancient metadata
		for( Iterator<MessageMetadata> itr = _msgMetadata.values().iterator(); itr.hasNext();) {
			MessageMetadata item = itr.next();
			
			if( now.getTime() - item._timeSent.getTime() > METADATA_FLUSH_THRESHOLD)
				itr.remove();
		}
		
		_nextFlushOldMetadata = new Date( System.currentTimeMillis() + METADATA_FLUSH_PERIOD);
	}
	
	
	/**
	 * Add a standard set of handlers for reply Messages
	 */
	public void addDefaultHandlers() {
		addMessageHandler( new LinkgroupListMsgHandler());
		addMessageHandler( new LinkgroupDetailsMsgHandler());
		addMessageHandler( new SrmSpaceDetailsMsgHandler());
	}

}
