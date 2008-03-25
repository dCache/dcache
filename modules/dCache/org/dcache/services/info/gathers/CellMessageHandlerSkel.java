package org.dcache.services.info.gathers;

import org.apache.log4j.Logger;
import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StateComposite;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateUpdate;

import dmg.cells.nucleus.CellMessage;
import dmg.cells.nucleus.CellMessageAnswerable;


/**
 * This Class introduces a number of useful utilities common to all
 * CellMessageHandler parsing implementations. 
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class CellMessageHandlerSkel implements CellMessageAnswerable {
	
	private static final Logger _log = Logger.getLogger( CellMessageHandlerSkel.class);

	private final State _state = State.getInstance();
	private final MessageHandlerChain _msgHandlerChain = InfoProvider.getInstance().getMessageHandlerChain();
	
	/**
	 *  Process the information payload.  The metricLifetime gives how long
	 *  the metrics should last, in seconds.
	 */
	abstract public void process( Object msgPayload, long metricLifetime);

	
	/**
	 * Build a list of items under a specific path.  These are recorded as
	 * StateComposites (branch nodes).  
	 * @param update the StateUpdate to append
	 * @param parentPath the StatePath pointing to the parent of these items
	 * @param items an array of items.
	 * @param metricLifetime how long the metric should last, in seconds.
	 */
	protected void addItems( StateUpdate update, StatePath parentPath,
							Object[] items, long metricLifetime) {
		if( _log.isDebugEnabled())
			_log.debug( "appending list-items under " + parentPath);
		
		for( int i = 0; i < items.length; i++) {
			String listItem = (String) items[i];
			
			if( _log.isDebugEnabled())
				_log.debug( "    adding item " + listItem);
			
			update.appendUpdate( parentPath.newChild( listItem), new StateComposite( metricLifetime));
		}
	}
	
	
	/**
	 * Send a StateUpdate object to our State singleton.  If we get this wrong, log this
	 * fact somewhere.
	 * @param update the StateUpdate to apply to the state tree.
	 */
	protected void applyUpdates( StateUpdate update) {
		if( _log.isDebugEnabled())
			_log.debug( "adding update to state's to-do stack with " + update.count() + " updates for " + this.getClass().getSimpleName());

		_state.updateState(update);
	}
	
	
	
	/**
	 * The following methods are needed for CellMessageAnswerable.
	 */
	
	/**
	 * Incoming message: look it up and call the (abstract) process() method.
	 */
	public void answerArrived( CellMessage request , CellMessage answer) {
		Object payload = answer.getMessageObject();
		
		if( payload == null) {
			_log.warn( "ignoring incoming message for " + this.getClass().getSimpleName() + " will null payload");
			return;
		}
		
		if( _log.isDebugEnabled())
			_log.debug( "incoming CellMessage received from " + answer.getSourceAddress());

		long ttl = _msgHandlerChain.getMetricLifetime( request);
		
		process( payload, ttl);
	}
               

	/**
	 * Exception arrived, record it and carry on.
	 */
	public void exceptionArrived( CellMessage request , Exception   exception ) {
		_log.error( "Received remote exception: ", exception);
	}
	
	/**
	 * Timeouts we just ignore.
	 */
	public void answerTimedOut( CellMessage request) {}
}
