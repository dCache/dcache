package org.dcache.services.info.gathers;

import org.dcache.services.info.base.*;

/**
 * This Class introduces a number of useful utilities common to all
 * CellMessageHandler parsing implementations. 
 * 
 * @deprecated this class is to support the depricated "raw" CellMessage payloads 
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class CellMessageHandlerSkel implements CellMessageHandler {
	
	/**
	 *  Process the information payload.  The metricLifetime gives how long
	 *  the metrics should last, in seconds.
	 */
	abstract public void process( Object msgPayload, long metricLifetime);
	
	
	/**
	 * Add a bunch of StateComposites (branch nodes) under a specific location in 
	 * @param update the StateUpdate to append
	 * @param parentPath the StatePath pointing to the parent of these items
	 * @param items an array of items.
	 * @param metricLifetime how long the metric should last, in seconds.
	 */
	protected void addItems( StateUpdate update, StatePath parentPath,
							Object[] items, long metricLifetime) {
		for( int i = 0; i < items.length; i++) {
			String listItem = (String) items[i];
			update.appendUpdate( parentPath.newChild( listItem), new StateComposite( metricLifetime));
		}
	}
	
	
	/**
	 * Send a StateUpdate object to our State singleton.  If we get this wrong, log this
	 * fact somewhere.
	 * @param update the StateUpdate to apply to the state tree.
	 */
	protected void applyUpdates( StateUpdate update) {
		State.getInstance().updateState(update);
	}
}
