package org.dcache.services.info.gathers;

import org.dcache.services.info.InfoProvider;
import org.dcache.services.info.base.*;

import dmg.cells.nucleus.CellPath;

/**
 * This class sends a series of messages based on the current state tree.
 *   It uses a visitor to
 * extract all entried below a certain point in the state tree and constructs a message for 
 * each entry; for example, if the path is "dCache.pools" and the state contains entries for
 * "dCache.pools.fandango_1" and "dCache.pools.fandango_2" then two CellMessages are sent.
 * <p>
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ListBasedMessageDga extends SkelListBasedActivity {

	private CellPath _cp;
	private String _messagePrefix;
	private CellMessageHandler _handler;
	private MessageHandlerChain _msgHandlerChain = InfoProvider.getInstance().getMessageHandlerChain();
	
	/* The following two are only needed for toString() */
	private String _cellName;
	private String _parentPath;
	
	
	/**
	 * Create a new list-based data-gathering activity
	 * @param parent the StatePath that points to the list's parent item
	 * @param cellName the name of the cell to contact
	 * @param message the message to send.
	 * @param handler the cell handler for the return msg payload.
	 */
	public ListBasedMessageDga( StatePath parent, String cellName, String message, CellMessageHandler handler) {
		
		super( parent);
		
		_cellName = cellName;
		_parentPath = parent.toString();
		_messagePrefix = message;
		_handler = handler;
		
		_cp = new CellPath( cellName);
	}
	

	/**
	 * Triggered every-so-often, under control of SkelListBasedActivity.
	 */
	public void trigger() {
		
		super.trigger();
		
		String item = getNextItem();

		// Only null if there's nothing under _parentPath in dCache.
		if( item != null) {
			
			StringBuffer sb = new StringBuffer();
			sb.append(_messagePrefix);
			sb.append( " ");
			sb.append( item);

			_msgHandlerChain.sendCellMsg( _cp, sb.toString(), _handler, getMetricLifetime());
		}
	}
	
	

	public String toString() {
		StringBuffer sb = new StringBuffer();
		
		sb.append(this.getClass().getSimpleName());
		sb.append("[");
		sb.append( _cellName);
		sb.append( ", ");
		sb.append( _parentPath);
		sb.append( ", ");
		sb.append( _messagePrefix);
		sb.append( "]");
		
		return sb.toString();
	}
}
