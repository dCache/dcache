package org.dcache.services.info.stateInfo;

import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.dcache.services.info.base.BooleanStateValue;
import org.dcache.services.info.base.FloatingPointStateValue;
import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.State;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.StringStateValue;


/**
 * A very simple StateVisitor class.  This visitor builds a list of the names of immediate
 * children of a StateComposite.  The parent StateComposite is described by the StatePath. 
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ListVisitor extends SkeletonListVisitor {
	
	private static Logger _log = Logger.getLogger( ListVisitor.class);

	
	/**
	 * Obtain the set of items below a certain path within the dCache state.
	 * @param path the StatePath that is the parent to the required items. 
	 * @return the Set of all items that have the path as their parent.
	 */
	static public Set<String> getDetails( StatePath path) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering current status for path " + path);
		
		ListVisitor visitor = new ListVisitor( path);
		State.getInstance().visitState(visitor, path);		
		return visitor.getItems();
	}

	/**
	 * Obtain the set of items below a certain path within the future dCache state
	 * after a StateTransition has been applied.
	 * @param str the StateTransition that is pending.
	 * @param path the StatePath that is the parent to the require list of items.
	 * @return the Set of all items that have the path as their parent.
	 */
	static public Set<String> getDetails( StateTransition str, StatePath path) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering current status for path " + path);
		
		ListVisitor visitor = new ListVisitor( path);
		State.getInstance().visitState( str, visitor, path);
		return visitor.getItems();		
	}

	private Set<String> _listItems;
	
	public ListVisitor( StatePath parent) {		
		super( parent);
		_listItems = new HashSet<String>();
	}

	@Override
	protected void newListItem( String name) {
		newListItem( name);
		_listItems.add( name);
	}
	
	public Set<String> getItems() {
		return _listItems;
	}
}
