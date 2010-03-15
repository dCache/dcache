package org.dcache.services.info.stateInfo;

import java.util.Set;
import java.util.HashSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateTransition;


/**
 * A very simple StateVisitor class.  This visitor builds a list of the names of immediate
 * children of a StateComposite.  The parent StateComposite is described by the StatePath.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class ListVisitor extends SkeletonListVisitor {

	private static Logger _log = LoggerFactory.getLogger( ListVisitor.class);


	/**
	 * Obtain the set of items below a certain path within the dCache state.
	 * @param path the StatePath that is the parent to the required items.
	 * @return the Set of all items that have the path as their parent.
	 */
	static public Set<String> getDetails( StateExhibitor exhibitor, StatePath path) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering current status for path " + path);

		ListVisitor visitor = new ListVisitor( path);
		exhibitor.visitState(visitor);
		return visitor.getItems();
	}

	/**
	 * Obtain the set of items below a certain path within the future dCache state
	 * after a StateTransition has been applied.
	 * @param str the StateTransition that is pending.
	 * @param path the StatePath that is the parent to the require list of items.
	 * @return the Set of all items that have the path as their parent.
	 */
	static public Set<String> getDetails( StateExhibitor exhibitor, StateTransition str, StatePath path) {
		if( _log.isDebugEnabled())
			_log.debug( "Gathering current status for path " + path);

		ListVisitor visitor = new ListVisitor( path);
		exhibitor.visitState( visitor, str);
		return visitor.getItems();
	}

	private Set<String> _listItems;

	public ListVisitor( StatePath parent) {
		super( parent);
		_listItems = new HashSet<String>();
	}

	@Override
	protected void newListItem( String name) {
		super.newListItem( name);
		_listItems.add( name);
	}

	public Set<String> getItems() {
		return _listItems;
	}
}
