/**
 * 
 */
package org.dcache.services.info.secondaryInfoProviders;

import org.dcache.services.info.base.StatePathPredicate;
import org.dcache.services.info.base.StateWatcher;
import java.util.*;

/**
 * Provide secondary information about pools
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class AbstractStateWatcher implements StateWatcher {

	private Collection<StatePathPredicate> _predicates;
	
	public AbstractStateWatcher() {
		_predicates = new Vector<StatePathPredicate>();

		String[] paths = getPredicates();
		
		for( String path : paths)
			_predicates.add( StatePathPredicate.parsePath(path));
	}

	
	/**
	 * Override this method.
	 * @return an array of Strings, each a StatePathPredicate.
	 */
	protected String[] getPredicates() {
		// Dummy value;
		return new String[0];
	}

	public Collection<StatePathPredicate> getPredicate() {
		return _predicates;
	}
	
	/**
	 * Since we expect a single instance per class, just return the simple class name.
	 */
	public String toString() {
		return this.getClass().getSimpleName();
	}
}
