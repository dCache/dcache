/**
 * 
 */
package org.dcache.services.info.secondaryInfoProviders;

import org.dcache.services.info.base.StatePathPredicate;
import org.dcache.services.info.base.StateWatcher;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Provide a skeleton, basic implementation of a StateWatcher with
 * static StatePathPredicates.  Sub-classes of this class must
 * implement a getPredicates() method that returns an array of
 * Strings (the predicates).   
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
abstract public class AbstractStateWatcher implements StateWatcher {

	private final Collection<StatePathPredicate> _predicates = new ArrayList<StatePathPredicate>();
	
	public AbstractStateWatcher() {
		String[] paths = getPredicates();
		
		for( String path : paths)
			_predicates.add( StatePathPredicate.parsePath(path));
	}

	
	/**
	 * Override this method.  The method must return valid output
	 * when called from the constructor.
	 * @return an array of Strings, each a StatePathPredicate.
	 */
	abstract protected String[] getPredicates();

	
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
