/**
 * 
 */
package org.dcache.services.info.base;

import java.util.*;


/**
 * A collection of StatePaths that indicate a set of sub-trees of interest.
 *  
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePathPredicate {

	Collection<StatePath> _paths;
	
	public StatePathPredicate( StatePath path) {
		_paths = new HashSet<StatePath>();
		_paths.add(path);
	}
	
	public StatePathPredicate( Collection<StatePath> paths) {
		_paths = new HashSet<StatePath>( paths);
	}
	
	public StatePathPredicate( String path) {
		StatePath statePath = new StatePath(path);
		_paths = new HashSet<StatePath>();
		_paths.add( statePath);
	}
	
	public StatePathPredicate( String path[]) {
		Collection<StatePath> paths = StatePath.newPathCollection(path);
		_paths = new HashSet<StatePath>( paths);
	}
		
	/**
	 * Indicate whether a particular StatePath matches the
	 * predicate.
	 * 
	 * @param path the particular path within dCache's state
	 * @return true if this path matches this predicate, false otherwise.
	 */
	public boolean matches(StatePath path) {
		
		for( StatePath thisPath : _paths) 
			if( thisPath.equalsOrHasChild(path))
				return true;

		return false;
	}
	
	/**
	 * Given a collection of StatePath objects, does at least one
	 * of them give matches() true?
	 * 
	 * This is a default implementation, subclasses may have a
	 * better way of implementing this.
	 * @param pathCol a Collection of StatePaths
	 * @return true if there is at least on member of pathCol that
	 * matches() would return true, false otherwise.
	 */
	public boolean matches(Collection<StatePath> pathCol) {

		// TODO: this is a brute-force approach; we should be able to do better.
		for( StatePath matchingPath : pathCol )
			if( matches(matchingPath))
				return true;
		
		return false;
	}
}
