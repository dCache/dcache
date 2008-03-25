/**
 * 
 */
package org.dcache.services.info.base;

import java.util.Iterator;
import java.util.Set;



/**
 * A StatePathPredicate indicates interest in a particular part of the dCache state
 * tree.  It is an extension of the StatePath in that, logically, any StatePath can
 * be used to construct a StatePathPredicate.
 * <p>
 * The principle usage of the StatePathPredicate is select some subset of the values
 * within dCache's state.  To do this, the <tt>matches()</tt> method should be used.
 * <p>
 * When testing whether a StatePath matches, a StatePathPredicate considers the
 * asterisk character ('*') to be a wildcard and will match any corresponding value
 * in the StatePath.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePathPredicate extends StatePath {
	
	/**
	 * Parse a dot-separated path to build a StatePathPredicate 
	 * @param path the path, as an ordered list of path elements, each element separated by a dot.
	 * @return the corresponding StatePath.
	 */
	static public StatePathPredicate parsePath( String path) {
		String elements[] = path.split("\\.");
		return new StatePathPredicate( elements);
	}

	
	public StatePathPredicate( StatePath path) {
		super( path);
	}
	
	public StatePathPredicate( String path) {
		super( path);
	}
	
	private StatePathPredicate( String[] elements) {
		super( elements);
	}

	/**
	 * Indicate whether a particular StatePath matches the
	 * predicate.
	 * 
	 * @param path the particular path within dCache's state
	 * @return true if this path matches this predicate, false otherwise.
	 */
	public boolean matches(StatePath path) {
		
		Iterator<String> myItr = this._elements.iterator();
		for( String pathElement : path._elements) {
			String myElement = myItr.next();
			if( !pathElement.equals(myElement) && !myElement.equals("*"))
				return false;
		}
		
		return true;
	}
	
	
	/**
	 * Check whether any of the StatePaths in the paths matches.
	 * @param paths the set of StatePaths to consider
	 * @return true if any match, false otherwise.
	 */
	public boolean anyPathMatches( Set<StatePath> paths) {
		for( StatePath path : paths)
			if( this.matches(path))
				return true;
		
		return false;
	}
	
	public StatePathPredicate childPath() {
		return new StatePathPredicate( super.childPath());
	}
	
	/**
	 * Return true if the top-most element of this predicate matches the given String.
	 * NB the String must have been intern().  This is true for all string-literals and
	 * all elements within a StatePath.
	 * @param name the name of the child element
	 * @return true if child element matches top-most element, false otherwise
	 */
	public boolean topElementMatches( String name) {
		String topElement = _elements.get(0);
		
		if( topElement == "*")
			return true;
		
		if( topElement == name)
			return true;
		
		// TODO: Support for Regular Expression here?
		// e.g. test first char matches '{', last char matches '}' then compile the RegExp state machine
		// and check; as an optimisation, compile the RegExp once and store it.
		
		return false;
	}
}
