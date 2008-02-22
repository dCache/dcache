/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.*;

/**
 *  StatePath provides a representation of a value's location within the
 *  dCache State.  A path consists of an ordered list of path elements, each
 *  element is a String.
 *  <p>
 *  In addition to the constructor, various methods exist to create derived
 *  paths: StatePaths that are, in some sense, relative to an existing
 *  StatePath; for example, <tt>newChild()</tt> and <tt>childPath()</tt>.
 *  <p>
 *  The constructor provides an easy method of creating a complex StatePath.
 *  It will parse the String and split it at dot boundaries, forming the
 *  path.  Some paths may have elements that contain dots.  To construct
 *  corresponding StatePath representations, use the <tt>newChild()</tt>
 *  method. 
 *  
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePath {
	
	List<String> _elements;

	/**
	 * Build a Collection of StatePaths from an array of paths.
	 * @param paths an array of paths
	 * @return a HashSet-backed Collection of StatePaths
	 */
	static public Collection<StatePath> newPathCollection( String paths[]) {
		Collection<StatePath> collection = new HashSet<StatePath>( paths.length);
		
		for( int i=0; i < paths.length; i++)
			collection.add(new StatePath(paths[i]));
		
		return collection;
	}
	
	
	private StatePath() {
		_elements = new ArrayList<String>();
	}


	/**
	 * Create a new StatePath that duplicates an existing one. 
	 * @param path the StatePath to copy.
	 */
	public StatePath( StatePath path) {
		_elements = new ArrayList<String>( path._elements);
	}
	
	/**
	 * Create a new StatePath based on a List of path elements.
	 * @param elements
	 */
	private StatePath( List<String> elements) {
		_elements = new ArrayList<String>();
		_elements.addAll(elements);	
	}
	
	
	/**
	 * Provide a new StatePath by parsing a String representation.
	 * @param path: the path, as a dot-seperated list of elements.  
	 */
	public StatePath( String path) {
		String elements[] = path.split("\\.");
		
		_elements = new ArrayList<String>(elements.length);
		for(int i=0; i < elements.length; i++)
			_elements.add(elements[i]);
	}

	/**
	 * Check whether another path points to the same location.
	 * @param otherPath: the other path to compare
	 * @return: whether the other path point to the same location.
	 */
	public boolean equals( StatePath otherPath) {
		if( otherPath == null)
			return false;
		
		return _elements.equals(otherPath._elements);
	}
	
	
	/**
	 * Check whether otherPath points to the same location, or
	 * is a child of this path.  This is true iff each element of
	 * this path is identical to the corresponding element in otherPath.
	 * <pre>
	 *  StatePath p1 = new StatePath( "foo.bar");
	 *  StatePath p2 = new StatePath( "foo.bar.baz");
	 *  
	 *  p1.equalsOrHasChild( p1) // true
	 *  p2.equalsOrHasChild( p2) // true
	 *  p1.equalsOrHasChild( p2) // true
	 *  p2.equalsOrHasChild( p1) // false
	 * </pre>
	 * @param otherPath the potential child path 
	 * @return true if otherPath is a child of this path.
	 */
	public boolean equalsOrHasChild( StatePath otherPath) {

		// Check for an obviously mismatch.
		if( _elements.size() > otherPath._elements.size())
			return false;

		for( int i = 0; i < _elements.size(); i++) {
			
			String thisElement = _elements.get(i);
			
			if( !thisElement.equals( otherPath._elements.get(i)))
				return false;
		}
		
		return true;
	}
	
	/**
	 * Check whether otherPath points to a location that is a child of this location.  This
	 * is true iff each element of this path is identical to the corresponding element in
	 * otherPath and otherPath has length precisely greater by one. 
	 * <pre>
	 *  StatePath p1 = new StatePath( "foo.bar");
	 *  StatePath p2 = new StatePath( "foo.bar.baz");
	 *  StatePath p3 = new StatePath( "foo.bar.baz.other");
	 *  
	 *  p1.isParentOf( p1) // false
	 *  p1.isParentOf( p2) // true
	 *  p1.isParentOf( p3) // false
	 *  p2.isParentOf( p1) // false
	 *  p2.isParentOf( p2) // false
	 *  p2.isParentOf( p3) // true
	 * </pre>
	 * 
	 * @param otherPath
	 * @return
	 */
	public boolean isParentOf( StatePath otherPath) {
		if( (_elements.size() + 1) != otherPath._elements.size())
			return false;
		
		for( int i = 0; i < _elements.size(); i++)
			if( !_elements.get(i).equals( otherPath._elements.get(i)))
				return false;
		
		return true;
	}

	
	/**
	 * Convert a StatePath to it's corresponding string value.  This
	 * is identical to calling toString( ".");
	 */
	public String toString() {
		return toString(".");
	}
	
	/**
	 * Convert a StatePath to a String representation.  Each element is
	 * separated by the separator String. 
	 * @param separator the String to seperate each path element
	 * @return the String representation.
	 */
	public String toString( String separator) {
		StringBuffer out = new StringBuffer();
		
		for( Iterator<String> itr = _elements.iterator(); itr.hasNext();) {
			String e = itr.next();
			if( out.length() > 0)
				out.append(separator);
			out.append(e);
		}
		
		return out.toString();		
	}
	
	/**
	 * @return the first element of the path.
	 */
	public String getFirstElement() {
		return _elements.get(0);
	}
	
	/**
	 * @return the last element of the path.
	 */
	public String getLastElement() {
		return _elements.get( _elements.size()-1);
	}
	
	
	/**
	 * Create a new StatePath with an extra path element.  This method does no
	 * splitting of the parameter: it is safe to pass a String with dots.
	 * <p>
	 * If you want to create a newChild with dot-splitting one solution is
	 * to first create a StatePath with the new path:
	 * <p>
	 * <pre>
	 *     path = path.newChild( new StatePath( pathWithDots));
	 * </pre>
	 * 
	 * @param element: the name of the child path element
	 * @return a new StatePath with the additional, final path element
	 */
	public StatePath newChild( String element) {
		
		
		StatePath newPath = new StatePath( _elements);
		
		newPath._elements.add( new String(element));
		
		return newPath;
	}
	
	/**
	 * Create a new StatePath with extra path elements; for example, if the path is
	 * representing <tt>aa.bb</tt>, then <tt>path.newChild("cc")</tt> will return
	 * a new StatePath representing <tt>aa.bb.cc</tt>
	 *  <p>
	 * @param subPath: the extra path elements to append.
	 * @return a new StatePath with a combined path.
	 */
	public StatePath newChild( StatePath subPath) {

		StatePath newPath = new StatePath( _elements);
		
		newPath._elements.addAll( subPath._elements);

		return newPath;		
	}
	
	
	/**
	 * Build a new StatePath that points to the same location from the immediate
	 * child's point-of-view.  For example, if the
	 * current path is charactised as <tt>aa.bb.cc</tt>, then
	 * the returned StatePath is characterised by <tt>bb.cc</tt>.
	 * <p>
	 * If the path has no children of children, null is returned. 
	 * 
	 * @return the path for the child element, or null if there is no child.
	 */
	public StatePath childPath() {
		if( _elements == null || _elements.size() <= 1)
			return null;
		
		return new StatePath( _elements.subList(1, _elements.size()));
	}
	
	/**
	 * Build a new StatePath that points to this StatePath node's parent node; for example,
	 * if <tt>path</tt> is characterised by <tt>aa.bb.cc</tt> then <tt>path.parentPath()</tt>
	 * returns a new StatePath that is characterised by <tt>aa.bb</tt>
	 * @return the new StatePath, pointing to the parent, or null if the node has no parent.
	 */
	public StatePath parentPath() {
		if( _elements.size() <= 1)
			return null;
		
		return new StatePath( _elements.subList(0, _elements.size()-1));		
	}
	
	/**
	 * Check whether this path contains any branches; i.e., if the number of elements
	 * in the path is strictly greater than one.
	 * @return true if the path contains no branches, false otherwise.
	 */
	public boolean isSimplePath() {
		return _elements.size() == 1;
	}	
}
