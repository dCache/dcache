package org.dcache.services.info.base;

import java.util.*;

/**
 * All nodes within the State componsition must implement this interface.
 * This is modelled on the Composite pattern.
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface StateComponent {

	/**
	 * Needed for the Visitor pattern: this method performs actions on the StateVisitor
	 * object via the methods defined in the interface.
	 * <p>
	 * Visiting a tree involves iterating over all StateComponent and invoking the corresponding
	 * method in the StateVisitor object.
	 * <p>
	 * The start parameter allows an initial skip to a predefined location, before iterating over
	 * all sub-StateComponents.  In effect, this allows visiting over only a sub-tree.
	 * 
	 * @param path this parameter informs an StateComponent of its location within the tree so the visit methods can include this information. 
	 * @param start the point in the tree to start visiting all children, or null to visit the whole tree.
	 * @param visitor the object that operations should be preformed on.
	 */
	void acceptVisitor( StatePath path, StatePath start, StateVisitor visitor);

	/**
	 * Add a new StateComponent at a certain point within the state tree.
	 * Adding a StateComponent will replace the existing object.
	 *   
	 * @param path the point at which the component should be added.
	 * @param value the component (metric) to add to the state.
	 * @throws BadStatePathException if it was impossible to add the metric at the requested path. 
	 */
	void add( StatePath path, StateComponent value) throws BadStatePathException;
	
	/**
	 * This function returns the Date at which this StateComponent should be removed from the state.
	 * This should be used for optimising removal times, but <i>not</i> for checking whether a
	 * component should be removed: that must be done with the <tt>hasExpired()</tt> method.
	 * @return the Date when an object should be removed, or null indicating that either is never to
	 * be removed or else the removal time cannot be predicted in advance. 
	 */
	public Date getExpiryDate();

	/**
	 * Whether this component has expired.
	 * @return true if the parent object should remove this object, false otherwise.
	 */
	boolean hasExpired();
}
