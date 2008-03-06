/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.Collection;

/**
 * Some objects want to know when (some portion of) dCache state changes.
 * These object's class must implement this interface.  They must
 * also be registered with the State object to have any effect.
 * 
 * @author Paul Millar <paul.millar@desy.de>
 */
public interface StateWatcher {

	/**
	 * Provide access to a Set of StatePathPredicates.  These describe
	 * which subset of the total dCache state this watcher is
	 * interested in; specifically, which values, if changed, may result
	 * in evaluate() returning a different result.
	 * @return a Set of predicates
	 */
	public Collection<StatePathPredicate> getPredicate();
	
	/**
	 * Evaluate the forthcoming changes and calculate some
	 * derived data.
	 * @return the new values for the dCache State.  This update
	 * should <i>not</i> contain any of the supplied information,
	 * or null if no changes are necessary.
	 */
	public void trigger(StateTransition str);	
}
