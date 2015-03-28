/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.Collection;

/**
 * Some objects want to know when (some portion of) dCache state changes.
 * These object's class must implement this interface.  They must
 * also be registered with the State object to have any effect.
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
     * This method is called when a pending transition alters
     * one (or more) metrics that match a StatePathPredicate
     * from {@link getPredicate}.
     * <p>
     * If the StateWatcher is acting as a secondary information provider, so
     * maintains derived metrics, it may choose to update those metrics
     * based on the values that are to change in the forthcoming transition.
     * If this is so, the new metric values are to be added to the provided
     * StateUpdate object.
     */
    public void trigger(StateUpdate update, StateExhibitor currentState, StateExhibitor futureState);
}
