package org.dcache.services.info.base;

import java.util.Date;



/**
 * All nodes within the State composition must implement this interface.  Nodes are
 * either StateComposite class (i.e., branches) or sub-classes of the abstract StateValue
 * (i.e., data).
 * <p>
 * StateComponents may be mortal, ephemeral or immortal.  If mortal, then the
 * <code>getExpiryDate()</code> method specifies when this StateComponent should be removed.  Ephemeral
 * StateComponents do not have a built-in expire time but do not affect the lifetime of their parent
 * StateComposite.  Immortal StateComponents never expire.
 * <p>
 * This class implements the Composite pattern and includes support for the visitor pattern.
 * @author Paul Millar <paul.millar@desy.de>
 */
interface StateComponent
{
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
     * @param visitor the object that operations should be preformed on.
     */
    void acceptVisitor(StatePath path, StateVisitor visitor);

    /**
     * As above, but after the effects of a transition.
     * @param transition
     * @param path
     * @param visitor
     */
    void acceptVisitor(StateTransition transition, StatePath path, StateVisitor visitor);


    /**
     * Check whether a predicate has been triggered
     * @param ourPath  The StatePath to this StateComponent
     * @param predicate The predicate under consideration
     * @param transition the StateTransition under effect.
     * @return true if the StatePathPredicate has been triggered, false otherwise
     */
    boolean predicateHasBeenTriggered(StatePath ourPath, StatePathPredicate predicate, StateTransition transition);

    /**
     * Apply a transformation, updating the live set of data.
     * @param ourPath the StatePath to this StateComponent
     * @param transition the StateTransition that should be applied.
     */
    void applyTransition(StatePath ourPath, StateTransition transition);

    /**
     * Update a StateTransition based adding a new metric.
     * @param ourPath the StatePath of this component
     * @param childPath the StatePath, relative to this component, of the new metric
     * @param newChild the new metric value.
     * @param transition the StateTransition to update.
     * @throws MetricStatePathException
     */
    void buildTransition(StatePath ourPath, StatePath childPath, StateComponent newChild, StateTransition transition) throws MetricStatePathException;

    /**
     * This method returns the Date at which this StateComponent should be removed from the state.
     * @return the Date when an object should be removed, or null indicating that either is never to
     * be removed or else the removal time cannot be predicted in advance.
     */
    Date getExpiryDate();


    /**
     *  This method returns the earliest Date that any Mortal StateComponent underneath this
     *  StateComponent will expire.  This includes children of children and so on.  If this
     *  StateComponent contains no Mortal children then null is returned.
     * @return the earliest Date when a Mortal child will expire.
     */
    Date getEarliestChildExpiryDate();

    /**
     * Update a StateTransition based on this StateComponent's children.
     * @param ourPath this StateComponent's path within dCache tree.  For the top-most
     * StateComponent this is null.
     * @param transition the StateTransition object within which we should register children to be deleted.
     * @param forced whether we should simply remove our children, or test whether they are to be deleted
     */
    void buildRemovalTransition(StatePath ourPath, StateTransition transition, boolean forced);


    /**
     *  Update a StateTransition so all components below remainingPath from the current
     *  StateComponent will be removed.  This is equivalent to
     *  buildRemovalTransition(..., transition, true)
     *  for the top-most element in the named subtree.
     */
    void buildPurgeTransition(StateTransition transition, StatePath ourPath, StatePath remainingPath);

    /**
     * Whether this component has expired.
     * @return true if the parent object should remove this object, false otherwise.
     */
    boolean hasExpired();

    boolean isEphemeral();
    boolean isImmortal();
    boolean isMortal();
}
