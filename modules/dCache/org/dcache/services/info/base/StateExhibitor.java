package org.dcache.services.info.base;

/**
 * A Class that implement StateExhibitor allows objects enquire about the
 * current state of dCache. This is achieved by implementing the Visitor
 * pattern: a class that wishes to discover some aspect of dCache's state
 * must implement the StateVisitor interface.
 * <p>
 * The class implementing StateExhibitor must ensure the self-consistency of
 * the data produced. This is likely achieved by holding some form of
 * read-lock. It is desirable that these locks are not held for an excessive
 * time; therefore, it is important that the class implementing StateVisitor
 * does not undertake activity that is likely to block activity of the Thread
 * for an unpredictable or a long time.
 * <p>
 * There is support for querying the future state of dCache after some
 * changes take place. This support is available as a duplicate set of
 * methods that accept the StateTransition object that represents the changes
 * to dCache state.
 */
public interface StateExhibitor {

    /**
     * Query the current state of dCache. All of the state is visited.
     *
     * @param visitor
     *            the StateVisitor object that compiles the information about
     *            dCache state.
     */
    public void visitState( StateVisitor visitor);

    /**
     * Query the future state of dCache after a StateTransition has been
     * applied. All of the state is visited.
     *
     * @param visitor
     *            the StateVisitor that compiles the information about future
     *            dCache state.
     * @param transition
     *            the change that will be applied.
     */
    public void visitState( StateVisitor visitor, StateTransition transition);

    /**
     * Query the current state of dCache. Those elements leading up to the
     * start StatePath will be visited with special methods.
     *
     * @param visitor
     * @param start
     */
    public void visitState( StateVisitor visitor, StatePath start);

    /**
     * Visit the future state of dCache after a StateTransition has been
     * applied. Those elements leading up to the start StatePath will be
     * visited with special methods.
     *
     * @param transition
     * @param visitor
     * @param start
     */
    public void visitState( StateTransition transition, StateVisitor visitor,
                            StatePath start);
}
