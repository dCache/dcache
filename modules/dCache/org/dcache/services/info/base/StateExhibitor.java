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
     * Query the current state of dCache.
     */
    public void visitState( StateVisitor visitor);

    /**
     * Query the future state of dCache after a StateTransition has been
     * applied.
     */
    public void visitState( StateVisitor visitor, StateTransition transition);
}
