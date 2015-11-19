package org.dcache.services.info.base;

/**
 * A Class that implement StateExhibitor allows objects enquire about the
 * state of dCache. This is achieved by implementing the Visitor
 * pattern: a class that wishes to discover some aspect of dCache's state
 * must implement the StateVisitor interface.
 * <p>
 * The class implementing StateExhibitor must ensure the self-consistency of
 * the data produced. This is likely achieved by holding some form of
 * read-lock. It is desirable that these locks are not held for an excessive
 * time; therefore, it is important that the class implementing StateVisitor
 * does not undertake activity that is likely to block activity of the Thread
 * for an unpredictable or a long time.
 */
public interface StateExhibitor
{
    /**
     * Query the state of dCache.
     */
    void visitState(StateVisitor visitor);
}
