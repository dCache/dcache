package org.dcache.services.info.serialisation;

import org.dcache.services.info.base.StateGuide;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StateVisitor;
import org.dcache.services.info.base.guides.SubtreeStateGuide;
import org.dcache.services.info.base.guides.VisitEverythingStateGuide;

/**
 * The SubtreeVisitor allows one to visit either the whole dCache
 * state or some subtree of the state.  The scope of the visiting
 * may be changed.
 */
public abstract class SubtreeVisitor implements StateVisitor {

    private StateGuide _guide;

    public SubtreeVisitor() {
        setVisitScopeToEverything();
    }

    public SubtreeVisitor(StatePath subtreeRoot) {
        setVisitScopeToSubtree(subtreeRoot);
    }

    protected void setVisitScopeToEverything() {
        _guide = new VisitEverythingStateGuide();
    }

    protected void setVisitScopeToSubtree(StatePath subtreeRoot) {
        _guide = new SubtreeStateGuide(subtreeRoot);
    }

    @Override
    public boolean isVisitable(StatePath path) {
        return _guide.isVisitable(path);
    }

    protected boolean isInsideScope(StatePath path) {
        if (_guide instanceof SubtreeStateGuide) {
            SubtreeStateGuide ssg = (SubtreeStateGuide) _guide;
            return ssg.isInSubtree(path);
        } else {
            return true;
        }
    }
}
