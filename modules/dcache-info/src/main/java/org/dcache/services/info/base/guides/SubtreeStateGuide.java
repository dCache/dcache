package org.dcache.services.info.base.guides;

import org.dcache.services.info.base.StateGuide;
import org.dcache.services.info.base.StatePath;

/**
 * A SubtreeStateGuide allows a StateVisitor to visit all of a specific
 * subtree. The class allows descent into all StateComponents that are an
 * ancestor of the subtree root (so allowing the visit to reach the subtree
 * root) and all StateComponents that have the subtree root as an ancestor
 * (i.e., visiting the subtree).
 */
public class SubtreeStateGuide implements StateGuide {

    private final StatePath _subtreeRoot;

    public SubtreeStateGuide(StatePath root) {
        _subtreeRoot = root;
    }

    @Override
    public boolean isVisitable(StatePath path) {

        if (isRootPath(path)) {
            return true;
        }

        if (isRootPath(_subtreeRoot)) {
            return true;
        }

        if (path.equalsOrHasChild(_subtreeRoot)) {
            return true;
        }

        if (_subtreeRoot.equalsOrHasChild(path)) {
            return true;
        }

        return false;
    }

    private boolean isRootPath(StatePath path) {
        return path == null;
    }

    public boolean isInSubtree(StatePath path) {
        return _subtreeRoot.equalsOrHasChild(path);
    }

    @Override
    public String toString() {
        return "{SubtreeStateGuide: " + _subtreeRoot + "}";
    }
}
