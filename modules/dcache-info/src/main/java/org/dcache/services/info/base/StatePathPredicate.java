/**
 *
 */
package org.dcache.services.info.base;

import java.util.Iterator;



/**
 * A StatePathPredicate indicates interest in a particular part of the dCache state
 * tree.  It is an extension of the StatePath in that, logically, any StatePath can
 * be used to construct a StatePathPredicate.
 * <p>
 * The principle usage of the StatePathPredicate is select some subset of the values
 * within dCache's state.  To do this, the <tt>matches()</tt> method should be used.
 * <p>
 * When testing whether a StatePath matches, a StatePathPredicate considers the
 * asterisk character ('*') to be a wildcard and will match any corresponding value
 * in the StatePath.
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePathPredicate extends StatePath
{
    private static final String WILDCARD_ELEMENT = "*";


    /**
     * Parse a dot-separated path to build a StatePathPredicate
     * @param path the path, as an ordered list of path elements, each element separated by a dot.
     * @return the corresponding StatePath.
     */
    public static StatePathPredicate parsePath(String path)
    {
        if (path == null) {
            return null;
        }

        String elements[] = path.split("\\.");
        return new StatePathPredicate(elements);
    }


    /**
     * Whether two elements are considered matching.
     * @param predicateElement
     * @param pathElement
     * @return true if pathElement matches predicateElement
     */
    private static boolean elementsMatch(String predicateElement, String pathElement)
    {
        if (pathElement == null || predicateElement == null) {
            return false;
        }

        if (predicateElement.equals(WILDCARD_ELEMENT)) {
            return true;
        }

        if (pathElement.equals(predicateElement)) {
            return true;
        }

        return false;
    }

    public StatePathPredicate(StatePath path)
    {
        super(path);
    }

    public StatePathPredicate(String path)
    {
        super(path);
    }

    private StatePathPredicate(String[] elements)
    {
        super(elements);
    }

    /**
     * Indicate whether a particular StatePath matches the
     * predicate.  A match is where each element of this predicate matches
     * the corresponding element of the StatePath.  The StatePath length must
     * be equal to or greater than this StatePathPredicte.
     *
     * @param path the particular path within dCache's state.
     * @return true if this path matches this predicate, false otherwise.
     */
    public boolean matches(StatePath path)
    {
        if (path == null) {
            return false;
        }

        if (path._elements.size() != this._elements.size()) {
            return false;
        }

        Iterator<String> myItr = this._elements.iterator();

        for (String pathElement : path._elements) {

            String myElement = myItr.next();

            if (!StatePathPredicate.elementsMatch(myElement, pathElement)) {
                return false;
            }
        }

        return true;
    }


    /**
     * Build a new StatePathPredicate that matches the childPaths of the StatePaths
     * that match this StatePathPredicate.  For example, if the current
     * StatePathPredicate is characterised as <tt>aa.bb.*.cc</tt>, then
     * the returned StatePathPredicate is characterised by <tt>bb.*.cc</tt>.
     * <p>
     * If the StatePathPredicate has no children of children, null is returned.
     *
     * @return the path for the child element, or null if there is no child.
     */
    @Override
    public StatePathPredicate childPath()
    {
        StatePath childPath = super.childPath();
        return childPath == null ? null : new StatePathPredicate(childPath);
    }


    /**
     * Return true if the top-most element of this predicate matches the given String.
     * @param name the name of the child element
     * @return true if child element matches top-most element, false otherwise
     */
    public boolean topElementMatches(String name)
    {
        return StatePathPredicate.elementsMatch(_elements.get(0), name);
    }
}
