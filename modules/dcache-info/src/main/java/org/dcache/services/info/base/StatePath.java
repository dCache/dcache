/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import java.util.ArrayList;
import java.util.List;



/**
 *  StatePath provides a representation of a value's location within the
 *  dCache State.  A path consists of an ordered list of path elements, each
 *  element is a String.
 *  <p>
 *  In addition to the constructor, various methods exist to create derived
 *  paths: StatePaths that are, in some sense, relative to an existing
 *  StatePath; for examples of these, see the <tt>newChild()</tt> and
 *  <tt>childPath()</tt> methods.
 *  <p>
 *  The constructor provides an easy method of creating a complex StatePath.
 *  It will parse the String and split it at dot boundaries, forming the
 *  path.  Some paths may have elements that contain dots.  To construct
 *  corresponding StatePath representations, use the <tt>newChild()</tt>
 *  method.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StatePath {

    private static final int NULL_ELEMENT_HASH = 0xDEADBEAF;

    protected final List<String> _elements;
    private int _myHashCode;
    private boolean _haveHashCode;
    private String _toString;


    /**
     * Parse a dot-separated path to build a StatePath
     * @param path the path, as an ordered list of path elements, each element separated by a dot.
     * @return the corresponding StatePath.
     */
    static public StatePath parsePath(String path) {
        String elements[] = path.split("\\.");
        return new StatePath(elements);
    }

    /**
     * Create a new StatePath that duplicates an existing one.
     * @param path the StatePath to copy.
     */
    protected StatePath(StatePath path) {
        _elements = new ArrayList<>(path._elements);
    }

    /**
     * Create a new StatePath when given a List of path elements.
     * @param pathElements  The List of path elements from which to construct the state path.
     * @return the corresponding StatePath
     */
    static public StatePath buildFromList(List<String> pathElements ) {
        return new StatePath(pathElements, pathElements.size());
    }


    /**
     * Create a new StatePath based on a List of path elements.
     * @param elements
     */
    private StatePath(List<String> elements, int elementCount) {
        _elements = new ArrayList<>(elementCount);

        for (String element : elements) {
            _elements.add(element.intern());
        }
    }

    /**
     * Build a new StatePath based on an array of elements.
     * @param elements the path elements, in order.
     */
    protected StatePath (String[] elements) {
        _elements = new ArrayList<>(elements.length);

        for (String element : elements) {
            _elements.add(element.intern());
        }
    }

    /**
     * Provide a new StatePath with a single path element.  The result is the
     * same as new StatePath().newChild(name);
     * @param name: the name of the path element.
     */
    public StatePath(String element) {
        _elements = new ArrayList<>(1);
        _elements.add(element != null ? element.intern() : null);
    }


    /**
     *  Calculate the hash code and store it for later quick reference.
     */
    void calcHashCode() {
        int code = 0;
        int elementCount = 0;

        for (String element : _elements) {
            int stringHash=0;

            if (element == null) {
                stringHash = NULL_ELEMENT_HASH;
            } else {
                // Since Java's String hashCode is so poor, spice it up a little.
                byte bytes[] = element.getBytes();

                int len = bytes.length > 10 ? 10 : bytes.length; // limit length
                for (int i = 0; i < len; i++) {
                    stringHash ^= (bytes[i]) << (i * 5 + elementCount) % 24;
                }
            }

            code ^= stringHash;
            elementCount++;
        }



        _myHashCode = code;
        _haveHashCode = true;
    }


    /**
     * Check whether another path points to the same location.
     * @param otherPath: the other path to compare
     * @return: whether the other path point to the same location.
     */
    @Override
    public boolean equals(Object otherObject) {
        if (!(otherObject instanceof StatePath)) {
            return false;
        }

        if (otherObject == this) {
            return true;
        }

        StatePath otherPath = (StatePath) otherObject;

        if (otherPath._elements.size() != _elements.size()) {
            return false;
        }

        for (int i=0; i < _elements.size(); i++) {
            if (otherPath._elements.get(i) != _elements
                    .get(i))  // Our use of intern() allows this
            {
                return false;
            }
        }

        return true;
    }


    /**
     * Overload the hashCode to honour the contract:
     *    A.hashCode()==B.hashCode() =>  A.equals(B)
     */
    @Override
    public int hashCode() {
        if (!_haveHashCode) {
            calcHashCode();
        }

        return _myHashCode;
    }


    /**
     * Check whether otherPath points to the same location, or
     * is a child of this path.  This is true iff each element of
     * this path is identical to the corresponding element in otherPath.
     * <pre>
     *  StatePath p1 = new StatePath("foo.bar");
     *  StatePath p2 = new StatePath("foo.bar.baz");
     *
     *  p1.equalsOrHasChild(p1) // true
     *  p2.equalsOrHasChild(p2) // true
     *  p1.equalsOrHasChild(p2) // true
     *  p2.equalsOrHasChild(p1) // false
     * </pre>
     * @param otherPath the potential child path
     * @return true if otherPath is a child of this path.
     */
    public boolean equalsOrHasChild(StatePath otherPath) {


        /**
         *  A StatePath variable with value null corresponds to
         *  the root element of the info tree.  All non-null
         *  StatePath objects correspond to a location in the
         *  info tree below the root element.
         *
         *  When given null as an argument, the query is against
         *  the root element.  Any object where the
         *  equalsOrHasChild() method is being called is not
         *  null (obviously), therefore is not the root
         *  element.
         */
        if (otherPath == null) {
            return false;
        }

        // Check for an obviously mismatch.
        if (_elements.size() > otherPath._elements.size()) {
            return false;
        }

        for (int i = 0; i < _elements.size(); i++) {
            // We use intern()ed strings for this to work.
            if (_elements.get(i) != otherPath._elements.get(i)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check whether otherPath points to a location that is a child of this location.  This
     * is true iff each element of this path is identical to the corresponding element in
     * otherPath and otherPath has length precisely greater by one.
     * <pre>
     *  StatePath p1 = new StatePath("foo.bar");
     *  StatePath p2 = new StatePath("foo.bar.baz");
     *  StatePath p3 = new StatePath("foo.bar.baz.other");
     *
     *  p1.isParentOf(p1) // false
     *  p1.isParentOf(p2) // true
     *  p1.isParentOf(p3) // false
     *  p2.isParentOf(p1) // false
     *  p2.isParentOf(p2) // false
     *  p2.isParentOf(p3) // true
     * </pre>
     *
     * @param otherPath
     * @return
     */
    public boolean isParentOf(StatePath otherPath) {

        if (otherPath == null) {
            return false;
        }

        if ((_elements.size() + 1) != otherPath._elements.size()) {
            return false;
        }

        for (int i = 0; i < _elements.size(); i++) {
            if (_elements.get(i) != otherPath._elements
                    .get(i))         // intern()ed Strings allows this.
            {
                return false;
            }
        }

        return true;
    }


    /**
     * Convert a StatePath to it's corresponding string value.  This
     * is identical to calling toString(".");
     */
    @Override
    public String toString() {
        if (_toString == null) {
            _toString = toString(".", 0);
        }

        return _toString;
    }

    /**
     * Convert a StatePath to a String representation.  Each element is
     * separated by the separator String.
     * @param separator the String to separate each path element
     * @param don't display this number of initial elements
     * @return the String representation.
     */
    public String toString(String separator, int count) {
        StringBuilder out = new StringBuilder();
        int i=0;

        for (String e : _elements) {

            if (i >= count) {
                if (i > count) {
                    out.append(separator);
                }

                out.append(e);
            }

            i++;
        }

        return out.toString();
    }


    /**
     * Create a String representing this state with some initial elements removed.
     * The number of suppressed initial elements is the same as the number of
     * elements in the prefix StatePath
     * @param prefix the prefix to remove.
     * @return a string representation
     */
    public String toString(StatePath prefix) {
        int count = prefix != null ? prefix._elements.size() : 0;
        return toString(".", count);
    }

    /**
     * @return the first element of the path.
     */
    public String getFirstElement() {
        return _elements.get(0);
    }

    /**
     * @return the last element of the path.
     */
    public String getLastElement() {
        return _elements.get(_elements.size()-1);
    }


    /**
     * Create a new StatePath with an extra path element.  This method does no
     * splitting of the parameter: it is safe to pass a String with dots.
     * <p>
     * If you want to create a newChild with dot-splitting, one solution is
     * to first create a StatePath with the new path:
     * <p>
     * <pre>
     *     path = path.newChild(StatePath.parsePath(pathWithDots));
     * </pre>
     *
     * @param element: the name of the child path element
     * @return a new StatePath with the additional, final path element
     */
    public StatePath newChild(String element) {

        StatePath newPath = new StatePath(_elements, _elements.size() + 1);

        newPath._elements.add(element.intern());

        return newPath;
    }

    /**
     * Create a new StatePath with extra path elements; for example, if the path is
     * representing <tt>aa.bb</tt>, then <tt>path.newChild("cc")</tt> will return
     * a new StatePath representing <tt>aa.bb.cc</tt>
     *  <p>
     * @param subPath: the extra path elements to append.
     * @return a new StatePath with a combined path.
     */
    public StatePath newChild(StatePath subPath) {

        StatePath newPath = new StatePath(_elements, _elements.size() + subPath._elements.size());

        newPath._elements.addAll(subPath._elements);

        return newPath;
    }


    /**
     * Build a new StatePath that points to the same location from the immediate
     * child's point-of-view.  For example, if the
     * current path is characterised as <tt>aa.bb.cc</tt>, then
     * the returned StatePath is characterised by <tt>bb.cc</tt>.
     * <p>
     * If the path has no children of children, null is returned.
     *
     * @return the path for the child element, or null if there is no child.
     */
    public StatePath childPath() {
        if (_elements == null || _elements.size() <= 1) {
            return null;
        }

        return new StatePath(_elements.subList(1, _elements.size()), _elements.size()-1);
    }

    /**
     * Build a new StatePath that points to this StatePath node's parent node; for example,
     * if <tt>path</tt> is characterised by <tt>aa.bb.cc</tt> then <tt>path.parentPath()</tt>
     * returns a new StatePath that is characterised by <tt>aa.bb</tt>
     * @return the new StatePath, pointing to the parent, or null if the node has no parent.
     */
    public StatePath parentPath() {
        if (_elements.size() <= 1) {
            return null;
        }

        return new StatePath(_elements.subList(0, _elements.size()-1), _elements.size()-1);
    }

    /**
     * Check whether this path contains any branches; i.e., if the number of elements
     * in the path is strictly greater than one.
     * @return true if the path contains no branches, false otherwise.
     */
    public boolean isSimplePath() {
        return _elements.size() == 1;
    }
}
