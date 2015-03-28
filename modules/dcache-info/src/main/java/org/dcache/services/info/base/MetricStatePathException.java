package org.dcache.services.info.base;


/**
 * An exception thrown when a StatePath refers to an element that should be
 * a StateComponsite (i.e., a branch), but is a StateValue (a leaf).
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class MetricStatePathException extends BadStatePathException {

    static final String DEFAULT_PREFIX = "path element is a metric instead of a branch: ";

    private static final long serialVersionUID = 1;


    public MetricStatePathException(String path) {
        super(DEFAULT_PREFIX + path);
    }


    /**
     * Create a new MetricStatePathException when the child (childName) is a metric-node
     * instead of a branch-node.
     * @param pathToComposite the StatePath to this StateComposite
     * @param childName the name of the child that should have been a StateComposite.
     */
    public MetricStatePathException(StatePath pathToComposite, String childName) {
        super(DEFAULT_PREFIX + pathToComposite.newChild(childName).toString());
    }
}
