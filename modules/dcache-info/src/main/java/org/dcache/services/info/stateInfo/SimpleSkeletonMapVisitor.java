package org.dcache.services.info.stateInfo;

import org.dcache.services.info.base.StatePath;

/**
 * The SimpleSkeletonMapVisitor provides a simple framework for building a map between
 * a list of items and a particular metric within that list.  Although not abstract, this
 * Class should not be instantiated as it won't do anything useful!  Instead, a Class
 * should extend this class for each possible type of StateValue (the "metric").  This
 * is to allow the metric type to be reflected in the Map declaration.
 * <p>
 * To achieve this, the super-Class should override one of the visit*() methods, use
 * getPathToMetric() to test whether the metric is to be selected and getKey() as the key
 * in the corresponding Map.
 * <p>
 * The super-Class should also implement the getMap() method that should return a
 * Map of the correct type for the StateValue super-Class (metric type) under
 * consideration.
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SimpleSkeletonMapVisitor extends SkeletonListVisitor
{
    private final StatePath _relativePathToMetric;

    private StatePath _pathToMetric;

    /**
     * Build a Map between a list of items and a metric value within that list.  To do this, two
     * paths must be specified.  The first path locates the parent StateComposite (branch) that
     * the list items share as their common parent: the keys in the resulting Map will be the names
     * of the StateComposites that are immediate children of the StateComposite given by the first path.
     * <p>
     * The second path gives the relative path from a child of the parent StateComposite to the metric under
     * consideration.  If this is missing, no mapping is created.
     * <p>
     * For example, if the tree has String metrics like:
     * <pre>
     *   aa.bb.item1.cc.dd.value1
     *   aa.bb.item2.cc.dd.value2
     *   aa.bb.item3.cc.dd.value3
     * </pre>
     * Then supplying the paths aa.bb and cc.dd will result in a mapping with the following
     * Map.Entries:
     * <pre>  item1 --> value1
     *  item2 --> value2
     *  item3 --> value3
     * </pre>
     * Classes that extend this Class should implement a <code>getMap()</code> method.  This should
     * return a mapping between the key and the value and have the correct type.
     */
    protected SimpleSkeletonMapVisitor(StatePath pathToList, StatePath pathToMetric)
    {
        super(pathToList);
        _relativePathToMetric = pathToMetric;
    }

    @Override
    protected void newListItem(String listItemName)
    {
        super.newListItem(listItemName);

        _pathToMetric = getPathToList().newChild(listItemName).newChild(_relativePathToMetric);
    }

    protected StatePath getPathToMetric()
    {
        return _pathToMetric;
    }
}
