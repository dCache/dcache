package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.Map;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;
import org.dcache.services.info.base.StringStateValue;

/**
 * Build a Map<String,String> that maps between a particular entry in a list of items
 * and some StringStateValue a fixed relative path from each list item.  For example,
 * if the state tree contains entries like:
 * <pre>
 *   aa.bb.item1.cc.dd.stringMetric = StringStateValue("foo1")
 *   aa.bb.item2.cc.dd.stringMetric = StringStateValue("foo2")
 *   aa.bb.item3.cc.dd.stringMetric = StringStateValue("foo3")
 * </pre>
 * then using this class with pathToList of StatePath.parsePath("aa.bb" and
 * StatePath.parsePath("cc.dd.stringMetric") will yield a Map like:
 * <pre>
 *   "item1" --> "foo1"
 *   "item2" --> "foo2"
 *   "item3" --> "foo3"
 * </pre>
 */
public class SimpleStringMapVisitor extends SimpleSkeletonMapVisitor
{
    /**
     * Build a mapping between list items and some StringStateValue value for dCache's current state.
     * @param pathToList the StatePath of the list's parent StateComposite.
     * @param pathToMetric the StatePath, relative to the list item, of the StringStateValue
     * @return the mapping between list items and the metric values.
     */
    public static final Map<String,String> buildMap(StateExhibitor exhibitor,
            StatePath pathToList, StatePath pathToMetric)
    {
        SimpleStringMapVisitor visitor = new SimpleStringMapVisitor(pathToList, pathToMetric);
        exhibitor.visitState(visitor);
        return visitor.getMap();
    }

    private final Map <String,String> _map;

    public SimpleStringMapVisitor(StatePath pathToList, StatePath pathToMetric)
    {
        super(pathToList, pathToMetric);
        _map = new HashMap<>();
    }

    @Override
    public void visitString(StatePath path, StringStateValue value)
    {
        if (path.equals(getPathToMetric())) {
            _map.put(getKey(), value.toString());
        }
    }

    Map<String,String> getMap()
    {
        return _map;
    }
}
