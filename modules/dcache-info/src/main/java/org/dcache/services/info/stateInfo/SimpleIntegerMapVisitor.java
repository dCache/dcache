package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.Map;

import org.dcache.services.info.base.IntegerStateValue;
import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;

/**
 * Build a Map<String,Long> that maps between a particular entry in a list of items
 * and some IntegerStateValue a fixed relative path from each list item.  For example,
 * if the state tree contains entries like:
 * <pre>
 *   aa.bb.item1.cc.dd.integerMetric = IntegerStateValue(20)
 *   aa.bb.item2.cc.dd.integerMetric = IntegerStateValue(30)
 *   aa.bb.item3.cc.dd.integerMetric = IntegerStateValue(50)
 * </pre>
 * then using this class with pathToList of StatePath.parsePath("aa.bb") and
 * StatePath.parsePath("cc.dd.integerMetric") will yield a Map like:
 * <pre>
 *   "item1" --> Long(20)
 *   "item2" --> Long(30)
 *   "item3" --> Long(50)
 * </pre>
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SimpleIntegerMapVisitor extends SimpleSkeletonMapVisitor {

    /**
     * Build a mapping between list items and some StringStateValue value for dCache's current state.
     * @param pathToList the StatePath of the list's parent StateComposite.
     * @param pathToMetric the StatePath, relative to the list item, of the StringStateValue
     * @return the mapping between list items and the metric values.
     */
    public static final Map<String,Long> buildMap(StateExhibitor exhibitor, StatePath pathToList, StatePath pathToMetric) {
        SimpleIntegerMapVisitor visitor = new SimpleIntegerMapVisitor(pathToList, pathToMetric);
        exhibitor.visitState(visitor);
        return visitor.getMap();
    }

    private final Map <String,Long> _map;

    public SimpleIntegerMapVisitor(StatePath pathToList, StatePath pathToMetric) {
        super(pathToList, pathToMetric);

        _map = new HashMap<>();
    }

    @Override
    public void visitInteger(StatePath path, IntegerStateValue value) {
        if (path.equals(getPathToMetric())) {
            _map.put(getKey(), value.getValue());
        }
    }

    Map<String,Long> getMap() {
        return _map;
    }
}
