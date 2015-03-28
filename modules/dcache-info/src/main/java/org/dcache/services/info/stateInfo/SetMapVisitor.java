package org.dcache.services.info.stateInfo;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.dcache.services.info.base.StateExhibitor;
import org.dcache.services.info.base.StatePath;

/**
 * The SetMapVisitor builds a Map between a list of items and some list
 * within that item (at a fixed relative path).  For example, if the
 * dCache state has entries like:
 * <pre>
 * aa.bb.item1.cc.dd.i1E1
 * aa.bb.item1.cc.dd.i1E2
 * aa.bb.item1.cc.dd.i1E3
 * aa.bb.item2.cc.dd.i2E1
 * aa.bb.item2.cc.dd.i2E2
 * aa.bb.item2.cc.dd.i2E3
 * aa.bb.item3.cc.dd.i3E1
 * </pre>
 * then the output would be the mapping:
 * <pre>
 * item1 -> {i1E1, i1E2, i1E3}
 * item2 -> {i2E1, i2E2, i2E3}
 * item3 -> {i3E1}
 * </pre>
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class SetMapVisitor extends SkeletonListVisitor {

    /**
     * Obtain a Map between list items and their corresponding Set of the children of some
     * fixed relative path for dCache's current state.  For example, if dCache currently
     * contains:
     * <pre>
     * aa.bb.item1.cc.dd.i1E1
     * aa.bb.item1.cc.dd.i1E2
     * aa.bb.item1.cc.dd.i1E3
     * aa.bb.item2.cc.dd.i2E1
     * aa.bb.item2.cc.dd.i2E2
     * aa.bb.item2.cc.dd.i2E3
     * aa.bb.item3.cc.dd.i3E1
     * </pre>
     * then, with pathToMainList of aa.bb and pathToSecondList of cc.dd, the output
     * would be the mapping:
     * <pre>
     * "item1" -> {"i1E1", "i1E2", "i1E3"}
     * "item2" -> {"i2E1", "i2E2", "i2E3"}
     * "item3" -> {"i3E1"}
     * </pre>
     *
     * @param pathToMainList the StatePath for the common parent for the primary list
     * @param pathToSecondList the StatePath, relative to the list item for parent of the item list.
     * @return a mapping between an item and the set of items at a fixed relative path.
     */
    static public Map<String,Set<String>> getDetails(StateExhibitor exhibitor,
            StatePath pathToMainList,
            StatePath pathToSecondList) {
        SetMapVisitor visitor = new SetMapVisitor(pathToMainList, pathToSecondList);
        exhibitor.visitState(visitor);
        return visitor.getMap();
    }

    /** Record the relative path to the parent object of the secondard list items */
    final private StatePath _relativePathToList;

    /** The mapping to return */
    final private Map<String,Set<String>> _map = new HashMap<>();

    /** The (absolute) StatePath to the current list-item's parent */
    private StatePath _pathToSet;

    /** The set of secondary list-items for the current (primary) list item */
    private Set<String> _thisListItemSet;

    /**
     * Create a new visitor that extracts a mapping from dCache state.
     * @param pathToPrimaryList path of the common parent of the primary list
     * @param relativePathToSecondList path, relative to the primary list item, of
     * the common parent of the secondary list.
     */
    public SetMapVisitor(StatePath pathToPrimaryList, StatePath relativePathToSecondList) {
        super(pathToPrimaryList);
        _relativePathToList = relativePathToSecondList;
    }

    @Override
    protected void newListItem(String listItemName) {
        super.newListItem(listItemName);

        _pathToSet = getPathToList().newChild(listItemName).newChild(_relativePathToList);

        _thisListItemSet = new HashSet<>();
        _map.put(listItemName, _thisListItemSet);
    }


    @Override
    protected void exitingListItem(String listItemName) {
        super.exitingListItem(listItemName);

        _pathToSet = null;
        _thisListItemSet = null;
    }


    @Override
    public void visitCompositePreDescend(StatePath path, Map<String, String> metadata) {
        super.visitCompositePreDescend(path, metadata);

        if (isInListItem()) {
            if (_pathToSet.isParentOf(path)) {
                _thisListItemSet.add(path.getLastElement());
            }
        }
    }

    /**
     * Obtain the results of running the visitor over dCache's state.
     * @return a map between the primary and secondary lists.
     */
    public Map<String,Set<String>> getMap() {
        return _map;
    }
}
