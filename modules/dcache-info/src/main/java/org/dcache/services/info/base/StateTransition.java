package org.dcache.services.info.base;

import java.util.HashMap;
import java.util.Map;



/**
 * A StateTransition contains information about a pending alteration to the current
 * dCache state tree.  Unlike a StateUpdate, the StateTransition includes this information
 * in terms of Change-sets, each contains the changes for a specific StateComposites
 * (i.e., branches) instead of a collection of StatePaths.  Each change-set contains information
 * on new children, children that are to be updated, and those that are to be removed.
 * <p>
 * Providing information like this, the Visitor pattern can be applied, iterating over
 * proposed changes, with the StateTransition object working in collaboration with the
 * existing dCache state tree.
 * @see StateUpdate
 * @author Paul Millar <paul.millar@desy.de>
 */
public class StateTransition {

    final Map<StatePath, StateChangeSet> _allChanges = new HashMap<>();

    /**
     * Obtain the StateChangeSet for this path.  If one doesn't exist, null
     * is returned.
     * @param path
     * @return
     */
    protected StateChangeSet getStateChangeSet(StatePath path) {
        return _allChanges.get(path);
    }


    /**
     * Obtain the StateChangeSet for this path.  If one doesn't exist, an
     * empty one is created and returned.
     * @param path  The StatePath for the composite.
     * @return this StatePath's change-set
     */
    protected StateChangeSet getOrCreateChangeSet(StatePath path) {
        StateChangeSet changeSet;

        changeSet = _allChanges.get(path);

        if (changeSet == null) {
            changeSet = new StateChangeSet();
            _allChanges.put(path, changeSet);
        }

        return changeSet;
    }



    /**
     * Dump our contents to a (quite verbose) String.
     * @return
     */
    protected String dumpContents() {
        StringBuilder sb = new StringBuilder();
        boolean isFirst = true;

        for (Map.Entry<StatePath,StateChangeSet> entry : _allChanges.entrySet()) {
            StateChangeSet changeSet = entry.getValue();

            if (isFirst) {
                isFirst = false;
            } else {
                sb.append("\n");
            }

            sb.append("Path: ");
            sb.append(entry.getKey() != null ? entry.getKey() : "(null)");
            sb.append("\n");

            sb.append(changeSet.dumpContents());
        }

        return sb.toString();
    }
}
