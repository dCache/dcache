package org.dcache.services.info.base;

import java.util.Map;

/**
 * The MalleableStateTransition extends StateTransition by providing some
 * additional methods for establishing StateTransition without first
 * establishing a given state and walking that state with a StateUpdate.
 * <p>
 * It is intended to be used for unit testing.
 */
public class MalleableStateTransition extends StateTransition {

    /**
     * Update a MalleableStateTransition so that a single metric is added.
     * This routine may be called multiple times to configure the
     * StateTransition so multiple new metrics may be added.
     * <p>
     * The method takes a parameter {@code existing}. This is the number of
     * path elements that already exist in the state when processing this new
     * metric. For example, when adding two metrics with some common parent
     * to an empty state the first call has {@code existing} of zero and the
     * second has the length of the common parent path. The following code
     * illustrates this.
     *
     * <pre>
     * StatePath commonPath = StatePath.parsePath( &quot;aa.bb&quot;);
     * StatePath path1 = commonPath.newChildPath( &quot;metric1&quot;);
     * StatePath path2 = commonPath.newChildPath( &quot;metric2&quot;);
     * // ...etc..
     * transition.updateTransitionForNewMetric( path1, metric1, 0);
     * transition.updateTransitionForNewMetric( path2, metric2,
     *                                          commonPath._elements.size());
     * </pre>
     *
     * This method may be used for updating a metric by specifying an {@code
     * existing} value equal to the metric's StatePath length.
     *
     * @param path
     *            the StatePath of the metric
     * @param metricValue
     *            the StateValue of the new metric
     * @param existing
     *            number of path elements that already exist in the state
     */
    public void updateTransitionForNewMetric( StatePath path,
                                              StateValue metricValue,
                                              int existing) {
        StatePath parentPath = path.parentPath();

        updateTransitionForNewBranch( parentPath, existing);

        /**
         * Since updateTransitionForNewBranch won't iterate down beyond the
         * last element, we must add the final iteration ourselves.
         */
        if( parentPath != null && !parentPath.isSimplePath()) {
            getStateChangeSet(parentPath.parentPath()).recordChildItr(
                    parentPath.getLastElement());
        }

        String metricName = path.getLastElement();
        StateChangeSet scs = getOrCreateChangeSet( parentPath);

        if( existing < path._elements.size()) {
            scs.recordNewChild(metricName, metricValue);
        } else {
            scs.recordUpdatedChild(metricName, metricValue);
        }
    }

    /**
     * Update the StateTransition so a new branch is created.
     *
     * @param path
     *            the StatePath of the branch
     * @param existing
     *            number of path elements already existing.
     * @see #updateTransitionForNewMetric(StatePath, StateValue, int)
     */
    public void updateTransitionForNewBranch( StatePath path, int existing) {
        StatePath currentPath = null;
        int remainingExistingPathElements = existing;

        for( StatePath remainingPath = path; remainingPath != null; remainingPath = remainingPath.childPath()) {
            String childName = remainingPath.getFirstElement();

            StateChangeSet scs = getOrCreateChangeSet( currentPath);

            if( remainingExistingPathElements == 0 &&
                !scs.childIsNew( childName)) {
                scs.recordNewChild(childName, new StateComposite());
            } else {
                remainingExistingPathElements--;
            }

            if( !remainingPath.isSimplePath()) {
                scs.recordChildItr(childName);
            }

            currentPath = currentPath == null ? new StatePath( childName)
                    : currentPath.newChild( childName);
        }
    }

    public void updateTransitionChangingMetricToBranch( StatePath path, int existing) {
        updateTransitionForNewBranch( path, existing);

        StatePath parentPath = path.parentPath();
        StateChangeSet scs = getOrCreateChangeSet( parentPath);

        String metricName = path.getLastElement();
        scs.recordUpdatedChild( metricName, new StateComposite());
    }

    /**
     * Update a StateTransition so the metric at StatePath is to be removed.
     *
     * @param path
     *            the StatePath for the metric to be removed.
     */
    public void updateTransitionForRemovingElement( StatePath path) {
        StatePath currentPath = null;

        for( StatePath remainingPath = path; remainingPath != null; remainingPath = remainingPath.childPath()) {
            String childName = remainingPath.getFirstElement();

            StateChangeSet scs = getOrCreateChangeSet( currentPath);
            if( remainingPath.isSimplePath()) {
                scs.recordRemovedChild(childName);
            } else {
                scs.recordChildItr(childName);
            }

            currentPath = currentPath == null ? new StatePath( childName)
                    : currentPath.newChild( childName);
        }
    }

    /**
     * Provide a cryptic but human-readable String describing the
     * MalleableStateTransition. This is intended only for debugging
     * purposes.
     *
     * @return a description of this StateTransition.
     */
    public String debugInfo() {
        StringBuilder sb = new StringBuilder();

        sb.append( "  == MalleableStateTransition ==\n");

        for( Map.Entry<StatePath, StateChangeSet> entry : _allChanges.entrySet()) {
            StatePath path = entry.getKey();
            sb.append(" [").append((path != null) ? path.toString() : "(root)")
                    .append("]\n");

            StateChangeSet scs = entry.getValue();

            sb.append( scs.dumpContents());
            sb.append( "\n");
        }

        return sb.toString();
    }
}
