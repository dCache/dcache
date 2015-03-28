/**
 * Copyright 2007 the dCache team
 */
package org.dcache.services.info.base;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The StateUpdate is a simple collection containing zero or more proposed concurrent
 * changes to dCache's State.  Changes are zero or more purges and zero or more
 * updated metric values.  The purges are processed first followed by any state updates.
 * <p>
 * A purge clears a branch and all StateComponents below that branch.  Subsequent
 * state updates within the subtree will fill the subtree as expected.
 * <p>
 * Each state update change consists of a StatePath and a StateComponent.  The StatePath
 * indicates where the update is to take place and the StateComponent is the new value
 * to be stored.
 * <p>
 * StateUpdate objects are immutable, providing read-only access to proposed set of changes.
 *
 * @see StateTransition
 */
public class StateUpdate
{
    private static final Logger _log = LoggerFactory.getLogger(StateUpdate.class);

    /**
     * A single update to a dCache metric.
     */
    private static class StateUpdateInstance
    {
        final StatePath _path;
        final StateComponent _newValue;

        StateUpdateInstance(StatePath path, StateComponent newValue)
        {
            _path = path;
            _newValue = newValue;
        }
    }

    /** StateComponents to create or update */
    private final List<StateUpdateInstance> _updates = new ArrayList<>();

    /** A list of subtrees to purge */
    private final List<StatePath> _purge = new ArrayList<>();


    /**
     * Discover whether a StateUpdate object intends to update the specified location in
     * the state tree with a metric that is equal to the given StateComponent.
     * <p>
     * This method is intended for unit-testing only: normal operation should not need to
     * call this method.
     * @param path the StatePath of the item in question.
     * @param value the StateComponent that is being queried.
     */
    public boolean hasUpdate(StatePath path, StateComponent value)
    {
        if (path == null || value == null) {
            return false;
        }

        /**
         * Yes, this is an ugly O(n) linear search, but should only be for
         * a small number of entries.
         */

        for (StateUpdateInstance sui : _updates) {
            if (sui._path.equals(path) && sui._newValue.equals(value)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Discover whether a StateUpdate object intends to purge the specified location
     * in the state tree and all child elements.
     * <p>
     * This method is intended for unit-testing only: normal operation should not need to
     * call this method.
     * @param path the StatePath of the top-most element in the purge subtree
     * @return true if the StateUpdate intends to purge exactly the stated subtree, false otherwise.
     */
    public boolean hasPurge(StatePath path)
    {
        return _purge.contains(path);
    }

    /**
     * Count the number of metrics that are to be updated.
     * @return the number of metric updates contained within this StateUpdate.
     */
    public int count()
    {
        return _updates.size();
    }

    /**
     * Count the number of StatePaths that are the root of some
     * purge operation.
     * @return
     */
    public int countPurges()
    {
        return _purge.size();
    }

    /**
     * Provide a mechanism whereby we can append additional updates to
     * this state.
     * @param path: the StatePath of the new StateValue.
     * @param value: the new
     */
    public void appendUpdate(StatePath path, StateComponent value)
    {
        _updates.add(new StateUpdateInstance(path, value));
    }

    /**
     * Add a Collection of items as a series of StateComposites.  These
     * may be immortal or ephemeral.
     * @param path  The common StatePath for this Collection
     * @param items The items to add
     * @param isImmortal true for immortal StateComposites; false for ephemeral
     */
    public void appendUpdateCollection(StatePath path, Collection<String> items,
            boolean isImmortal)
    {
        for (String item : items) {
            appendUpdate(path.newChild(item), new StateComposite(isImmortal));
        }
    }

    /**
     * Add a Collection of items as mortal StateComposite objects.
     * @param path The common StatePath for this Collection
     * @param items The Collection of names.
     * @param lifetime the lifetime, in seconds, for the StateComposites.
     */
    public void appendUpdateCollection(StatePath path, List<String> items,
            long lifetime)
    {
        for (String item : items) {
            appendUpdate(path.newChild(item), new StateComposite(lifetime));
        }
    }


    /**
     * Add the name of a path that all elements underneath should be purged from
     * the information.  These purges, if any, happen before any addition or
     * updating elements.
     * @param path
     */
    public void purgeUnder(StatePath path)
    {
        _purge.add(path);
    }


    /**
     * Go through each of the proposed updates recorded and update the StateTransition object.
     * @param top the top-most StateComposite within dCache state
     * @param transition the StateTransition to update.
     * @throws BadStatePathException
     */
    protected void updateTransition(StateComposite top, StateTransition transition)
            throws BadStatePathException
    {
        BadStatePathException caughtThis = null;

        _log.debug("preparing transition with " + _purge.size() + " purge and " + _updates.size() + " update");

        for (StatePath path : _purge) {
            top.buildPurgeTransition(transition, null, path);
        }

        for (StateUpdateInstance update : _updates) {
            if (_log.isDebugEnabled()) {
                _log.debug("preparing transition to alter " + update
                        ._path.toString());
            }

            try {
                top.buildTransition(null, update._path, update._newValue, transition);
            } catch (BadStatePathException e) {
                if (caughtThis == null) {
                    caughtThis = e;
                }
            }
        }

        if (caughtThis != null) {
            throw caughtThis;
        }
    }

    /**
     * A handy routine for extracting information.
     * @return a human-readable string describing the StateUpdate object
     */
    public String debugInfo()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("=== StateUpdate ===\n");

        sb.append("  Number of purges: ").append(_purge.size())
                .append("\n");
        for (StatePath purgePath : _purge) {
            sb.append("    ").append(purgePath).append("\n");
        }

        sb.append("  Number of StateUpdates: ").append(_updates.size())
                .append("\n");
        for (StateUpdateInstance sui : _updates) {
            sb.append("    ").append(sui._path).append(" ")
                    .append(sui._newValue).append("\n");
        }

        return sb.toString();
    }
}
