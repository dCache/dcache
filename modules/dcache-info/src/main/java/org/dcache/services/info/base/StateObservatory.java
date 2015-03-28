package org.dcache.services.info.base;

import java.util.List;

/**
 * Classes that implement StateObservatory provides a facility where
 * StateWatchers can register to observe the current dCache state. A
 * StateObservatory allows various operations on this collection of
 * registered observers: the inclusion of a StateWatcher into the group, the
 * removal of a StateWatcher from the group, listing the current members of
 * the group and disabling and enabling these members.
 * <p>
 * A StateObservatory allows a StateCaretaker to inform StateWatchers of any
 * StateTransitions that satisfy the StateWatcher's interest. As a special
 * case, those StateWatchers that are secondary information providers (those
 * that maintain derived metrics) may wish to update metric values as a
 * result of a StateTransition.
 */
public interface StateObservatory
{
    /**
     * Set the StateWatcher objects that are aware of changes to dCache state.
     */
    public void setStateWatchers(List<StateWatcher> watchers);

    /**
     * Provide an array of Strings that describe the current group
     * membership.
     *
     * @return
     */
    public String[] listStateWatcher();

    /**
     * Enable all StateWatchers that match the given name
     *
     * @param name
     *            name of StateWatcher(s) that are to be enabled.
     * @return number of StateWatchers that matched name.
     */
    public int enableStateWatcher(String name);

    /**
     * Disable all StateWatchers that match the given name.
     *
     * @param name
     *            name of StateWatcher(s) that are to be disabled.
     * @return number of StateWatchers that matched name.
     */
    public int disableStateWatcher(String name);

    /**
     * Scan through the group of StateWatchers and trigger those that have
     * expressed an interest in a metric that is to change. A StateUpdate is
     * generated for all derived metric values that have changed as a result
     * of this transition.
     *
     * @param transition
     *            the StateTransition describing the pending changes.
     * @return a StateUpdate with all changes to derived metrics, or null if
     *         there are none.
     */
    public StateUpdate checkWatchers(StateTransition transition);
}
