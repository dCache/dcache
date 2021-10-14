package org.dcache.services.info.base;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This singleton class provides a (best-effort) complete representation of dCache instance's
 * current state.
 * <p>
 * It receives fresh information through the updateState() method, which accepts a StateUpdate
 * object. StateUpdate objects are created by classes in the org.dcache.services.info.gathers
 * package, either synchronously (see DataGatheringScheduler), or by dCache messages received
 * asynchronously (see MessageHandlerChain).
 * <p>
 * There is preliminary support for triggering activity on state changes through StateWatcher
 * interface. Some class may use this to publish aggregated (or otherwise, derived) data in a timely
 * fashion (see org.dcache.services.info.secondaryInfoProvider package). Other classes may use
 * StateWatcher interface to trigger external events, although future work may include adding
 * support for asynchronous event-based monitoring that would interface with, but remain separate
 * from this state.
 * <p>
 * The State object allows a visitor pattern, through the StateVisitor interface. The principle
 * usage of this is to serialise the current content (see classes under
 * org.dcache.services.info.serialisation package), but some synchronous classes also use this to
 * build lists from dCache current state (e.g., to send a message requesting data to each currently
 * known pool).
 *
 * @author Paul Millar <paul.millar@desy.de>
 */
public class State implements StateCaretaker, StateExhibitor, StateObservatory {

    /**
     * Constants used for persistent metadata
     */
    public static final String METADATA_BRANCH_CLASS_KEY = "branch";
    public static final String METADATA_BRANCH_IDNAME_KEY = "id";

    private static final Logger LOGGER = LoggerFactory.getLogger(State.class);

    /**
     * Class member variables...
     */

    /**
     * The root branch of the dCache state
     */
    private final StateComposite _state;

    /**
     * All registered StateWatchers
     */
    private volatile Collection<StateWatcherInfo> _watchers = new ArrayList<>();

    /**
     * Our read/write lock: we use the fair version to reduce the risk of writers constantly
     * blocking a reader
     */
    private final ReadWriteLock _stateRWLock = new ReentrantReadWriteLock(true);
    private final Lock _stateReadLock = _stateRWLock.readLock();
    private final Lock _stateWriteLock = _stateRWLock.writeLock();

    // TODO: remove this completely. It's only needed to support derived
    // metrics
    private StateUpdateManager _updateManager;

    public State() {
        // Build our persistent State metadata tree, with default contents
        StatePersistentMetadata metadata = new StatePersistentMetadata();
        metadata.addDefault();

        // Build our top-level immortal StateComposite.
        _state = new StateComposite(metadata);
    }

    /**
     * Record a new StateUpdateManager. This will be used to enqueue StateUpdates from secondary
     * information providers.
     *
     * @param sum the StateUpdateManager
     */
    public void setStateUpdateManager(StateUpdateManager sum) {
        _updateManager = sum;
    }

    /**
     * Discover when next to purge metrics from the dCache state.
     *
     * @return the Date when a metric or branch will next need to be expired.
     */
    @Override
    public Date getEarliestMetricExpiryDate() {
        Date earliestExpiryDate = _state.getEarliestChildExpiryDate();

        if (LOGGER.isTraceEnabled()) {
            if (earliestExpiryDate == null) {
                LOGGER.trace("reporting that earliest expiry time is never");
            } else {
                LOGGER.trace("reporting that earliest expiry time is {}s in the future",
                      (earliestExpiryDate.getTime() - System.currentTimeMillis()) / 1000);
            }
        }

        return earliestExpiryDate;
    }

    /**
     * Update the current dCache state by applying, at most, a single StateUpdate from a Stack of
     * pending updates. If no updates are needed, the routine will return quickly, although there is
     * the cost of entering the Stack's monitor.
     * <p>
     * Updating dCache state, by adding additional metrics, has three phases:
     * <ol>
     * <li>Compile a StateTransition, describing the changes within the
     * hierarchy.
     * <li>Check StateWatchers' StatePathPredicates and triggering those
     * affected.
     * <li>Traverse the tree, applying the StateTransition
     * </ol>
     */
    @Override
    public void processUpdate(StateUpdate update) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("beginning to process update: \n{}", update.debugInfo());
        }

        // It's just possible that we might be called with null; ignore these
        // spurious calls.
        if (update == null) {
            LOGGER.warn("processUpdate called with null StateUpdate");
            return;
        }

        if (update.countPurges() == 0 && update.count() == 0) {
            LOGGER.warn("StateUpdate with zero updates encountered");
            return;
        }

        StateTransition transition = new StateTransition();

        try {
            _stateReadLock.lock();

            /**
             * Update our new StateTransition based on the StateUpdate.
             */
            try {
                update.updateTransition(_state, transition);
            } catch (BadStatePathException e) {
                LOGGER.error("Error updating state:", e);
            }

            LOGGER.trace("checking StateWatchers");

            StateUpdate resultingUpdate = checkWatchers(transition);

            // TODO: don't enqueue the update but merge with existing StateTransition and
            // look for additional StateWatchers.
            if (resultingUpdate != null) {
                _updateManager.enqueueUpdate(resultingUpdate);
            }

        } finally {
            _stateReadLock.unlock();
        }

        applyTransition(transition);
    }

    /**
     * Apply a StateTransition to dCache state. This is the final step in updating the dCache state
     * where the proposed changes are made permanent. This requires obtaining a writer-lock on the
     * state.
     *
     * @param transition the StateTransition to apply.
     */
    private void applyTransition(StateTransition transition) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("now applying following transition to state:\n\n{}",
                  transition.dumpContents());
        }

        try {
            _stateWriteLock.lock();

            _state.applyTransition(null, transition);

        } finally {
            _stateWriteLock.unlock();
        }
    }

    /**
     * For a given a StateTransition, check all registered StateWatchers to see if they are
     * affected. This is achieved by checking each StateWatcher's Collection of
     * StatePathPredicates.
     * <p>
     * If the StatePathPredicate matches some significant change within the StateTransition, the
     * corresponding StateWatcher's
     * <code>trigger()</code> method is called. The trigger method is
     * provided with a StateUpdate within which it may register additional metrics.
     * <p>
     *
     * @param transition The StateTransition to apply
     * @return a StateUpdate containing new metrics, or null if there are no new metrics to update.
     */
    @Override
    public StateUpdate checkWatchers(StateTransition transition) {
        StateUpdate update = new StateUpdate();
        StateExhibitor currentState = this;
        StateExhibitor futureState = null;

        for (StateWatcherInfo thisWatcherInfo : _watchers) {
            StateWatcher thisWatcher = thisWatcherInfo.getWatcher();

            if (!thisWatcherInfo.isEnabled()) {
                LOGGER.trace("skipping disabled watcher {}", thisWatcher);
                continue;
            }

            LOGGER.trace("checking watcher {}", thisWatcher);

            boolean hasBeenTriggered = false;

            for (StatePathPredicate thisPredicate : thisWatcher.getPredicate()) {
                LOGGER.trace("checking watcher {} predicate {}", thisWatcher, thisPredicate);

                hasBeenTriggered = _state.predicateHasBeenTriggered(null,
                      thisPredicate,
                      transition);

                if (hasBeenTriggered) {
                    break; // we only need one predicate to match, so quit
                }
                // early.
            }

            if (hasBeenTriggered) {
                LOGGER.info("triggering watcher {}", thisWatcher);
                thisWatcherInfo.incrementCounter();
                if (futureState == null) {
                    futureState = new PostTransitionStateExhibitor(currentState, transition);
                }

                thisWatcher.trigger(update, currentState, futureState);
            }
        }

        return update.count() > 0 || update.countPurges() > 0 ? update : null;
    }


    @Override
    public void setStateWatchers(List<StateWatcher> watchers) {
        List<StateWatcherInfo> newList = new ArrayList<>(watchers.size());

        for (StateWatcher watcher : watchers) {
            newList.add(new StateWatcherInfo(watcher));
        }

        _watchers = newList;
    }


    /**
     * Return a String containing a list of all state watchers.
     *
     * @param prefix a String to prefix each line of the output
     * @return the list of watchers.
     */
    @Override
    public String[] listStateWatcher() {
        String[] watchers = new String[_watchers.size()];

        int i = 0;
        for (StateWatcherInfo thisWatcherInfo : _watchers) {
            watchers[i++] = thisWatcherInfo.toString();
        }

        return watchers;
    }

    /**
     * Enable all registered StateWatchers that match a given name
     *
     * @param name the StateWatcher name to enable.
     * @return the number of matching entries.
     */
    @Override
    public int enableStateWatcher(String name) {
        int count = 0;

        for (StateWatcherInfo thisWatcherInfo : _watchers) {
            if (name.equals(thisWatcherInfo.getWatcher().toString())) {
                thisWatcherInfo.enable();
                count++;
            }
        }

        return count;
    }

    /**
     * Disable all registered StateWatchers that match a given name
     *
     * @param name the StateWatcher name to disable.
     * @return the number of matching entries.
     */
    @Override
    public int disableStateWatcher(String name) {
        int count = 0;

        for (StateWatcherInfo thisWatcherInfo : _watchers) {
            if (name.equals(thisWatcherInfo.getWatcher().toString())) {
                thisWatcherInfo.disable();
                count++;
            }
        }

        return count;
    }

    /**
     * Check for, and remove, expired (mortal) StateComponents. This will also remove all ephemeral
     * children of moral StateComposites (branches).
     * <p>
     * The process of removing data from dCache tree is similar to
     * <code>processUpdateStack()</code> and may trigger StateWatchers. This
     * method has three phases:
     * <ol>
     * <li>Build a StateTransition, describing the affected StateComponents,
     * <li>Check for, and trigger, affected StateWatchers,
     * <li>Apply the changes described in the StateTransition
     * <ol>
     */
    @Override
    public synchronized void removeExpiredMetrics() {
        // A quick check before obtaining the lock
        Date expDate = getEarliestMetricExpiryDate();

        if (expDate == null || expDate.after(new Date())) {
            return;
        }

        LOGGER.trace("Building StateTransition for expired StateComponents");
        StateTransition transition = new StateTransition();

        try {
            _stateReadLock.lock();

            _state.buildRemovalTransition(null, transition, false);

            StateUpdate resultingUpdate = checkWatchers(transition);

            // TODO: don't enqueue the update but merge with existing StateTransition and
            // look for additional StateWatchers.
            if (resultingUpdate != null) {
                _updateManager.enqueueUpdate(resultingUpdate);
            }

        } finally {
            _stateReadLock.unlock();
        }

        applyTransition(transition);
    }

    /**
     * Allow an arbitrary algorithm to visit the current dCache state, that is, to receive
     * call-backs describing the process of walking over the state and the contents therein.
     * <p>
     * The data obtained from a single call of <code>visitState()</code> is protected from
     * inconsistencies due to data being updated whilst the iteration is taking place. No such
     * protection is available for multiple calls to <code>visitState()</code>.
     *
     * @param visitor the algorithm that wishes to visit our current state
     */
    @Override
    public void visitState(StateVisitor visitor) {
        LOGGER.trace("visitor {} wishing to visit current state", visitor);

        try {
            long beforeLock = System.currentTimeMillis();

            LOGGER.trace("visitor {} acquiring read lock", visitor);
            _stateReadLock.lock();

            long afterLock = System.currentTimeMillis();

            LOGGER.trace("visitor {} acquired read lock (took {} ms), starting visit.",
                  visitor, (afterLock - beforeLock) / 1000.0);

            if (visitor.isVisitable(null)) {
                _state.acceptVisitor(null, visitor);
            }

            long afterVisit = System.currentTimeMillis();

            LOGGER.trace("visitor {} completed visit (took {} ms), releasing read lock.",
                  visitor, (afterVisit - afterLock) / 1000.0);
        } finally {
            _stateReadLock.unlock();
        }

        LOGGER.trace("visitor {} finished.", visitor);
    }

    /**
     * Small, simple class to hold information about our registered StateWatchers, whether they are
     * enabled and how many times they've been triggered.
     */
    private static class StateWatcherInfo {

        StateWatcher _watcher;
        boolean _isEnabled = true;
        long _counter;

        StateWatcherInfo(StateWatcher watcher) {
            _watcher = watcher;
        }

        boolean isEnabled() {
            return _isEnabled;
        }

        void enable() {
            _isEnabled = true;
        }

        void disable() {
            _isEnabled = false;
        }

        void incrementCounter() {
            _counter++;
        }

        StateWatcher getWatcher() {
            return _watcher;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();

            sb.append(_watcher.toString()).append(" ");
            sb.append("(");
            sb.append(_isEnabled ? "enabled" : "disabled");
            sb.append(", triggered: ").append(_counter);
            sb.append(")");

            return sb.toString();
        }
    }

    /**
     * Emit output suitable for the info cell command.
     *
     * @param pw
     */
    public void getInfo(PrintWriter pw) {
        pw.print(listStateWatcher().length);
        pw.println(" state watchers.");

        pw.print(_updateManager.countPendingUpdates());
        pw.println(" pending updates to state.");
    }
}
