package org.dcache.services.info.base;

/**
 * A class that implement StateUpdateManager will take that StateUpate
 * objects entrusted to their care will be applied to the State in a timely
 * fashion.
 * <p>
 * Implementations of StateUpdateManager are expected not to block the
 * calling Thread when it enqueues a StateUpdate object.
 * <p>
 * The precise ordering in which the queued StateUpdate objects are processed
 * is not guaranteed. However, it is expected that a class implementing
 * StateUpdateManager will process queued StateUpdates until it has exhausted
 * the outstanding work.
 * <p>
 * The StateUpdateManager object is also responsible for expunging those
 * metrics that have expired. The StateCaretaker is queried to discover when
 * it is likely that metrics will need to be removed.
 *
 * @see StateCaretaker
 */
public interface StateUpdateManager
{
    /**
     * Instruct the StateUpdateManager to shutdown and wait for this to
     * happen. The StateUpdateManager should process any outstanding work
     * before stopping, so this method may block the calling thread.
     * <p>
     * It is only valid to call this method once per StateUpdateManager
     * object.
     *
     * @throws IllegalStateException
     *             if {@link #stop()} has already been called.
     */
    void shutdown();

    /**
     * Accept a StateUpdate object for processing. The StateUpdate object is
     * enqueued so this method may return quickly.
     *
     * @param pendingUpdate
     *            the StateUpdate that should be processed.
     */
    void enqueueUpdate(StateUpdate pendingUpdate);

    /**
     * Count the number of pending StateUpdate objects that are currently
     * queued.
     *
     * @return the number of pending updates.
     */
    int countPendingUpdates();
}
