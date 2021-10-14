package org.dcache.pool.migration;

/**
 * Callback interface for migration Task completion.
 */
public interface TaskCompletionHandler {

    /**
     * The task was cancelled.
     *
     * @see Task#cancel()
     */
    void taskCancelled(Task task);

    /**
     * The task failed with a transient error.
     */
    void taskFailed(Task task, int rc, String msg);

    /**
     * The task failed with a permanent error and should not be retried.
     */
    void taskFailedPermanently(Task task, int rc, String msg);

    /**
     * The task completed without error.
     */
    void taskCompleted(Task task);
}
