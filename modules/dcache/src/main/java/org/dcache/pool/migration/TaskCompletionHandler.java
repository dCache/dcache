package org.dcache.pool.migration;

/**
 * Callback interface for migration Task completion.
 */
public interface TaskCompletionHandler
{
    /**
     * The task was cancelled.
     *
     * @param task
     * @see Task#cancel()
     */
    void taskCancelled(Task task);

    /**
     * The task failed with a transient error.
     *
     * @param task
     */
    void taskFailed(Task task, String msg);

    /**
     * The task failed with a permanent error and should not be retried.
     *
     * @param task
     */
    void taskFailedPermanently(Task task, String msg);

    /**
     * The task completed without error.
     *
     * @param task
     */
    void taskCompleted(Task task);
}
