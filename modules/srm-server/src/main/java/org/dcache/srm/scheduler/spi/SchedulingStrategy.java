package org.dcache.srm.scheduler.spi;

import org.dcache.srm.request.Job;

/**
 * SRM request scheduling strategy.
 *
 * Implementations of this class are simply queues of Jobs. Implementations are free
 * to reorder Jobs however they like and may thus implement different scheduling
 * algorithms.
 *
 * Implementations should avoid keeping references to the actual jobs and instead
 * use the IDs of the jobs. This is to avoid locking the queued jobs into memory in
 * case the environment decides to keep the jobs in a database.
 *
 * Implementations must be safe to use by multiple concurrent threads.
 */
public interface SchedulingStrategy
{
    /**
     * Adds a new job.
     */
    void add(Job job);

    /**
     * Removes and returns the ID of the <em>next job</em>. How the <em>next job</em> is
     * defined is the essence of any implementation of this interface.
     *
     * @return The ID of the next job to process, or null if there are no jobs to process.
     */
    Long remove();

    /**
     * Number of jobs in the queue.
     */
    int size();
}
