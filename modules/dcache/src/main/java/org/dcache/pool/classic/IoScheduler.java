package org.dcache.pool.classic;

import java.util.List;
import java.util.NoSuchElementException;

import diskCacheV111.vehicles.JobInfo;

import org.dcache.pool.movers.Mover;
import org.dcache.util.IoPriority;

/**
 * @since 1.9.11
 */
public interface IoScheduler {

    /**
     * Add a new request to schedule.
     *
     * @param transfer transfer to add.
     * @param priority priority of the transfer.
     * @return a mover id for the transfer
     */
    public int add(Mover<?> transfer, IoPriority priority);

    /**
     * Cancel the request. Any IO in progress will be interrupted.
     * @param id
     * @throws NoSuchElementException
     */
    public void cancel(int id) throws NoSuchElementException;

    /**
     * Get the maximal number of concurrently running jobs by this scheduler.
     *
     * @return maximal number of jobs.
     */
    public int getMaxActiveJobs();

    /**
     * Get current number of concurrently running jobs.
     * @return number of running jobs.
     */
    public int getActiveJobs();

    /**
     * Get number of requests waiting for execution.
     * @return number of pending requests.
     */
    public int getQueueSize();

    /**
     * Get the number of write requests running or waiting to run.
     */
    public int getCountByPriority(IoPriority priority);

    /**
     * Set maximal number of concurrently running jobs by this scheduler. All pending
     * jobs will be executed.
     * @param max
     */
    public void setMaxActiveJobs(int max);

    /**
     * Get the name of this scheduler.
     * @return name of the scheduler
     */
    public String getName();

    /**
     * Shutdown the scheduler. All subsequent execution request will be rejected.
     */
    public void shutdown() throws InterruptedException;

    // legacy crap
    public List<JobInfo> getJobInfos();

    /**
     * Get job information
     * @param id
     * @return
     * @throws NoSuchElementException if job with specified <code>id</code> does not exist
     */
    public JobInfo getJobInfo(int id) throws NoSuchElementException;

    public StringBuffer printJobQueue(StringBuffer sb);
}
