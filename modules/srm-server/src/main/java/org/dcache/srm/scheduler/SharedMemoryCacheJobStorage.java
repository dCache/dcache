package org.dcache.srm.scheduler;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;

import org.dcache.srm.request.Job;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Canonicalizing and caching job storage decorator.
 *
 * Guarantees that for jobs with a given scheduler id, for a given job ID the same
 * Job instance is returned as long as the job is not in a final state. Other jobs
 * are not canonicalized.
 *
 * All non-final jobs with the given scheduler id are cached in a static SharedMemoryCache.
 * The expiration time of such jobs are periodically checked and the jobs are expired
 * when the time has passed.
 *
 * Since the cache is shared among all instances of SharedMemoryCacheJobStorage,
 * Job IDs must be unique over all instances. This has the additional benefit
 * that the cache allows early detection of job IDs that don't belong to the
 * decorated job storage: We can detect if an ID belongs to a job storage with a
 * different type and avoid an expensive probe from the decorated storage.
 */
public class SharedMemoryCacheJobStorage<J extends Job> implements JobStorage<J>
{
    private static final SharedMemoryCache sharedMemoryCache =
            new SharedMemoryCache();

    private static final Timer timer = new Timer("Job expiration", true);
    private final JobStorage<J> storage;
    private final Class<J> type;
    private final String schedulerId;
    private final Set<J> jobsToExpire = Collections.newSetFromMap(new ConcurrentHashMap<>());

    public SharedMemoryCacheJobStorage(JobStorage<J> storage, Class<J> type, String schedulerId)
    {
        this.storage = requireNonNull(storage);
        this.type = requireNonNull(type);
        this.schedulerId = requireNonNull(schedulerId);
    }

    private J canonicalize(J job)
    {
        if (job == null) {
            return null;
        }
        if (!schedulerId.equals(job.getSchedulerId())) {
            return job;
        }
        J canonicalJob = sharedMemoryCache.canonicalize(job);
        if (job == canonicalJob) {
            updateExpirationSet(job);
        }
        return canonicalJob;
    }

    private void updateExpirationSet(J job)
    {
        if (job.getState().isFinal()) {
            jobsToExpire.remove(job);
        } else {
            jobsToExpire.add(job);
        }
    }

    @Override
    public void init() throws DataAccessException
    {
        storage.init();
        storage.getActiveJobs(schedulerId).forEach(this::canonicalize);
        timer.schedule(new ExpirationTask(), SECONDS.toMillis(10), SECONDS.toMillis(60));
    }

    @Override
    public J getJob(long jobId) throws DataAccessException
    {
        Job jobFromCache = sharedMemoryCache.getJob(jobId);
        if (jobFromCache == null) {
            return canonicalize(storage.getJob(jobId));
        } else if (type.isInstance(jobFromCache)) {
            return type.cast(jobFromCache);
        } else {
            return null;
        }
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException
    {
        Job jobFromCache = sharedMemoryCache.getJob(jobId);
        if (jobFromCache == null) {
            return canonicalize(storage.getJob(jobId, connection));
        } else if (type.isInstance(jobFromCache)) {
            return type.cast(jobFromCache);
        } else {
            return null;
        }
    }

    @Override
    public void saveJob(J job, boolean force) throws DataAccessException
    {
        storage.saveJob(job, force);
        sharedMemoryCache.update(job);
        updateExpirationSet(job);
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws DataAccessException
    {
        return storage.getLatestCompletedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws DataAccessException
    {
        return storage.getLatestDoneJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws DataAccessException
    {
        return storage.getLatestFailedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws DataAccessException
    {
        return storage.getLatestCanceledJobIds(maxNum);
    }

    @Override
    public Set<J> getActiveJobs(String schedulerId)
    {
        return schedulerId.equals(this.schedulerId)
               ? sharedMemoryCache.getJobs(type)
               : storage.getActiveJobs(schedulerId);
    }

    private class ExpirationTask extends TimerTask
    {
        @Override
        public void run()
        {
            for (J job : jobsToExpire) {
                try {
                    job.checkExpiration();
                } catch (RuntimeException e) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, e);
                }
            }
        }
    }
}
