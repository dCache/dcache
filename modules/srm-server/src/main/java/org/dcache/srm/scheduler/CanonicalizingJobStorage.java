package org.dcache.srm.scheduler;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.dcache.srm.request.Job;

/**
 * Canonicalizing job storage.
 *
 * Guarantees that for a given job ID, the same Job instance is returned as long as
 * that instance is not garbage collected.
 *
 * The canonicalizing map is shared among all instances of CanonicalizingJobStorage,
 * thus Job IDs must be unique over all instances. This has the benefit
 * that the map allows early detection of job IDs that don't belong to the
 * decorated job storage and avoid an expensive load from the storage.
 */
public class CanonicalizingJobStorage<J extends Job> implements JobStorage<J>
{
    private static final ConcurrentMap<Long, Job> map = new MapMaker().weakValues().makeMap();
    private final JobStorage<J> storage;
    private final Class<J> type;

    public CanonicalizingJobStorage(JobStorage<J> storage, Class<J> type)
    {
        this.storage = storage;
        this.type = type;
    }

    private J canonicalize(Long jobId, J job)
    {
        if (job != null) {
            Job other = map.putIfAbsent(jobId, job);
            if (other != null) {
                job = type.cast(other);
            }
        }
        return job;
    }

    private Set<J> canonicalize(Set<J> jobs)
    {
        return Sets.newHashSet(Collections2.transform(jobs, new Function<J, J>()
        {
            @Override
            public J apply(J job)
            {
                return canonicalize(job.getId(), job);
            }
        }));
    }


    @Override
    public void init() throws DataAccessException
    {
        storage.init();
    }

    @Override
    public J getJob(long jobId) throws DataAccessException
    {
        Job job = map.get(jobId);
        if (job == null) {
            return canonicalize(jobId, storage.getJob(jobId));
        } else if (type.isInstance(job)) {
            return type.cast(job);
        } else {
            return null;
        }
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException
    {
        Job job = map.get(jobId);
        if (job == null) {
            return canonicalize(jobId, storage.getJob(jobId, connection));
        } else if (type.isInstance(job)) {
            return type.cast(job);
        } else {
            return null;
        }
    }

    @Override
    public Set<J> getJobs(String scheduler) throws DataAccessException
    {
        return canonicalize(storage.getJobs(scheduler));
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws DataAccessException
    {
        return canonicalize(storage.getJobs(scheduler, state));
    }

    @Override
    public void saveJob(J job, boolean saveIfMonitoringDisabled) throws DataAccessException
    {
        Job other = map.putIfAbsent(job.getId(), job);
        if (other != null && other != job) {
            throw new IllegalStateException("Duplicate job #" + job.getId());
        }
        storage.saveJob(job, saveIfMonitoringDisabled);
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return storage.isJdbcLogRequestHistoryInDBEnabled();
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
    public Set<J> getActiveJobs() throws DataAccessException
    {
        return canonicalize(storage.getActiveJobs());
    }
}
