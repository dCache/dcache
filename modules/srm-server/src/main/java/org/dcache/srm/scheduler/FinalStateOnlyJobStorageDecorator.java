package org.dcache.srm.scheduler;

import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.dcache.srm.request.Job;

/**
 *
 * @author timur
 */
public class FinalStateOnlyJobStorageDecorator<J extends Job> implements JobStorage<J> {

    private final JobStorage<J> jobStorage;
    public FinalStateOnlyJobStorageDecorator(JobStorage<J> jobStorage ) {
        this.jobStorage = jobStorage;
    }

    @Override
    public void init() throws DataAccessException
    {
        jobStorage.init();
    }

    @Override
    public J getJob(long jobId) throws DataAccessException {
        return jobStorage.getJob(jobId);
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException {
        return jobStorage.getJob(jobId, connection);
    }

    @Override
    public Set<J> getJobs(String scheduler) throws DataAccessException {
        return jobStorage.getJobs(scheduler);
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws DataAccessException {
        return jobStorage.getJobs(scheduler, state);
    }

    @Override
    public void saveJob(J job, boolean force) throws DataAccessException {
        if (force || job.getState().isFinal()) {
            jobStorage.saveJob(job, force);
        }
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws DataAccessException
    {
        return jobStorage.getLatestCompletedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws DataAccessException
    {
        return jobStorage.getLatestDoneJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws DataAccessException
    {
        return jobStorage.getLatestFailedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws DataAccessException
    {
        return jobStorage.getLatestCanceledJobIds(maxNum);
    }

    @Override
    public Set<J> getActiveJobs()
    {
        return jobStorage.getActiveJobs();
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return jobStorage.isJdbcLogRequestHistoryInDBEnabled();
    }
}
