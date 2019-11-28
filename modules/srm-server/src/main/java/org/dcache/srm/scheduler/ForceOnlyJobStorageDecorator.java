package org.dcache.srm.scheduler;

import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.dcache.srm.request.Job;

/**
 *
 * @author timur
 */
public class ForceOnlyJobStorageDecorator<J extends Job> implements JobStorage<J> {

    private final JobStorage<J> jobStorage;
    public ForceOnlyJobStorageDecorator(JobStorage<J> jobStorage ) {
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
    public void saveJob(J job, boolean force) throws TransactionException {
        if (force) {
            jobStorage.saveJob(job, force);
        }
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws DataAccessException
    {
        return jobStorage.getLatestCompletedJobIds(maxNum);
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

}
