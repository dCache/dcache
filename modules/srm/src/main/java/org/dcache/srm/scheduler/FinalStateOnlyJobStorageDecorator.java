package org.dcache.srm.scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
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
    public void init() throws SQLException
    {
        jobStorage.init();
    }

    @Override
    public J getJob(long jobId) throws SQLException {
        return jobStorage.getJob(jobId);
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException {
        return jobStorage.getJob(jobId, connection);
    }

    @Override
    public Set<J> getJobs(String scheduler) throws SQLException {
        return jobStorage.getJobs(scheduler);
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws SQLException {
        return jobStorage.getJobs(scheduler, state);
    }

    @Override
    public void saveJob(J job, boolean saveIfMonitoringDisabled) throws SQLException {
        if(job.getState().isFinalState()) {
            jobStorage.saveJob(job,saveIfMonitoringDisabled);
        }
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws SQLException
    {
        return jobStorage.getLatestCompletedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws SQLException
    {
        return jobStorage.getLatestDoneJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws SQLException
    {
        return jobStorage.getLatestFailedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws SQLException
    {
        return jobStorage.getLatestCanceledJobIds(maxNum);
    }

    @Override
    public Set<J> getActiveJobs()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return jobStorage.isJdbcLogRequestHistoryInDBEnabled();
    }
}
