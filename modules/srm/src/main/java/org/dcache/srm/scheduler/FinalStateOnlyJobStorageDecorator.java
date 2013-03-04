package org.dcache.srm.scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;

import org.dcache.srm.request.Job;

/**
 *
 * @author timur
 */
public class FinalStateOnlyJobStorageDecorator implements JobStorage {

    private final JobStorage jobStorage;
    public FinalStateOnlyJobStorageDecorator(JobStorage jobStorage ) {
        this.jobStorage = jobStorage;
    }

    @Override
    public Job getJob(Long jobId) throws SQLException {
        return jobStorage.getJob(jobId);
    }

    @Override
    public Job getJob(Long jobId, Connection connection) throws SQLException {
        return jobStorage.getJob(jobId, connection);
    }

    @Override
    public Set<Job> getJobs(String scheduler) throws SQLException {
        return jobStorage.getJobs(scheduler);
    }

    @Override
    public Set<Job> getJobs(String scheduler, State state) throws SQLException {
        return jobStorage.getJobs(scheduler, state);
    }

    @Override
    public void saveJob(Job job, boolean saveIfMonitoringDisabled) throws SQLException {
        if(job == null) {
            throw new NullPointerException("job is null");
        }
        if(job.getState().isFinalState()) {
            jobStorage.saveJob(job,saveIfMonitoringDisabled);
        }
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return jobStorage.isJdbcLogRequestHistoryInDBEnabled();
    }
}
