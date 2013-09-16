package org.dcache.srm.scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Set;

import org.dcache.srm.request.Job;

/**
 * Noop (No Operation) implementation of the JobStorage interface
 * @author timur
 */
public class NoopJobStorage<J extends Job> implements JobStorage<J> {

    public NoopJobStorage( ) {
    }

    @Override
    public void init() throws SQLException
    {
    }

    @Override
    public J getJob(long jobId) throws SQLException {
        return null;
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException {
        return null;
    }

    @Override
    public Set<J> getJobs(String scheduler) throws SQLException {
        return Collections.emptySet();
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws SQLException {
        return Collections.emptySet();
    }

    @Override
    public void saveJob(J job, boolean saveIfMonitoringDisabled) throws SQLException {
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws SQLException
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws SQLException
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws SQLException
    {
        return Collections.emptySet();
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws SQLException
    {
        return Collections.emptySet();
    }

    @Override
    public Set<J> getActiveJobs()
    {
        return Collections.emptySet();
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return false;
    }
}
