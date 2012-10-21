package org.dcache.srm.scheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.Collections;
import org.dcache.srm.request.Job;

/**
 * Noop (No Operation) implementation of the JobStorage interface
 * @author timur
 */
public class NoopJobStorage implements JobStorage {

    public NoopJobStorage( ) {
    }

    @Override
    public Job getJob(Long jobId) throws SQLException {
        return null;
    }

    @Override
    public Job getJob(Long jobId, Connection connection) throws SQLException {
        return null;
    }

    @Override
    public Set getJobs(String scheduler) throws SQLException {
        return Collections.emptySet();
    }

    @Override
    public Set getJobs(String scheduler, State state) throws SQLException {
        return Collections.emptySet();
    }

    @Override
    public void saveJob(Job job, boolean saveIfMonitoringDisabled) throws SQLException {
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return false;
    }
}
