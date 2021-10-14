package org.dcache.srm.scheduler;

import java.sql.Connection;
import java.util.Collections;
import java.util.Set;
import org.dcache.srm.request.Job;

/**
 * Noop (No Operation) implementation of the JobStorage interface
 *
 * @author timur
 */
public class NoopJobStorage<J extends Job> implements JobStorage<J> {

    public NoopJobStorage() {
    }

    @Override
    public void init() {
    }

    @Override
    public J getJob(long jobId) {
        return null;
    }

    @Override
    public J getJob(long jobId, Connection connection) {
        return null;
    }

    @Override
    public void saveJob(J job, boolean force) {
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) {
        return Collections.emptySet();
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) {
        return Collections.emptySet();
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) {
        return Collections.emptySet();
    }

    @Override
    public Set<J> getActiveJobs() {
        return Collections.emptySet();
    }

}
