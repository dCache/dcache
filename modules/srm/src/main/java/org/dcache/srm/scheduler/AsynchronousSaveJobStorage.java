package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import org.dcache.srm.request.Job;

public class AsynchronousSaveJobStorage<J extends Job> implements JobStorage<J>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsynchronousSaveJobStorage.class);

    private final JobStorage<J> storage;
    private final ConcurrentMap<Long,UpdateState> states = new ConcurrentHashMap<>();
    private final ExecutorService executor;

    private static enum UpdateState
    {
        QUEUED, PROCESSING
    }

    public AsynchronousSaveJobStorage(JobStorage<J> storage, ExecutorService executor)
    {
        this.storage = storage;
        this.executor = executor;
    }

    @Override
    public void init() throws SQLException
    {
        storage.init();
    }

    @Override
    public J getJob(long jobId) throws SQLException
    {
        return storage.getJob(jobId);
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException
    {
        return storage.getJob(jobId, connection);
    }

    @Override
    public Set<J> getJobs(String scheduler) throws SQLException
    {
        return storage.getJobs(scheduler);
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws SQLException
    {
        return storage.getJobs(scheduler, state);
    }

    public void saveJob(final J job, final boolean saveIfMonitoringDisabled) throws SQLException
    {
        if (!saveIfMonitoringDisabled && !isJdbcLogRequestHistoryInDBEnabled()) {
            return;
        }
        if (states.put(job.getId(), UpdateState.QUEUED) == null) {
            boolean success = false;
            try {
                Runnable task =
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                states.put(job.getId(), UpdateState.PROCESSING);
                                try {
                                    storage.saveJob(job, saveIfMonitoringDisabled);
                                } catch (SQLException e) {
                                    LOGGER.error("SQL statement failed: {}", e.getMessage());
                                } catch (Throwable e) {
                                    Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
                                } finally {
                                    if (!states.remove(job.getId(), UpdateState.PROCESSING)) {
                                        executor.execute(this);
                                    }
                                }
                            }
                        };

                executor.execute(task);
                success = true;
            } catch (RejectedExecutionException e) {
                // ignore the saving errors, this will affect monitoring and
                // future status updates, but is not critical
                LOGGER.error("Persistence of request {} failed, queue is too long.", job.getId());
            } finally {
                if (!success) {
                    states.remove(job.getId());
                }
            }
        }
    }

    @Override
    public boolean isJdbcLogRequestHistoryInDBEnabled()
    {
        return storage.isJdbcLogRequestHistoryInDBEnabled();
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws SQLException
    {
        return storage.getLatestCompletedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestDoneJobIds(int maxNum) throws SQLException
    {
        return storage.getLatestDoneJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestFailedJobIds(int maxNum) throws SQLException
    {
        return storage.getLatestFailedJobIds(maxNum);
    }

    @Override
    public Set<Long> getLatestCanceledJobIds(int maxNum) throws SQLException
    {
        return storage.getLatestCanceledJobIds(maxNum);
    }

    @Override
    public Set<J> getActiveJobs() throws SQLException
    {
        return storage.getActiveJobs();
    }
}
