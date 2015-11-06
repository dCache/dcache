package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.dcache.srm.request.Job;
import org.dcache.srm.util.JDC;

public class AsynchronousSaveJobStorage<J extends Job> implements JobStorage<J>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(AsynchronousSaveJobStorage.class);

    private final JobStorage<J> storage;
    private final ConcurrentMap<Long,UpdateState> states = new ConcurrentHashMap<>();
    private final Executor executor;

    private static enum UpdateState
    {
        QUEUED_FORCED, QUEUED_NOT_FORCED, PROCESSING
    }

    public AsynchronousSaveJobStorage(JobStorage<J> storage, Executor executor)
    {
        this.storage = storage;
        this.executor = executor;
    }

    @Override
    public void init() throws DataAccessException
    {
        storage.init();
    }

    @Override
    public J getJob(long jobId) throws DataAccessException
    {
        return storage.getJob(jobId);
    }

    @Override
    public J getJob(long jobId, Connection connection) throws SQLException
    {
        return storage.getJob(jobId, connection);
    }

    @Override
    public Set<J> getJobs(String scheduler) throws DataAccessException
    {
        return storage.getJobs(scheduler);
    }

    @Override
    public Set<J> getJobs(String scheduler, State state) throws DataAccessException
    {
        return storage.getJobs(scheduler, state);
    }

    public void saveJob(final J job, final boolean force)
    {
        if (!force && !isJdbcLogRequestHistoryInDBEnabled()) {
            return;
        }

        UpdateState state;
        if (force) {
            state = states.put(job.getId(), UpdateState.QUEUED_FORCED);
        } else {
            while ((state = states.putIfAbsent(job.getId(), UpdateState.QUEUED_NOT_FORCED)) == UpdateState.PROCESSING &&
                    !states.replace(job.getId(), UpdateState.PROCESSING, UpdateState.QUEUED_NOT_FORCED));
        }

        if (state == null) {
            boolean success = false;
            try {
                Runnable task =
                        new Runnable()
                        {
                            @Override
                            public void run()
                            {
                                try (JDC ignored = job.applyJdc()) {
                                    UpdateState state = states.put(job.getId(), UpdateState.PROCESSING);
                                    try {
                                        storage.saveJob(job, state == UpdateState.QUEUED_FORCED);
                                    } catch (DataAccessException e) {
                                        LOGGER.error("SQL statement failed: {}", e.getMessage());
                                    } catch (Throwable e) {
                                        Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(
                                                Thread.currentThread(), e);
                                    } finally {
                                        if (!states.remove(job.getId(), UpdateState.PROCESSING)) {
                                            executor.execute(this);
                                        }
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
        return storage.getActiveJobs();
    }
}
