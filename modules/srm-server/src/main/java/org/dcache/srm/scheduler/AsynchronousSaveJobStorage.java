package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

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

    private enum UpdateState
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
    public void saveJob(final J job, final boolean force)
    {
        UpdateState existingState;
        if (force) {
            existingState = states.put(job.getId(), UpdateState.QUEUED_FORCED);
        } else {
            existingState = states.putIfAbsent(job.getId(), UpdateState.QUEUED_NOT_FORCED);
            while (existingState == UpdateState.PROCESSING &&
                    !states.replace(job.getId(), UpdateState.PROCESSING, UpdateState.QUEUED_NOT_FORCED)) {
                existingState = states.putIfAbsent(job.getId(), UpdateState.QUEUED_NOT_FORCED);
            }
        }

        if (existingState == null) {
            boolean success = false;
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
                                } catch (TransactionException e) {
                                    LOGGER.error("SQL statement failed: {}", e.getMessage());
                                } catch (Throwable e) {
                                    Thread.currentThread().getUncaughtExceptionHandler().uncaughtException(Thread.currentThread(), e);
                                } finally {
                                    if (!states.remove(job.getId(), UpdateState.PROCESSING)) {
                                        executor.execute(this);
                                    }
                                }
                            }
                        }
                    };
            try {
                executor.execute(task);
                success = true;
            } catch (RejectedExecutionException e) {
                if (force) {
                    // Execute forced save synchronously, thus creating back pressure.
                    task.run();
                    success = true;
                } else {
                    LOGGER.warn("Persistence of request {} skipped, queue is too long.", job.getId());
                }
            } finally {
                if (!success) {
                    states.remove(job.getId());
                }
            }
        }
    }

    @Override
    public Set<Long> getLatestCompletedJobIds(int maxNum) throws DataAccessException
    {
        return storage.getLatestCompletedJobIds(maxNum);
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
