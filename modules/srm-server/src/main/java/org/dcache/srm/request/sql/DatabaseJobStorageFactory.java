package org.dcache.srm.request.sql;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.dao.DataAccessException;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.CopyFileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.scheduler.AsynchronousSaveJobStorage;
import org.dcache.srm.scheduler.CanonicalizingJobStorage;
import org.dcache.srm.scheduler.FinalStateOnlyJobStorageDecorator;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.NoopJobStorage;
import org.dcache.srm.scheduler.SchedulerContainer;
import org.dcache.srm.scheduler.SharedMemoryCacheJobStorage;
import org.dcache.srm.util.Configuration;

public class DatabaseJobStorageFactory extends JobStorageFactory
{
    private final Map<Class<? extends Job>, JobStorage<?>> jobStorageMap =
        new LinkedHashMap<>(); // JobStorage initialization order is significant to ensure file
                               // requests are cached before container requests are loaded
    private final Map<Class<? extends Job>, JobStorage<?>> unmodifiableJobStorageMap =
            Collections.unmodifiableMap(jobStorageMap);
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;

    private <J extends Job> void add(Configuration.DatabaseParameters config,
                     Class<J> entityClass,
                     Class<? extends DatabaseJobStorage<J>> storageClass)
            throws InstantiationException,
                   IllegalAccessException,
                   IllegalArgumentException,
                   InvocationTargetException,
                   NoSuchMethodException,
                   SecurityException, DataAccessException
    {
        JobStorage<J> js;
        if (config.isDatabaseEnabled()) {
            js = storageClass
                    .getConstructor(Configuration.DatabaseParameters.class, ScheduledExecutorService.class)
                    .newInstance(config, scheduledExecutor);
            js = new AsynchronousSaveJobStorage<>(js, executor);
            if (config.getStoreCompletedRequestsOnly()) {
                js = new FinalStateOnlyJobStorageDecorator<>(js);
            }
        } else {
            js = new NoopJobStorage<>();
        }
        jobStorageMap.put(entityClass, new CanonicalizingJobStorage<>(new SharedMemoryCacheJobStorage<>(js, entityClass), entityClass));
    }

    public DatabaseJobStorageFactory(Configuration config) throws DataAccessException, IOException
    {
        executor = new ThreadPoolExecutor(
                config.getJdbcExecutionThreadNum(), config.getJdbcExecutionThreadNum(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(config.getMaxQueuedJdbcTasksNum()),
                new ThreadFactoryBuilder().setNameFormat("srm-db-save-%d").build());
        scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("srm-db-gc-%d").build());
        try {
            add(config.getDatabaseParametersForBringOnline(),
                BringOnlineFileRequest.class,
                BringOnlineFileRequestStorage.class);
            add(config.getDatabaseParametersForBringOnline(),
                BringOnlineRequest.class,
                BringOnlineRequestStorage.class);

            add(config.getDatabaseParametersForCopy(),
                CopyFileRequest.class,
                CopyFileRequestStorage.class);
            add(config.getDatabaseParametersForCopy(),
                CopyRequest.class,
                CopyRequestStorage.class);

            add(config.getDatabaseParametersForPut(),
                PutFileRequest.class,
                PutFileRequestStorage.class);
            add(config.getDatabaseParametersForPut(),
                PutRequest.class,
                PutRequestStorage.class);

            add(config.getDatabaseParametersForGet(),
                GetFileRequest.class,
                GetFileRequestStorage.class);
            add(config.getDatabaseParametersForGet(),
                GetRequest.class,
                GetRequestStorage.class);

            add(config.getDatabaseParametersForList(),
                LsFileRequest.class,
                LsFileRequestStorage.class);
            add(config.getDatabaseParametersForList(),
                LsRequest.class,
                LsRequestStorage.class);

            add(config.getDatabaseParametersForReserve(),
                ReserveSpaceRequest.class,
                ReserveSpaceRequestStorage.class);
        } catch (InstantiationException e) {
            Throwables.propagateIfPossible(e.getCause(), IOException.class);
            throw new RuntimeException("Request persistence initialization failed: " + e.toString(), e);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Request persistence initialization failed: " + e.toString(), e);
        }
    }

    public void init() throws DataAccessException
    {
        for (JobStorage<?> jobStorage : jobStorageMap.values()) {
            jobStorage.init();
        }
    }

    public void restoreJobsOnSrmStart(SchedulerContainer schedulers)
    {
        for (JobStorage<?> storage: jobStorageMap.values()) {
            Set<? extends Job> jobs = storage.getActiveJobs();
            schedulers.restoreJobsOnSrmStart(jobs);
        }
    }

    public void shutdown()
    {
        scheduledExecutor.shutdown();
        executor.shutdown();
        try {
            if (scheduledExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
                executor.awaitTermination(3, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <J extends Job> JobStorage<J> getJobStorage(J job) {
        return getJobStorage((Class<J>) job.getClass());
    }

    @Override
    @SuppressWarnings("unchecked")
    public <J extends Job> JobStorage<J> getJobStorage(Class<? extends J> jobClass) {
        JobStorage<J> js = (JobStorage<J>) jobStorageMap.get(jobClass);
        if (js == null) {
            throw new UnsupportedOperationException(
                    "JobStorage for class " + jobClass + " is not supported");
        }
        return js;
    }

    @Override
    public Map<Class<? extends Job>, JobStorage<?>> getJobStorages()
    {
        return unmodifiableJobStorageMap;
    }

}
