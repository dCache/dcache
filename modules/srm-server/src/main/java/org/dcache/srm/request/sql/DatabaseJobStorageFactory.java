package org.dcache.srm.request.sql;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.dcache.srm.SRMUserPersistenceManager;
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
import org.dcache.srm.scheduler.ForceOnlyJobStorageDecorator;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.NoopJobStorage;
import org.dcache.srm.scheduler.SchedulerContainer;
import org.dcache.srm.scheduler.SharedMemoryCacheJobStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Configuration.DatabaseParameters;

import static com.google.common.base.Preconditions.checkNotNull;

public class DatabaseJobStorageFactory extends JobStorageFactory
{
    private final Map<Class<? extends Job>, JobStorage<?>> jobStorageMap =
        new LinkedHashMap<>(); // JobStorage initialization order is significant to ensure file
                               // requests are cached before container requests are loaded
    private final Map<Class<? extends Job>, JobStorage<?>> unmodifiableJobStorageMap =
            Collections.unmodifiableMap(jobStorageMap);
    private final Map<Class<? extends Job>, DatabaseParameters> configurations =
            new HashMap<>();
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduledExecutor;

    private <J extends Job> void add(DatabaseParameters config, Class<J> entityClass,
                     Supplier<JobStorage<J>> storageFactory)
            throws InstantiationException,
                   IllegalAccessException,
                   IllegalArgumentException,
                   InvocationTargetException,
                   NoSuchMethodException,
                   SecurityException, DataAccessException
    {
        JobStorage<J> js;
        if (config.isDatabaseEnabled()) {
            js = storageFactory.get();
            js = new AsynchronousSaveJobStorage<>(js, executor);
            if (config.getStoreCompletedRequestsOnly()) {
                js = new ForceOnlyJobStorageDecorator<>(js);
            }
        } else {
            js = new NoopJobStorage<>();
        }
        jobStorageMap.put(entityClass, new CanonicalizingJobStorage<>(new SharedMemoryCacheJobStorage<>(js, entityClass), entityClass));
        configurations.put(entityClass, config);
    }

    public DatabaseJobStorageFactory(@Nonnull String srmId, Configuration config, SRMUserPersistenceManager manager)
            throws DataAccessException, IOException
    {
        checkNotNull(srmId);
        checkNotNull(manager);
        executor = new ThreadPoolExecutor(
                config.getJdbcExecutionThreadNum(), config.getJdbcExecutionThreadNum(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(config.getMaxQueuedJdbcTasksNum()),
                new ThreadFactoryBuilder().setNameFormat("srm-db-save-%d").build());
        scheduledExecutor =
                Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("srm-db-gc-%d").build());
        try {
            add(config.getDatabaseParametersForBringOnline(),
                BringOnlineFileRequest.class,
                () -> new BringOnlineFileRequestStorage(config.getDatabaseParametersForBringOnline(), scheduledExecutor));
            add(config.getDatabaseParametersForBringOnline(),
                BringOnlineRequest.class,
                () -> new BringOnlineRequestStorage(srmId, config.getDatabaseParametersForBringOnline(), scheduledExecutor, manager));

            add(config.getDatabaseParametersForCopy(),
                CopyFileRequest.class,
                () -> new CopyFileRequestStorage(config.getDatabaseParametersForCopy(), scheduledExecutor));
            add(config.getDatabaseParametersForCopy(),
                CopyRequest.class,
                () -> new CopyRequestStorage(srmId, config.getDatabaseParametersForCopy(), scheduledExecutor, manager));

            add(config.getDatabaseParametersForPut(),
                PutFileRequest.class,
                () -> new PutFileRequestStorage(config.getDatabaseParametersForPut(), scheduledExecutor));
            add(config.getDatabaseParametersForPut(),
                PutRequest.class,
                () -> new PutRequestStorage(srmId, config.getDatabaseParametersForPut(), scheduledExecutor, manager));

            add(config.getDatabaseParametersForGet(),
                GetFileRequest.class,
                () -> new GetFileRequestStorage(config.getDatabaseParametersForGet(), scheduledExecutor));
            add(config.getDatabaseParametersForGet(),
                GetRequest.class,
                () -> new GetRequestStorage(srmId, config.getDatabaseParametersForGet(), scheduledExecutor, manager));

            add(config.getDatabaseParametersForList(),
                LsFileRequest.class,
                () -> new LsFileRequestStorage(config.getDatabaseParametersForList(), scheduledExecutor));
            add(config.getDatabaseParametersForList(),
                LsRequest.class,
                () -> new LsRequestStorage(srmId, config.getDatabaseParametersForList(), scheduledExecutor, manager));

            add(config.getDatabaseParametersForReserve(),
                ReserveSpaceRequest.class,
                () -> new ReserveSpaceRequestStorage(srmId, config.getDatabaseParametersForReserve(), scheduledExecutor, manager));
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
        for (Map.Entry<Class<? extends Job>, JobStorage<?>> entry : jobStorageMap.entrySet()) {
            DatabaseParameters config = configurations.get(entry.getKey());
            Set<? extends Job> jobs = entry.getValue().getActiveJobs();
            schedulers.restoreJobsOnSrmStart(jobs, config.isCleanPendingRequestsOnRestart());
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
