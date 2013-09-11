package org.dcache.srm.request.sql;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

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
import org.dcache.srm.scheduler.FinalStateOnlyJobStorageDecorator;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.NoopJobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author timur
 */
public class DatabaseJobStorageFactory extends JobStorageFactory{
    private static final Logger logger =
            LoggerFactory.getLogger(DatabaseJobStorageFactory.class);
    private final Map<Class<? extends Job>,JobStorage<?>> jobStorageMap =
        new HashMap<>();

    private <J extends Job> void add(Configuration.DatabaseParameters config,
                     Class<J> entityClass,
                     Class<? extends DatabaseJobStorage<J>> storageClass)
        throws InstantiationException,
               IllegalAccessException,
               IllegalArgumentException,
               InvocationTargetException,
               NoSuchMethodException,
               SecurityException
    {
        if (config.isDatabaseEnabled()) {
            JobStorage<J> js =
                storageClass.getConstructor(Configuration.DatabaseParameters.class).newInstance(config);
            if (config.getStoreCompletedRequestsOnly()) {
                js = new FinalStateOnlyJobStorageDecorator<>(js);
            }
            jobStorageMap.put(entityClass, js);
        } else {
            jobStorageMap.put(entityClass, new NoopJobStorage<J>());
        }
    }

    public DatabaseJobStorageFactory(Configuration config) throws SQLException
    {
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

            Job.registerJobStorages(ImmutableMap.copyOf(jobStorageMap));
        } catch (InstantiationException e) {
            Throwables.propagateIfPossible(e.getCause(), SQLException.class);
            throw new RuntimeException("Request perisistence initialization failed: " + e.toString(), e);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException("Request perisistence initialization failed: " + e.toString(), e);
        }
    }

    public void loadExistingJobs() throws SQLException, InterruptedException, IllegalStateTransition
    {
        for (JobStorage<?> js: jobStorageMap.values()) {
            try {
                if (js instanceof DatabaseJobStorage) {
                    ((DatabaseJobStorage<?>) js).updatePendingJobs();
                }
            } catch (Exception e) {
                logger.error("updatePendingJobs failed",e);
            }
        }
        SchedulerFactory schedulerFactory =
                SchedulerFactory.getSchedulerFactory();

        for(Class<? extends Job> jobType: jobStorageMap.keySet()) {
            Scheduler scheduler;
            try {
                scheduler = schedulerFactory.getScheduler(jobType);
            } catch(UnsupportedOperationException uoe) {
                //ignore, not all types of jobs are scheduled
                //some are just containers for file requests
                continue;
            }
            // get all pending unscheduled jobs
            for (Job job: jobStorageMap.get(jobType).getJobs(null, State.PENDING)) {
                scheduler.schedule(job);
            }
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

}
