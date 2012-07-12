package org.dcache.srm.request.sql;
import org.dcache.srm.request.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.NoopJobStorage;
import org.dcache.srm.scheduler.FinalStateOnlyJobStorageDecorator;
import org.dcache.srm.util.Configuration;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.lang.reflect.InvocationTargetException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DatabaseJobStorageFactory extends JobStorageFactory{
    private static final Logger logger =
            LoggerFactory.getLogger(DatabaseJobStorageFactory.class);
    private static final NoopJobStorage noop = new NoopJobStorage();
    private final Map<Class<?>,JobStorage> jobStorageMap =
        new HashMap<Class<?>,JobStorage>();

    private void add(Configuration.DatabaseParameters config,
                     Class<?> entityClass,
                     Class<? extends DatabaseJobStorage> storageClass)
        throws InstantiationException,
               IllegalAccessException,
               IllegalArgumentException,
               InvocationTargetException,
               NoSuchMethodException,
               SecurityException
    {
        if (config.isDatabaseEnabled()) {
            JobStorage js =
                storageClass.getConstructor(Configuration.DatabaseParameters.class).newInstance(config);
            if (config.getStoreCompletedRequestsOnly()) {
                js = new FinalStateOnlyJobStorageDecorator(js);
            }
            jobStorageMap.put(entityClass, js);
        } else {
            jobStorageMap.put(entityClass, noop);
        }
    }

    public DatabaseJobStorageFactory(Configuration config) {
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

            for (JobStorage js: jobStorageMap.values()) {
                Job.registerJobStorage(js);
            }
            for (JobStorage js: jobStorageMap.values()) {
                try {
                    if (js instanceof DatabaseJobStorage) {
                        ((DatabaseJobStorage) js).updatePendingJobs();
                    }
                } catch (Exception e) {
                    logger.error("updatePendingJobs failed",e);
                }
            }
            SchedulerFactory schedulerFactory =
                    SchedulerFactory.getSchedulerFactory();

            for(Class jobType: jobStorageMap.keySet()) {
                Scheduler scheduler;
                try {
                    scheduler = schedulerFactory.getScheduler(jobType);
                } catch(UnsupportedOperationException uoe) {
                    //ignore, not all types of jobs are scheuled
                    //some are just containers for file requests
                    continue;
                }
                JobStorage djs = jobStorageMap.get(jobType);
                // get all pending unsheduled jobs
                Set<Job> jobs = djs.getJobs(null, State.PENDING);
                for(Job job:jobs) {
                    scheduler.schedule(job);
                }
            }


        } catch(Exception e) {
            throw new RuntimeException("DatabaseJobStorageFactory intialization",
                    e);
        }
    }

    @Override
    public JobStorage getJobStorage(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        return getJobStorage(job.getClass());
    }

    @Override
    public JobStorage getJobStorage(Class jobClass) {
        JobStorage js = jobStorageMap.get(jobClass);
        if (js != null) {
            return js;
        }
        throw new UnsupportedOperationException(
                 "JobStorage for class "+jobClass+ " is not supported");
    }

}
