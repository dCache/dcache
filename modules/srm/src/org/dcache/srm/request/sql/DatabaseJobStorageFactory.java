package org.dcache.srm.request.sql;
import org.dcache.srm.request.*;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class DatabaseJobStorageFactory extends JobStorageFactory{
    private static final Logger logger =
            LoggerFactory.getLogger(DatabaseJobStorageFactory.class);
    private Map<Class,DatabaseJobStorage> dbJobStorageMap;

    public DatabaseJobStorageFactory(Configuration config) {
        try {
            dbJobStorageMap = new HashMap<Class,DatabaseJobStorage>();

            dbJobStorageMap.put(BringOnlineFileRequest.class,
                    new BringOnlineFileRequestStorage(config) );
            dbJobStorageMap.put(BringOnlineRequest.class,
                    new BringOnlineRequestStorage(config) );

            dbJobStorageMap.put(CopyFileRequest.class,
                    new CopyFileRequestStorage(config) );
            dbJobStorageMap.put(CopyRequest.class,
                    new CopyRequestStorage(config) );

            dbJobStorageMap.put(PutFileRequest.class,
                    new PutFileRequestStorage(config) );
            dbJobStorageMap.put(PutRequest.class,
                    new PutRequestStorage(config) );

            dbJobStorageMap.put(GetFileRequest.class,
                    new GetFileRequestStorage(config) );
            dbJobStorageMap.put(GetRequest.class,
                    new GetRequestStorage(config) );

            dbJobStorageMap.put(LsFileRequest.class,
                    new LsFileRequestStorage(config) );
            dbJobStorageMap.put(LsRequest.class,
                    new LsRequestStorage(config) );

            dbJobStorageMap.put(ReserveSpaceRequest.class,
                    new ReserveSpaceRequestStorage(config) );
            for(DatabaseJobStorage djs:dbJobStorageMap.values()) {
                Job.registerJobStorage(djs);
            }
            for(DatabaseJobStorage djs:dbJobStorageMap.values()) {
                try {
                    djs.updatePendingJobs();
                } catch (Exception e) {
                    logger.error("updatePendingJobs failed",e);
                }
            }
            SchedulerFactory schedulerFactory = 
                    SchedulerFactory.getSchedulerFactory();

            for(Class jobType:dbJobStorageMap.keySet()) {
                Scheduler scheduler;
                try {
                    scheduler = schedulerFactory.getScheduler(jobType);
                } catch(UnsupportedOperationException uoe) {
                    //ignore, not all types of jobs are scheuled
                    //some are just containers for file requests
                    continue;
                }
                DatabaseJobStorage djs = dbJobStorageMap.get(jobType);
                scheduler.jobStorageAdded(djs);
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

    public JobStorage getJobStorage(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        return getJobStorage(job.getClass());
    }

    public JobStorage getJobStorage(Class jobClass) {
        DatabaseJobStorage djs = dbJobStorageMap.get(jobClass);
        if(djs != null) return djs;
         throw new UnsupportedOperationException(
                 "JobStorage for class "+jobClass+ " is not supported");
    }

}
