package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import org.dcache.srm.request.BringOnlineFileRequest;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.GetFileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.PutFileRequest;
import org.dcache.srm.request.ReserveSpaceRequest;
import org.dcache.srm.util.Configuration;

/**
 *
 * @author timur
 */
public class SchedulerFactory {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerFactory.class);
    private final Map<Class<? extends Job>,Scheduler> schedulerMap;
    private static SchedulerFactory factory;

    private SchedulerFactory(Configuration config, String name) {
        schedulerMap = new HashMap<>();

        Scheduler lsRequestScheduler = new Scheduler("ls_" + name, LsFileRequest.class);
        // scheduler parameters
        lsRequestScheduler.setMaxThreadQueueSize(config.getLsReqTQueueSize());
        lsRequestScheduler.setThreadPoolSize(config.getLsThreadPoolSize());
        lsRequestScheduler.setMaxWaitingJobNum(config.getLsMaxWaitingRequests());
        lsRequestScheduler.setMaxReadyQueueSize(config.getLsReadyQueueSize());
        lsRequestScheduler.setMaxReadyJobs(config.getLsMaxReadyJobs());
        lsRequestScheduler.setMaxNumberOfRetries(config.getLsMaxNumOfRetries());
        lsRequestScheduler.setRetryTimeout(config.getLsRetryTimeout());
        lsRequestScheduler.setMaxRunningByOwner(config.getLsMaxRunningBySameOwner());
        lsRequestScheduler.setPriorityPolicyPlugin(config.getLsPriorityPolicyPlugin());
        lsRequestScheduler.start();
        schedulerMap.put(LsFileRequest.class,lsRequestScheduler);


        Scheduler getRequestScheduler = new Scheduler("get_" + name, GetFileRequest.class);
        // scheduler parameters
        getRequestScheduler.setMaxThreadQueueSize(config.getGetReqTQueueSize());
        getRequestScheduler.setThreadPoolSize(config.getGetThreadPoolSize());
        getRequestScheduler.setMaxWaitingJobNum(config.getGetMaxWaitingRequests());
        getRequestScheduler.setMaxReadyQueueSize(config.getGetReadyQueueSize());
        getRequestScheduler.setMaxReadyJobs(config.getGetMaxReadyJobs());
        getRequestScheduler.setMaxNumberOfRetries(config.getGetMaxNumOfRetries());
        getRequestScheduler.setRetryTimeout(config.getGetRetryTimeout());
        getRequestScheduler.setMaxRunningByOwner(config.getGetMaxRunningBySameOwner());
        getRequestScheduler.setPriorityPolicyPlugin(config.getGetPriorityPolicyPlugin());
        getRequestScheduler.start();
        schedulerMap.put(GetFileRequest.class,getRequestScheduler);


        Scheduler bringOnlineRequestScheduler = new Scheduler("bring_online_" + name, BringOnlineFileRequest.class);
        // scheduler parameters
        bringOnlineRequestScheduler.setMaxThreadQueueSize(config.getBringOnlineReqTQueueSize());
        bringOnlineRequestScheduler.setThreadPoolSize(config.getBringOnlineThreadPoolSize());
        bringOnlineRequestScheduler.setMaxWaitingJobNum(config.getBringOnlineMaxWaitingRequests());
        bringOnlineRequestScheduler.setMaxReadyQueueSize(config.getBringOnlineReadyQueueSize());
        bringOnlineRequestScheduler.setMaxReadyJobs(config.getBringOnlineMaxReadyJobs());
        bringOnlineRequestScheduler.setMaxNumberOfRetries(config.getBringOnlineMaxNumOfRetries());
        bringOnlineRequestScheduler.setRetryTimeout(config.getBringOnlineRetryTimeout());
        bringOnlineRequestScheduler.setMaxRunningByOwner(config.getBringOnlineMaxRunningBySameOwner());
        bringOnlineRequestScheduler.setPriorityPolicyPlugin(config.getBringOnlinePriorityPolicyPlugin());
        bringOnlineRequestScheduler.start();
        schedulerMap.put(BringOnlineFileRequest.class,bringOnlineRequestScheduler);


        Scheduler putRequestScheduler = new Scheduler("put_" + name, PutFileRequest.class);
        // scheduler parameters
        putRequestScheduler.setMaxThreadQueueSize(config.getPutReqTQueueSize());
        putRequestScheduler.setThreadPoolSize(config.getPutThreadPoolSize());
        putRequestScheduler.setMaxWaitingJobNum(config.getPutMaxWaitingRequests());
        putRequestScheduler.setMaxReadyQueueSize(config.getPutReadyQueueSize());
        putRequestScheduler.setMaxReadyJobs(config.getPutMaxReadyJobs());
        putRequestScheduler.setMaxNumberOfRetries(config.getPutMaxNumOfRetries());
        putRequestScheduler.setRetryTimeout(config.getPutRetryTimeout());
        putRequestScheduler.setMaxRunningByOwner(config.getPutMaxRunningBySameOwner());
        putRequestScheduler.setPriorityPolicyPlugin(config.getPutPriorityPolicyPlugin());
        putRequestScheduler.start();
        schedulerMap.put(PutFileRequest.class,putRequestScheduler);

        Scheduler copyRequestScheduler = new Scheduler("copy_" + name, Job.class);
        // scheduler parameters
        copyRequestScheduler.setMaxThreadQueueSize(config.getCopyReqTQueueSize());
        copyRequestScheduler.setThreadPoolSize(config.getCopyThreadPoolSize());
        copyRequestScheduler.setMaxWaitingJobNum(config.getCopyMaxWaitingRequests());
        copyRequestScheduler.setMaxNumberOfRetries(config.getCopyMaxNumOfRetries());
        copyRequestScheduler.setRetryTimeout(config.getCopyRetryTimeout());
        copyRequestScheduler.setMaxRunningByOwner(config.getCopyMaxRunningBySameOwner());
        copyRequestScheduler.setPriorityPolicyPlugin(config.getCopyPriorityPolicyPlugin());
        copyRequestScheduler.start();
        schedulerMap.put(CopyRequest.class,copyRequestScheduler);

        Scheduler reserveSpaceScheduler = new Scheduler("reserve_space_" + name, ReserveSpaceRequest.class);
        reserveSpaceScheduler.setMaxThreadQueueSize(config.getReserveSpaceReqTQueueSize());
        reserveSpaceScheduler.setThreadPoolSize(config.getReserveSpaceThreadPoolSize());
        reserveSpaceScheduler.setMaxWaitingJobNum(config.getReserveSpaceMaxWaitingRequests());
        reserveSpaceScheduler.setMaxReadyQueueSize(config.getReserveSpaceReadyQueueSize());
        reserveSpaceScheduler.setMaxReadyJobs(config.getReserveSpaceMaxReadyJobs());
        reserveSpaceScheduler.setMaxNumberOfRetries(config.getReserveSpaceMaxNumOfRetries());
        reserveSpaceScheduler.setRetryTimeout(config.getReserveSpaceRetryTimeout());
        reserveSpaceScheduler.setMaxRunningByOwner(config.getReserveSpaceMaxRunningBySameOwner());
        reserveSpaceScheduler.setPriorityPolicyPlugin(config.getReserveSpacePriorityPolicyPlugin());
        reserveSpaceScheduler.start();
        schedulerMap.put(ReserveSpaceRequest.class,reserveSpaceScheduler);
    }

    public void shutdown() {
        for( Scheduler scheduler : schedulerMap.values()) {
            scheduler.stop();
        }
    }

    public static void initSchedulerFactory(Configuration config, String name) {
        initSchedulerFactory( new SchedulerFactory(config,name));
    }

    public synchronized static void initSchedulerFactory(SchedulerFactory afactory) {
        if(afactory == null) {
            throw new NullPointerException(" factory argument is null");
        }
        if(factory == null) {
            factory = afactory;
        }  else {
            throw new IllegalStateException("already initialized");
        }
    }


    public static synchronized SchedulerFactory getSchedulerFactory() {
        if(factory == null) {
            throw new IllegalStateException("not initialized");
        }
        return factory;
    }


    public Scheduler getScheduler(Job job) {
        if(job == null) {
            throw new IllegalArgumentException("job is null");
        }
        return getScheduler(job.getClass());

    }

    public Scheduler getScheduler(Class<? extends Job> jobType) {
        Scheduler scheduler = schedulerMap.get(jobType);
        if(scheduler != null) {
            return scheduler;
        }
        throw new UnsupportedOperationException(
                "Scheduler for class "+jobType+ " is not supported");
    }
}
