package org.dcache.srm.scheduler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.*;
import java.util.Map;
import java.util.HashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author timur
 */
public class SchedulerFactory {
    private static final Logger logger = LoggerFactory.getLogger(SchedulerFactory.class);
    private final Map<Class,Scheduler> schedulerMap;
    private final String name;
    private static SchedulerFactory factory=null;

    private SchedulerFactory(Configuration config, String name) {
        schedulerMap = new HashMap<Class,Scheduler>();
        
        Scheduler lsRequestScheduler = new Scheduler("ls_" + name);
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


        Scheduler getRequestScheduler = new Scheduler("get_" + name);
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


        Scheduler bringOnlineRequestScheduler = new Scheduler("bring_online_" + name);
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


        Scheduler putRequestScheduler = new Scheduler("put_" + name);
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

        Scheduler copyRequestScheduler = new Scheduler("copy_" + name);
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

        Scheduler reserveSpaceScheduler = new Scheduler("reserve_space" + name);
        reserveSpaceScheduler.start();
        schedulerMap.put(ReserveSpaceRequest.class,copyRequestScheduler);

        this.name = name;
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

    public Scheduler getScheduler(Class jobType) {
        Scheduler scheduler = schedulerMap.get(jobType);
        if(scheduler != null) return scheduler;
         throw new UnsupportedOperationException(
                 "Scheduler for class "+jobType+ " is not supported");
    }


}
