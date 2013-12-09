package org.dcache.srm.scheduler;

import org.dcache.srm.request.BringOnlineFileRequest;
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
public class SchedulerFactory
{
    private final Configuration config;
    private final String name;

    public SchedulerFactory(Configuration config, String name)
    {
        this.config = config;
        this.name = name;
    }

    public Scheduler buildLsScheduler()
    {
        Scheduler scheduler = new Scheduler("ls_" + name, LsFileRequest.class);

        scheduler.setMaxThreadQueueSize(config.getLsReqTQueueSize());
        scheduler.setThreadPoolSize(config.getLsThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getLsMaxWaitingRequests());
        scheduler.setMaxReadyQueueSize(config.getLsReadyQueueSize());
        scheduler.setMaxReadyJobs(config.getLsMaxReadyJobs());
        scheduler.setMaxNumberOfRetries(config.getLsMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getLsRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getLsMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getLsPriorityPolicyPlugin());

        return scheduler;
    }


    public Scheduler buildGetScheduler()
    {
        Scheduler scheduler = new Scheduler("get_" + name, GetFileRequest.class);

        scheduler.setMaxThreadQueueSize(config.getGetReqTQueueSize());
        scheduler.setThreadPoolSize(config.getGetThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getGetMaxWaitingRequests());
        scheduler.setMaxReadyQueueSize(config.getGetReadyQueueSize());
        scheduler.setMaxReadyJobs(config.getGetMaxReadyJobs());
        scheduler.setMaxNumberOfRetries(config.getGetMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getGetRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getGetMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getGetPriorityPolicyPlugin());

        return scheduler;
    }

    public Scheduler buildBringOnlineScheduler()
    {
        Scheduler scheduler = new Scheduler("bring_online_" + name, BringOnlineFileRequest.class);

        scheduler.setMaxThreadQueueSize(config.getBringOnlineReqTQueueSize());
        scheduler.setThreadPoolSize(config.getBringOnlineThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getBringOnlineMaxWaitingRequests());
        scheduler.setMaxReadyQueueSize(config.getBringOnlineReadyQueueSize());
        scheduler.setMaxReadyJobs(config.getBringOnlineMaxReadyJobs());
        scheduler.setMaxNumberOfRetries(config.getBringOnlineMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getBringOnlineRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getBringOnlineMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getBringOnlinePriorityPolicyPlugin());

        return scheduler;
    }

    public Scheduler buildPutScheduler()
    {
        Scheduler scheduler = new Scheduler("put_" + name, PutFileRequest.class);

        scheduler.setMaxThreadQueueSize(config.getPutReqTQueueSize());
        scheduler.setThreadPoolSize(config.getPutThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getPutMaxWaitingRequests());
        scheduler.setMaxReadyQueueSize(config.getPutReadyQueueSize());
        scheduler.setMaxReadyJobs(config.getPutMaxReadyJobs());
        scheduler.setMaxNumberOfRetries(config.getPutMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getPutRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getPutMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getPutPriorityPolicyPlugin());

        return scheduler;
    }

    public Scheduler buildCopyScheduler()
    {
        Scheduler scheduler = new Scheduler("copy_" + name, Job.class);
        // scheduler parameters
        scheduler.setMaxThreadQueueSize(config.getCopyReqTQueueSize());
        scheduler.setThreadPoolSize(config.getCopyThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getCopyMaxWaitingRequests());
        scheduler.setMaxNumberOfRetries(config.getCopyMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getCopyRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getCopyMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getCopyPriorityPolicyPlugin());

        return scheduler;
    }

    public Scheduler buildReserveSpaceScheduler()
    {
        Scheduler scheduler = new Scheduler("reserve_space_" + name, ReserveSpaceRequest.class);

        scheduler.setMaxThreadQueueSize(config.getReserveSpaceReqTQueueSize());
        scheduler.setThreadPoolSize(config.getReserveSpaceThreadPoolSize());
        scheduler.setMaxWaitingJobNum(config.getReserveSpaceMaxWaitingRequests());
        scheduler.setMaxReadyQueueSize(config.getReserveSpaceReadyQueueSize());
        scheduler.setMaxReadyJobs(config.getReserveSpaceMaxReadyJobs());
        scheduler.setMaxNumberOfRetries(config.getReserveSpaceMaxNumOfRetries());
        scheduler.setRetryTimeout(config.getReserveSpaceRetryTimeout());
        scheduler.setMaxRunningByOwner(config.getReserveSpaceMaxRunningBySameOwner());
        scheduler.setPriorityPolicyPlugin(config.getReserveSpacePriorityPolicyPlugin());

        return scheduler;
    }
}
