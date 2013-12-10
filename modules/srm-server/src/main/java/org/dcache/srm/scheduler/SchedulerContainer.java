package org.dcache.srm.scheduler;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import org.dcache.srm.request.Job;


/**
 * The SchedulerContainer class groups together different Scheduler objects
 * and allows operations that can (potentially) span more than one scheduler.
 */
public class SchedulerContainer
{
    private ImmutableMap<Class<? extends Job>, Scheduler> schedulerMap =
            ImmutableMap.of();

    public void add(Scheduler scheduler)
    {
        schedulerMap = ImmutableMap.<Class<? extends Job>, Scheduler> builder()
                .putAll(schedulerMap)
                .put(scheduler.getType(), scheduler)
                .build();
    }

    public void addAll(Collection<Scheduler> schedulers)
    {
        ImmutableMap.Builder<Class<? extends Job>, Scheduler> builder =
                ImmutableMap.<Class<? extends Job>, Scheduler> builder();
        builder.putAll(schedulerMap);
        for (Scheduler scheduler : schedulers) {
            builder.put(scheduler.getType(), scheduler);
        }
        schedulerMap = builder.build();
    }

    public void start()
    {
        for (Scheduler scheduler : schedulerMap.values()) {
            scheduler.start();
        }
    }

    public void stop()
    {
        for (Scheduler scheduler : schedulerMap.values()) {
            scheduler.stop();
        }
    }

    public double getLoad(Class<? extends Job> type)
    {
        Scheduler scheduler = getScheduler(type);
        return (double)scheduler.getTotalRunningThreads() /
                scheduler.getThreadPoolSize();
    }

    public void setMaxReadyJobs(Class<? extends Job> type, int value)
    {
        Scheduler scheduler = getScheduler(type);
        scheduler.setMaxReadyJobs(value);
    }

    public void schedule(Job job) throws InterruptedException, IllegalStateException, IllegalStateTransition
    {
        Scheduler scheduler = getScheduler(job.getSchedulerType());
        job.scheduleWith(scheduler);
    }

    private Scheduler getScheduler(Class<? extends Job> type)
    {
        return getScheduler(null, type);
    }

    public void schedule(Iterable<? extends Job> jobs) throws InterruptedException, IllegalStateTransition
    {
        Scheduler scheduler = null;

        for (Job job : jobs) {
            scheduler = getScheduler(scheduler, job.getSchedulerType());
            job.scheduleWith(scheduler);
        }
    }

    private Scheduler getScheduler(Scheduler suggestion, Class<? extends Job> type)
    {
        if (suggestion == null || !suggestion.getType().equals(type)) {
            suggestion = schedulerMap.get(type);
        }

        if (suggestion == null) {
            throw new UnsupportedOperationException("Scheduler for " + type +
                    " is not supported");
        }

        return suggestion;
    }

    public CharSequence getInfo()
    {
        StringBuilder sb = new StringBuilder();
        for (Scheduler scheduler : schedulerMap.values()) {
            scheduler.getInfo(sb);
        }
        return sb;
    }

    public CharSequence getDetailedInfo(Class<? extends Job> type)
    {
        Scheduler scheduler = getScheduler(type);

        StringBuilder sb = new StringBuilder();

        scheduler.printThreadQueue(sb);
        scheduler.printPriorityThreadQueue(sb);
        scheduler.printReadyQueue(sb);

        return sb;
    }
}
