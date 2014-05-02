package org.dcache.srm.scheduler;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map.Entry;

import org.dcache.srm.request.Job;


/**
 * The SchedulerContainer class groups together different Scheduler objects
 * and allows operations that can (potentially) span more than one scheduler.
 */
public class SchedulerContainer
{
    private ImmutableMap<Class<? extends Job>, Scheduler<?>> schedulers;

    public SchedulerContainer()
    {
        schedulers = ImmutableMap.of();
    }

    public SchedulerContainer(Scheduler<?>... schedulers)
    {
        ImmutableMap.Builder<Class<? extends Job>, Scheduler<?>> builder =
                ImmutableMap.builder();
        for (Scheduler<?> scheduler : schedulers) {
            builder.put(scheduler.getType(), scheduler);
        }
        this.schedulers = builder.build();
    }

    public void setSchedulers(Collection<Scheduler<?>> schedulers)
    {
        ImmutableMap.Builder<Class<? extends Job>, Scheduler<?>> builder =
                ImmutableMap.builder();
        for (Scheduler<?> scheduler : schedulers) {
            builder.put(scheduler.getType(), scheduler);
        }
        this.schedulers = builder.build();
    }

    public double getLoad(Class<? extends Job> type)
    {
        return getScheduler(type).getLoad();
    }

    public void setMaxReadyJobs(Class<? extends Job> type, int value)
    {
        Scheduler<?> scheduler = getScheduler(type);
        scheduler.setMaxReadyJobs(value);
    }

    public void schedule(Job job) throws InterruptedException, IllegalStateException, IllegalStateTransition
    {
        Scheduler<?> scheduler = getScheduler(job.getSchedulerType());
        job.scheduleWith(scheduler);
    }

    private Scheduler<?> getScheduler(Class<? extends Job> type)
    {
        return getScheduler(null, type);
    }

    private Scheduler<?> getScheduler(Scheduler<?> suggestion, Class<? extends Job> type)
            throws UnsupportedOperationException
    {
        if (suggestion == null || !suggestion.getType().isAssignableFrom(type)) {
            for (Entry<Class<? extends Job>, Scheduler<?>> entry : schedulers.entrySet()) {
                if (entry.getKey().isAssignableFrom(type)) {
                    suggestion = entry.getValue();
                    break;
                }
            }
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
        for (Scheduler<?> scheduler : schedulers.values()) {
            scheduler.getInfo(sb);
        }
        return sb;
    }

    public CharSequence getDetailedInfo(Class<? extends Job> type)
    {
        Scheduler<?> scheduler = getScheduler(type);

        StringBuilder sb = new StringBuilder();

        scheduler.printThreadQueue(sb);
        scheduler.printReadyQueue(sb);

        return sb;
    }

    public void restoreJobsOnSrmStart(Iterable<? extends Job> activeJobs)
    {
        Scheduler<?> scheduler = null;

        for (Job job : activeJobs) {
            scheduler = getScheduler(scheduler, job.getSchedulerType());
            if (scheduler.getId().equals(job.getSchedulerId())) {
                job.onSrmRestart(scheduler);
            } // else another SRM instance is handling this job
        }
    }
}
