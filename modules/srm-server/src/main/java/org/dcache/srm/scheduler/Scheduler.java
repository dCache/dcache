/*
COPYRIGHT STATUS:
  Dec 1st 2001, Fermi National Accelerator Laboratory (FNAL) documents and
  software are sponsored by the U.S. Department of Energy under Contract No.
  DE-AC02-76CH03000. Therefore, the U.S. Government retains a  world-wide
  non-exclusive, royalty-free license to publish or reproduce these documents
  and software for U.S. Government purposes.  All documents and software
  available from this server are protected under the U.S. and Foreign
  Copyright Laws, and FNAL reserves all rights.


 Distribution of the software available from this server is free of
 charge subject to the user following the terms of the Fermitools
 Software Legal Information.

 Redistribution and/or modification of the software shall be accompanied
 by the Fermitools Software Legal Information  (including the copyright
 notice).

 The user is asked to feed back problems, benefits, and/or suggestions
 about the software to the Fermilab Software Providers.


 Neither the name of Fermilab, the  URA, nor the names of the contributors
 may be used to endorse or promote products derived from this software
 without specific prior written permission.



  DISCLAIMER OF LIABILITY (BSD):

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED  WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED  WARRANTIES OF MERCHANTABILITY AND FITNESS
  FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL FERMILAB,
  OR THE URA, OR THE U.S. DEPARTMENT of ENERGY, OR CONTRIBUTORS BE LIABLE
  FOR  ANY  DIRECT, INDIRECT,  INCIDENTAL, SPECIAL, EXEMPLARY, OR
  CONSEQUENTIAL DAMAGES  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT
  OF SUBSTITUTE  GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
  BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY  OF
  LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
  NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT  OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE  POSSIBILITY OF SUCH DAMAGE.


  Liabilities of the Government:

  This software is provided by URA, independent from its Prime Contract
  with the U.S. Department of Energy. URA is acting independently from
  the Government and in its own private capacity and is not acting on
  behalf of the U.S. Government, nor as its contractor nor its agent.
  Correspondingly, it is understood and agreed that the U.S. Government
  has no connection to this software and in no manner whatsoever shall
  be liable for nor assume any responsibility or obligation for any claim,
  cost, or damages arising out of or resulting from the use of the software
  available from this server.


  Export Control:

  All documents and software available from this server are subject to U.S.
  export control laws.  Anyone downloading information from this server is
  obligated to secure any necessary Government licenses before exporting
  documents or software obtained from this server.
 */

package org.dcache.srm.scheduler;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import java.util.Collection;
import java.util.Formatter;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.JobStateChangeAware;
import org.dcache.srm.scheduler.spi.SchedulingStrategy;
import org.dcache.srm.scheduler.spi.SchedulingStrategyProvider;
import org.dcache.srm.scheduler.spi.TransferStrategy;
import org.dcache.srm.scheduler.spi.TransferStrategyProvider;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.base.Strings.repeat;

public class Scheduler <T extends Job> implements JobStateChangeAware
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Scheduler.class);
    private final Class<T> type;

    private int maxRequests;

    // thread pool related variables
    private final ExecutorService pooledExecutor;

    private final ScheduledExecutorService scheduler
            = Executors.newSingleThreadScheduledExecutor();

    // ready state related variables
    private int maxReadyJobs;

    // async wait state related variables
    private int maxInProgress;

    private final String id;
    private volatile boolean running;

    //
    // this timer is used for tracking the expiration of retry timeout
    private final Timer retryTimer;

    // this will contain the number of
    private final long timeStamp = System.currentTimeMillis();
    private long queuesUpdateMaxWait = 60 * 1000;

    private static volatile Map<String, Scheduler<?>> schedulers = ImmutableMap.of();

    private final WorkSupplyService workSupplyService;
    private final CopyOnWriteArrayList<StateChangeListener> listeners = new CopyOnWriteArrayList<>();

    private SchedulingStrategy schedulingStrategy;
    private TransferStrategy transferStrategy;
    private String schedulingStrategyName;
    private String transferStrategyName;

    private Multimap<State,Long> jobs = MultimapBuilder.enumKeys(State.class).hashSetValues().build();

    public static Scheduler<?> getScheduler(String id)
    {
        return schedulers.get(id);
    }

    public static synchronized void addScheduler(String id, Scheduler<?> scheduler)
    {
        schedulers = ImmutableMap.<String, Scheduler<?>>builder()
                .putAll(schedulers)
                .put(id, scheduler)
                .build();
    }

    public Scheduler(String id, Class<T> type)
    {
        this.type = type;
        this.id = checkNotNull(id);
        checkArgument(!id.isEmpty(), "need non-empty string as an id");

        workSupplyService = new WorkSupplyService();
        retryTimer = new Timer();
        pooledExecutor = Executors.newCachedThreadPool();

        addScheduler(id, this);
    }

    @Required
    public void setSchedulingStrategyProvider(SchedulingStrategyProvider provider)
    {
        schedulingStrategyName = provider.getName();
        schedulingStrategy = provider.createStrategy(this);
    }

    @Required
    public void setTransferStrategyProvider(TransferStrategyProvider provider)
    {
        transferStrategyName = provider.getName();
        transferStrategy = provider.createStrategy(this);
    }

    public void start() throws IllegalStateException
    {
        synchronized (this) {
            checkState(!running, "Scheduler is running.");
            running = true;
        }
        workSupplyService.startAsync().awaitRunning();
    }

    public void stop()
    {
        synchronized (this) {
            checkState(running, "Scheduler is not running.");
            running = false;
        }

        workSupplyService.stopAsync().awaitTerminated();
        retryTimer.cancel();
        scheduler.shutdownNow();
        pooledExecutor.shutdownNow();
    }

    /**
     * Places a newly created job in the queue.
     *
     * The job is moved to the QUEUED state. The job will be moved to the INPROGRESS state
     * in accordance with the scheduling strategy of this scheduler.
     *
     * This action is subject to request limits and may cause the job to be failed
     * immediately if the limits are exceeded.
     */
    public void queue(Job job) throws IllegalStateTransition
    {
        checkState(running, "Scheduler is not running");
        checkOwnership(job);
        LOGGER.trace("queue is called for job with id={} in state={}", job.getId(), job.getState());

        acceptJob(job, "Queued.");
    }


    /**
     * Accept a job after restarting the SRM.
     *
     * The job is moved to the QUEUED state. The job will be moved to the INPROGRESS state
     * in accordance with the scheduling strategy of this scheduler.
     *
     * This action is subject to request limits and may cause the job to be failed
     * immediately if the limits are exceeded.
     * @param job the job loaded from the job storage.
     */
    public void inherit(Job job) throws IllegalStateTransition
    {
        State state = job.getState();

        switch (state) {
        // Unscheduled or queued jobs were never worked on before the SRM restart; we
        // simply queue them now.
        case UNSCHEDULED:
        case QUEUED:
        case INPROGRESS:
            acceptJob(job, "Restored from database.");
            break;

        // Jobs in RQUEUED, READY or TRANSFERRING states require no further
        // processing. We can leave them for the client to discover the TURL
        // or place the job into the DONE state, respectively.
        case RQUEUED:
        case READY:
        case TRANSFERRING:
            // nothing to do, but keep track of state changes.
            job.subscribe(this);
            break;

        default:
            throw new RuntimeException("Unexpected state when restoring on startup: " + state);
        }
    }

    private void acceptJob(Job job, String why) throws IllegalStateTransition
    {
        job.subscribe(this);

        job.wlock();
        try {
            checkState(!job.getState().isFinal(), "Cannot accept job in state %s ",
                    job.getState());

            if (!threadQueue(job)) {
                LOGGER.warn("Maximum request limit reached.");
                job.setState(State.FAILED, "Site busy: Too many queued requests.");
                return;
            }

            job.setState(State.QUEUED, why);
        } finally {
            job.wunlock();
        }
    }

    /**
     * Requests the job's run method to be called from this scheduler's thread pool.
     */
    public void execute(Job job)
    {
        checkState(running, "Scheduler is not running");
        checkOwnership(job);
        LOGGER.trace("execute is called for job with id={} in state={}", job.getId(), job.getState());
        executeJob(job);
    }

    /**
     * Schedule running this job after some delay.
     */
    public void schedule(Job job, long delay, TimeUnit units)
    {
        checkState(running, "Scheduler is not running");
        checkOwnership(job);
        LOGGER.trace("schedule called for job with id={} in state={}", job.getId(), job.getState());
        scheduler.schedule(() -> executeJob(job), delay, units);
    }

    private void executeJob(Job job)
    {
        pooledExecutor.execute(new JobWrapper(job));
    }

    public synchronized int getTotalQueued()
    {
        return jobs.get(State.QUEUED).size();
    }

    private int getTotalInprogress()
    {
        return jobs.get(State.INPROGRESS).size();
    }

    public synchronized int getTotalRQueued()
    {
        return jobs.get(State.RQUEUED).size();
    }

    public synchronized int getTotalReady()
    {
        return jobs.get(State.READY).size();
    }

    public void tryToReadyJob(Job job)
    {
        if (transferStrategy.canTransfer(job)) {
            try {
                job.setState(State.READY, "Execution succeeded.");
            } catch (IllegalStateTransition ist) {
                //nothing more we can do here
                LOGGER.error("Illegal State Transition : {}", ist.getMessage());
            }
        }
    }

    private boolean threadQueue(Job job)
    {
        if (getTotalRequests() < getMaxRequests()) {
            schedulingStrategy.add(job);
            workSupplyService.distributeWork();
            return true;
        }
        return false;
    }

    private synchronized int getTotalRequests()
    {
        return jobs.size();
    }

    public double getLoad()
    {
        return (getTotalQueued() + getTotalInprogress()) / (double) getMaxInProgress();
    }

    public long getTimestamp()
    {
        return timeStamp;
    }

    /**
     * This class is responsible for keeping the PoolExecutors busy with jobs
     * taken from the priority- and non-priority queues.
     */
    private class WorkSupplyService extends AbstractExecutionThreadService
    {
        private boolean hasBeenNotified;

        @Override
        public void run()
        {
            try {
                while (isRunning()) {
                    try {
                        updateThreadQueue();

                        synchronized (this) {
                            if (!hasBeenNotified) {
                                // logger.debug("Scheduler(id={}"getId()+").run() waiting for events...");
                                wait(queuesUpdateMaxWait);
                            }
                            hasBeenNotified = false;
                        }
                    } catch (SRMInvalidRequestException e) {
                        LOGGER.error("Sheduler(id={}) detected an SRM error: {}", getId(), e.toString());
                    } catch (RuntimeException e) {
                        LOGGER.error("Sheduler(id=" + getId() + ") detected a bug", e);
                    }
                }
            } catch (InterruptedException e) {
                LOGGER.error("Sheduler(id=" + getId() +
                        ") terminating update thread, since it caught an InterruptedException",
                        e);
            }
            //we are interrupted,
            //terminating thread
        }

        @Override
        public void triggerShutdown()
        {
            distributeWork();
        }

        public synchronized void distributeWork()
        {
            hasBeenNotified = true;
            notify();
        }

        @Override
        public String serviceName()
        {
            return "Scheduler-" + id;
        }

        private void updateThreadQueue()
                throws SRMInvalidRequestException, InterruptedException
        {
            Long id;
            while (getTotalInprogress() < getMaxInProgress() && (id = schedulingStrategy.remove()) != null) {
                Job job = Job.getJob(id, type);
                job.wlock();
                try {
                    if (job.getState() == org.dcache.srm.scheduler.State.QUEUED) {
                        try {
                            job.setState(org.dcache.srm.scheduler.State.INPROGRESS, "In progress.");
                            execute(job);
                        } catch (IllegalStateTransition e) {
                            LOGGER.error("Bug detected.", e);
                            try {
                                job.setState(org.dcache.srm.scheduler.State.FAILED, e.getMessage());
                            } catch (IllegalStateTransition ignored) {
                            }
                        }
                    }
                } finally {
                    job.wunlock();
                }
            }
        }
    }

    private class JobWrapper implements Runnable
    {
        private final Job job;

        public JobWrapper(Job job)
        {
            this.job = job;
        }

        @Override
        public void run()
        {
            try (JDC ignored = job.applyJdc()) {
                try {
                    try {
                        try {
                            job.run();
                        } catch (SRMAuthorizationException e) {
                            LOGGER.warn(e.toString());
                            throw e;
                        } catch (DataAccessException e) {
                            LOGGER.error(e.toString());
                            throw new SRMInternalErrorException("Database access error.", e);
                        }
                    } catch (SRMException e) {
                        job.wlock();
                        try {
                            if (!job.getState().isFinal()) {
                                job.setStateAndStatusCode(State.FAILED, e.getMessage(), e.getStatusCode());
                            }
                        } finally {
                            job.wunlock();
                        }
                    } catch (RuntimeException | IllegalStateTransition e) {
                        LOGGER.error("Bug detected by SRM Scheduler", e);
                        job.wlock();
                        try {
                            if (!job.getState().isFinal()) {
                                job.setStateAndStatusCode(State.FAILED, "Internal error: " + e.toString(), TStatusCode.SRM_INTERNAL_ERROR);
                            }
                        } finally {
                            job.wunlock();
                        }
                    }
                } catch (IllegalStateTransition e) {
                    LOGGER.error("Bug detected by SRM Scheduler", e);
                } catch (Throwable t) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                }
            } finally {
                workSupplyService.distributeWork();
            }
        }
    }

    public void addStateChangeListener(StateChangeListener listener)
    {
        listeners.add(listener);
    }

    public void removeStateChangeListener(StateChangeListener listener)
    {
        listeners.remove(listener);
    }

    @Override
    public void jobStateChanged(Job job, State oldState, String description)
    {
        State newState = job.getState();

        synchronized (this) {
            jobs.remove(oldState, job.getId());
            if (!newState.isFinal()) {
                jobs.put(newState, job.getId());
            }
        }

        LOGGER.debug("state changed for job id {} from {} to {}", job.getId(), oldState, newState);

        for (StateChangeListener listener : listeners) {
            listener.stateChanged(job, oldState, newState);
        }
    }

    /**
     * Getter for property id.
     *
     * @return Value of property id.
     */
    public String getId()
    {
        return id;
    }

    /**
     * Getter for property maxReadyJobs.
     *
     * @return Value of property maxReadyJobs.
     */
    public synchronized int getMaxReadyJobs()
    {
        return maxReadyJobs;
    }

    /**
     * Setter for property maxReadyJobs.
     *
     * @param maxReadyJobs New value of property maxReadyJobs.
     */
    public synchronized void setMaxReadyJobs(int maxReadyJobs)
    {
        this.maxReadyJobs = maxReadyJobs;
    }

    /**
     * Getter for property maxThreadQueueSize.
     *
     * @return Value of property maxThreadQueueSize.
     */
    public synchronized int getMaxRequests()
    {
        return maxRequests;
    }

    public synchronized void setMaxRequests(int maxRequests)
    {
        this.maxRequests = maxRequests;
    }

    public synchronized int getMaxInProgress()
    {
        return maxInProgress;
    }

    public synchronized void setMaxInprogress(int maxAsyncWaitJobs)
    {
        this.maxInProgress = maxAsyncWaitJobs;
    }

    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        getInfo(sb);
        return sb.toString();
    }

    /**
     * Helper class for formatting the info output.
     */
    private static class InfoFormatter
    {
        private final Formatter formatter;
        private final int fieldWidth;
        private final int baseWidth;
        private final String field2;
        private final String field2NoState;

        public InfoFormatter(Appendable appendable, int fieldWidth, int width1, int width2)
        {
            this.formatter = new Formatter(appendable);
            this.fieldWidth = fieldWidth;
            this.baseWidth = Integer.max(width1, width2 - fieldWidth - 4) + 1;

            this.field2 = String.format("    %%-%ds %%%dd     [%%s]\n", baseWidth + fieldWidth + 4, fieldWidth);
            this.field2NoState = String.format("    %%-%ds %%%dd\n", baseWidth + fieldWidth + 4, fieldWidth);
        }

        public void field(String description, int count, State state)
        {
            format(field2, padEnd(description + " ", baseWidth + fieldWidth + 4, '.'), count, state);
        }

        public void field(String description, int count)
        {
            format(field2NoState, padEnd(description + " ", baseWidth + fieldWidth + 4, '.'), count);
        }

        public void line()
        {
            format("    %s\n", repeat("-", baseWidth + 2 * fieldWidth + 6));
        }

        public Formatter format(String format, Object... args)
        {
            return formatter.format(format, args);
        }
    }

    public synchronized void getInfo(Appendable appendable)
    {
        int fieldWidth = Math.max(3, String.valueOf(getMaxRequests()).length());
        InfoFormatter formatter =
                new InfoFormatter(appendable, fieldWidth,
                                  Integer.max(24, 20 + fieldWidth),
                                  28 + fieldWidth);
        formatter.field("Queued", getTotalQueued(), State.QUEUED);
        formatter.field("In progress (max " + getMaxInProgress() + ")", getTotalInprogress(), State.INPROGRESS);
        if (getTotalRQueued() + getMaxReadyJobs() + getTotalReady() > 0) {
            formatter.field("Queued for transfer", getTotalRQueued(), State.RQUEUED);
            formatter.field("Waiting for transfer (max " + getMaxReadyJobs() + ")", getTotalReady(), State.READY);
        }
        formatter.line();
        formatter.field("Total requests (max " + getMaxRequests() + ")", getTotalRequests());
        formatter.format("\n");
        formatter.format("    Scheduling strategy             : %s\n", schedulingStrategyName);
        formatter.format("    Transfer strategy               : %s\n", transferStrategyName);
        formatter.format("    Scheduler ID                    : %s\n", id);
    }

    private static void printQueue(StringBuilder sb, Collection<Long> queue)
    {
        if (queue.isEmpty()) {
            sb.append("Queue is empty\n");
        } else {
            int index = 0;
            for (long nextId : queue) {
                sb.append("queue element # ").append(index).append(" : ").append(nextId).append('\n');
                index++;
            }
        }
    }

    public synchronized void printThreadQueue(StringBuilder sb)
    {
        sb.append("ThreadQueue :\n");
        printQueue(sb, jobs.get(State.QUEUED));
    }

    public synchronized void printReadyQueue(StringBuilder sb)
    {
        sb.append("ReadyQueue :\n");
        printQueue(sb, jobs.get(State.RQUEUED));
    }

    public Class<T> getType()
    {
        return type;
    }

    private void checkOwnership(Job job)
    {
        if (!getType().isInstance(job)) {
            throw new IllegalArgumentException("Scheduler " + getId() + " doesn't accept " + job.getClass() + '.');
        }
        if (!id.equals(job.getSchedulerId()) || timeStamp != job.getSchedulerTimeStamp()) {
            throw new IllegalArgumentException("Job " + job.getId() + " doesn't belong to scheduler " + getId() + '.');
        }
    }
}


