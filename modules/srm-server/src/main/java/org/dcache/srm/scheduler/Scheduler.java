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
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.dao.DataAccessException;

import java.util.Collection;
import java.util.Formatter;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.spi.TransferStrategy;
import org.dcache.srm.scheduler.spi.TransferStrategyProvider;
import org.dcache.srm.scheduler.spi.SchedulingStrategy;
import org.dcache.srm.scheduler.spi.SchedulingStrategyProvider;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.*;
import static com.google.common.base.Strings.*;

public class Scheduler <T extends Job>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Scheduler.class);
    private final Class<T> type;

    private int maxRequests;

    // thread pool related variables
    private final ThreadPoolExecutor pooledExecutor;

    // ready state related variables
    private int maxReadyJobs;

    // async wait state related variables
    private int maxInProgress;

    // retry wait state related variables
    private int maxNumberOfRetries;
    private long retryTimeout;

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
        pooledExecutor = new ThreadPoolExecutor(30, 30, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());

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
        pooledExecutor.shutdownNow();
    }


    public void schedule(Job job)
            throws IllegalStateException, IllegalArgumentException,
                   IllegalStateTransition
    {
        checkState(running, "scheduler is not running");
        checkOwnership(job);
        LOGGER.trace("schedule is called for job with id={} in state={}", job.getId(), job.getState());

        job.wlock();
        try {
            switch (job.getState()) {
            case PENDING:
            case RETRYWAIT:
                if (threadQueue(job)) {
                    job.setState(State.TQUEUED, "Request enqueued.");
                } else {
                    LOGGER.warn("Maximum request limit reached.");
                    job.setState(State.FAILED, "Site busy: Too many queued requests.");
                }
                break;
            case RESTORED:
            case ASYNCWAIT:
            case RUNNINGWITHOUTTHREAD:
            case TQUEUED:
                LOGGER.trace("putting job in a thread queue, job#{}", job.getId());
                job.setState(State.PRIORITYTQUEUED, "Waiting for thread.");
                try {
                    pooledExecutor.execute(new JobWrapper(job));
                } catch (RejectedExecutionException e) {
                    job.setState(State.FAILED, "Site busy: Too many queued requests.");
                }
                break;
            default:
                throw new IllegalStateException("cannot schedule job in state =" + job.getState());
            }
        } finally {
            job.wunlock();
        }
    }

    public synchronized int getTotalRunningState()
    {
        return jobs.get(State.RUNNING).size();
    }

    public synchronized int getTotalRunningWithoutThreadState()
    {
        return jobs.get(State.RUNNINGWITHOUTTHREAD).size();
    }

    public synchronized int getTotalTQueued()
    {
        return jobs.get(State.TQUEUED).size();
    }

    public synchronized int getTotalPriorityTQueued()
    {
        return jobs.get(State.PRIORITYTQUEUED).size();
    }

    public synchronized int getTotalRQueued()
    {
        return jobs.get(State.RQUEUED).size();
    }

    public synchronized int getTotalReady()
    {
        return jobs.get(State.READY).size();
    }

    public synchronized int getTotalAsyncWait()
    {
        return jobs.get(State.ASYNCWAIT).size();
    }

    public synchronized int getTotalRetryWait()
    {
        return jobs.get(State.RETRYWAIT).size();
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

    private int getTotalInprogress()
    {
        return getTotalAsyncWait() + getTotalPriorityTQueued() + getTotalRunningState() + getTotalRunningWithoutThreadState();
    }

    public double getLoad()
    {
        return (getTotalTQueued() + getTotalInprogress()) / (double) getMaxInProgress();
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
                                //logger.debug("Scheduler(id="+getId()+").run() waiting for events...");
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
                    if (job.getState() == org.dcache.srm.scheduler.State.TQUEUED) {
                        try {
                            schedule(job);
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
                    LOGGER.trace("Scheduler(id={}) entering sync(job) block", getId());
                    job.wlock();
                    try {
                        LOGGER.trace("Scheduler(id={}) entered sync(job) block", getId());
                        State state = job.getState();
                        LOGGER.trace("Scheduler(id={}) JobWrapper run() running job in state={}", getId(), state);

                        switch (state) {
                        case CANCELED:
                        case FAILED:
                            LOGGER.trace("Scheduler(id={}) returning", getId());
                            return;
                        case PENDING:
                        case TQUEUED:
                        case PRIORITYTQUEUED:
                        case ASYNCWAIT:
                        case RETRYWAIT:
                            try {
                                LOGGER.debug("Scheduler(id={}) changing job state to running", getId());
                                job.setState(State.RUNNING, "Processing request");
                            } catch (IllegalStateTransition ist) {
                                LOGGER.error("Illegal State Transition : " + ist.getMessage());
                                return;
                            }
                            break;
                        default:
                            LOGGER.error("Scheduler(id={}) job is in state {}; can not execute, returning", getId(), state);
                            return;
                        }
                    } finally {
                        job.wunlock();
                    }

                    LOGGER.trace("Scheduler(id={}) exited sync block", getId());
                    try {
                        LOGGER.trace("Scheduler(id={}) calling job.run()", getId());
                        try {
                            job.run();
                        } catch (SRMAuthorizationException e) {
                            LOGGER.warn(e.toString());
                            throw e;
                        } catch (DataAccessException e) {
                            LOGGER.error(e.toString());
                            throw new SRMInternalErrorException("Database access error.", e);
                        }
                        LOGGER.trace("Scheduler(id={}) job.run() returned", getId());
                    } catch (SRMInternalErrorException e) {
                        job.wlock();
                        try {
                            if (!job.getState().isFinal()) {
                                if (job.getNumberOfRetries() < getMaxNumberOfRetries()) {
                                    job.setState(State.RETRYWAIT, e.getMessage());
                                    startRetryTimer(job);
                                } else {
                                    job.setStateAndStatusCode(State.FAILED, e.getMessage(), e.getStatusCode());
                                }
                            }
                        } finally {
                            job.wunlock();
                        }
                        return;
                    } catch (SRMException e) {
                        job.wlock();
                        try {
                            if (!job.getState().isFinal()) {
                                job.setStateAndStatusCode(State.FAILED, e.getMessage(), e.getStatusCode());
                            }
                        } finally {
                            job.wunlock();
                        }
                        return;
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
                        return;
                    }

                    job.wlock();
                    try {
                        if (job.getState() == State.RUNNING) {
                            job.setState(State.RQUEUED, "Putting on a \"Ready\" Queue.");
                        }
                    } finally {
                        job.wunlock();
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

    private void startRetryTimer(final Job job)
    {
        TimerTask task = new TimerTask()
        {
            @Override
            public void run()
            {
                job.wlock();
                try {
                    if (job.getState() == State.RETRYWAIT) {
                        schedule(job);
                    }
                } catch (IllegalStateTransition e) {
                    LOGGER.error("Bug detected.", e);
                } finally {
                    job.wunlock();
                }

            }
        };
        job.setRetryTimer(task);
        retryTimer.schedule(task, retryTimeout);
    }

    public void addStateChangeListener(StateChangeListener listener)
    {
        listeners.add(listener);
    }

    public void removeStateChangeListener(StateChangeListener listener)
    {
        listeners.remove(listener);
    }

    public void stateChanged(Job job, State oldState, State newState)
    {
        checkNotNull(job);

        synchronized (this) {
            jobs.remove(oldState, job.getId());
            if (!newState.isFinal()) {
                jobs.put(newState, job.getId());
            }
        }

        LOGGER.debug("state changed for job id {} from {} to {}", job.getId(), oldState, newState);
        if (oldState == State.RETRYWAIT && newState.isFinal()) {
            job.cancelRetryTimer();
        }

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
     * Getter for property threadPoolSize.
     *
     * @return Value of property threadPoolSize.
     */
    public synchronized int getThreadPoolSize()
    {
        return pooledExecutor.getMaximumPoolSize();
    }

    /**
     * Setter for property threadPoolSize.
     *
     * @param threadPoolSize New value of property threadPoolSize.
     */
    public synchronized void setThreadPoolSize(int threadPoolSize)
    {
        pooledExecutor.setCorePoolSize(threadPoolSize);
        pooledExecutor.setMaximumPoolSize(threadPoolSize);
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

    public synchronized int getMaxNumberOfRetries()
    {
        return maxNumberOfRetries;
    }

    public synchronized void setMaxNumberOfRetries(int maxNumberOfRetries)
    {
        this.maxNumberOfRetries = maxNumberOfRetries;
    }

    public synchronized long getRetryTimeout()
    {
        return retryTimeout;
    }

    public synchronized void setRetryTimeout(long retryTimeout)
    {
        this.retryTimeout = retryTimeout;
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
        private final String field1;
        private final String field2;
        private final String field2NoState;

        public InfoFormatter(Appendable appendable, int fieldWidth, int width1, int width2)
        {
            this.formatter = new Formatter(appendable);
            this.fieldWidth = fieldWidth;
            this.baseWidth = Ints.max(width1, width2 - fieldWidth - 4) + 1;

            this.field1 = String.format("    %%-%ds %%%dd%s     [%%s]\n", baseWidth, fieldWidth, repeat(" ", 4 + fieldWidth));
            this.field2 = String.format("    %%-%ds %%%dd     [%%s]\n", baseWidth + fieldWidth + 4, fieldWidth);
            this.field2NoState = String.format("    %%-%ds %%%dd\n", baseWidth + fieldWidth + 4, fieldWidth);
        }

        public void column1(String description, int count, State state)
        {
            format(field1, padEnd(description + " ", baseWidth, '.'), count, state);
        }

        public void column2(String description, int count, State state)
        {
            format(field2, padEnd(description + " ", baseWidth + fieldWidth + 4, '.'), count, state);
        }

        public void column2(String description, int count)
        {
            format(field2NoState, padEnd(description + " ", baseWidth + fieldWidth + 4, '.'), count);
        }

        public void sum(String description, int count)
        {
            format(field2NoState, padEnd(description + " ", baseWidth, '.') + padStart("SUM >>", fieldWidth + 4, ' '), count);
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
                                  Ints.max(24, 20 + fieldWidth),
                                  28 + fieldWidth);
        formatter.column2("Queued", getTotalTQueued(), State.TQUEUED);
        formatter.column1("Waiting for CPU", getTotalPriorityTQueued(), State.PRIORITYTQUEUED);
        formatter.column1("Running (max " + getThreadPoolSize() + ")", getTotalRunningState(), State.RUNNING);
        formatter.column1("Running without thread", getTotalRunningWithoutThreadState(), State.RUNNINGWITHOUTTHREAD);
        formatter.column1("Waiting for callback", getTotalAsyncWait(), State.ASYNCWAIT);
        formatter.sum("In progress (max " + getMaxInProgress() + ")", getTotalInprogress());
        formatter.column2("Queued for retry", getTotalRetryWait(), State.RETRYWAIT);
        if (getTotalRQueued() + getMaxReadyJobs() + getTotalReady() > 0) {
            formatter.column2("Queued for transfer", getTotalRQueued(), State.RQUEUED);
            formatter.column2("Waiting for transfer (max " + getMaxReadyJobs() + ")", getTotalReady(), State.READY);
        }
        formatter.line();
        formatter.column2("Total requests (max " + getMaxRequests() + ")", getTotalRequests());
        formatter.format("\n");
        formatter.format("    Maximum number of retries       : %d\n", maxNumberOfRetries);
        formatter.format("    Retry timeout                   : %d ms\n", retryTimeout);
        formatter.format("    Retry limit                     : %d retries\n", maxNumberOfRetries);
        formatter.format("    Scheduling strategy             : %s\n", schedulingStrategyName);
        formatter.format("    Transfer strategy               : %s\n", transferStrategyName);
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
        printQueue(sb, jobs.get(State.TQUEUED));
    }

    public synchronized void printReadyQueue(StringBuilder sb)
    {
        sb.append("ReadyQueue :\n");
        printQueue(sb, jobs.get(State.RQUEUED));
    }

    /**
     * Getter for property queuesUpdateMaxWait.
     *
     * @return Value of property queuesUpdateMaxWait.
     */
    public synchronized long getQueuesUpdateMaxWait()
    {
        return queuesUpdateMaxWait;
    }

    /**
     * Setter for property queuesUpdateMaxWait.
     *
     * @param queuesUpdateMaxWait New value of property queuesUpdateMaxWait.
     */
    public synchronized void setQueuesUpdateMaxWait(long queuesUpdateMaxWait)
    {
        this.queuesUpdateMaxWait = queuesUpdateMaxWait;
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


