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
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.policies.DefaultJobAppraiser;
import org.dcache.srm.scheduler.policies.JobPriorityPolicyInterface;
import org.dcache.srm.util.JDC;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class Scheduler <T extends Job>
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Scheduler.class);

    private int maxRequests;

    // thread queue variables
    private final ModifiableQueue requestQueue;
    private final CountByCreator threadQueuedJobsNum =
            new CountByCreator();

    // priority thread queue variables
    private final CountByCreator priorityThreadQueuedJobsNum =
            new CountByCreator();

    // running state related variables
    private int maxRunningByOwner;
    private final CountByCreator runningStateJobsNum =
            new CountByCreator();

    // runningWithoutThread state related variables
    private final CountByCreator runningWithoutThreadStateJobsNum =
            new CountByCreator();

    // thread pool related variables
    private final ThreadPoolExecutor pooledExecutor;

    // ready queue related variables
    private final CountByCreator readyQueuedJobsNum =
            new CountByCreator();

    // ready state related variables
    private int maxReadyJobs;
    private final CountByCreator readyJobsNum =
            new CountByCreator();

    // async wait state related variables
    private int maxInProgress;
    private final CountByCreator asyncWaitJobsNum =
            new CountByCreator();

    // retry wait state related variables
    private int maxNumberOfRetries;
    private long retryTimeout;
    private final CountByCreator retryWaitJobsNum =
            new CountByCreator();


    private final String id;
    private volatile boolean running;

    // this will not prevent jobs from getting into the waiting state but will
    // prevent the addition of the new jobs to the scheduler

    private final ModifiableQueue readyQueue;
    //
    // this timer is used for tracking the expiration of retry timeout
    private final Timer retryTimer;

    // this will contain the number of
    private final long timeStamp = System.currentTimeMillis();
    private long queuesUpdateMaxWait = 60 * 1000;

    private static volatile Map<String, Scheduler<?>> schedulers = ImmutableMap.of();

    private JobPriorityPolicyInterface jobAppraiser;
    private String priorityPolicyPlugin;

    private final WorkSupplyService workSupplyService;

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
        this.id = checkNotNull(id);
        checkArgument(!id.isEmpty(), "need non-empty string as an id");

        requestQueue = new ModifiableQueue(type);
        readyQueue = new ModifiableQueue(type);

        jobAppraiser = new DefaultJobAppraiser();
        priorityPolicyPlugin = jobAppraiser.getClass().getSimpleName();

        workSupplyService = new WorkSupplyService();
        retryTimer = new Timer();
        pooledExecutor = new ThreadPoolExecutor(30, 30, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        addScheduler(id, this);
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
                job.setState(State.TQUEUED, "Request enqueued.");
                if (!threadQueue(job)) {
                    LOGGER.warn("Thread queue limit reached.");
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

    /**
     * Add a job that requires no scheduling.  This is called during SRM
     * restart.
     *
     * REVISIT: should this be merged with schedule or non-scheduled job handling
     * moved outside of Scheduler.
     */
    public void add(Job job) throws IllegalStateException
    {
        job.wlock();
        try {
            switch (job.getState()) {
            case RQUEUED:
                increaseNumberOfReadyQueued(job);
                break;

            case READY:
                // NB. this may increase number of READY jobs beyond the
                // accepted limit (i.e., the limit was decreased during SRM
                // restart); however, there's not much we can do about this
                // as the client already knows about this TURL, so we cannot
                // reduce the number of active TURLs.
                increaseNumberOfReady(job);
                break;

            default:
                throw new IllegalStateException("cannot accept job in state " +
                        job.getState());
            }
        } finally {
            job.wunlock();
        }
    }


    private void increaseNumberOfRunningState(Job job)
    {
        runningStateJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfRunningState(Job job)
    {
        runningStateJobsNum.decrement(job.getSubmitterId());
    }

    public int getRunningStateByCreator(Job job)
    {
        return runningStateJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRunningState()
    {
        return runningStateJobsNum.getTotal();
    }

    private void increaseNumberOfRunningWithoutThreadState(Job job)
    {
        runningWithoutThreadStateJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfRunningWithoutThreadState(Job job)
    {
        runningWithoutThreadStateJobsNum.decrement(job.getSubmitterId());
    }

    public int getRunningWithoutThreadStateByCreator(Job job)
    {
        return runningWithoutThreadStateJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRunningWithoutThreadState()
    {
        return runningWithoutThreadStateJobsNum.getTotal();
    }

    private void increaseNumberOfTQueued(Job job)
    {
        threadQueuedJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfTQueued(Job job)
    {
        threadQueuedJobsNum.decrement(job.getSubmitterId());
    }

    public int getTQueuedByCreator(Job job)
    {
        return threadQueuedJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalTQueued()
    {
        return threadQueuedJobsNum.getTotal();
    }

    private void increaseNumberOfPriorityTQueued(Job job)
    {
        priorityThreadQueuedJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfPriorityTQueued(Job job)
    {
        priorityThreadQueuedJobsNum.decrement(job.getSubmitterId());
    }

    public int getPriorityTQueuedByCreator(Job job)
    {
        return priorityThreadQueuedJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalPriorityTQueued()
    {
        return priorityThreadQueuedJobsNum.getTotal();
    }

    private void increaseNumberOfReadyQueued(Job job)
    {
        readyQueuedJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfReadyQueued(Job job)
    {
        readyQueuedJobsNum.decrement(job.getSubmitterId());
    }

    public int getRQueuedByCreator(Job job)
    {
        return readyQueuedJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRQueued()
    {
        return readyQueuedJobsNum.getTotal();
    }

    private void increaseNumberOfReady(Job job)
    {
        readyJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfReady(Job job)
    {
        readyJobsNum.decrement(job.getSubmitterId());
    }

    public int getReadyByCreator(Job job)
    {
        return readyJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalReady()
    {
        return readyJobsNum.getTotal();
    }

    private void increaseNumberOfAsyncWait(Job job)
    {
        asyncWaitJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfAsyncWait(Job job)
    {
        asyncWaitJobsNum.decrement(job.getSubmitterId());
    }

    public int getAsyncWaitByCreator(Job job)
    {
        return asyncWaitJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalAsyncWait()
    {
        return asyncWaitJobsNum.getTotal();
    }

    private void increaseNumberOfRetryWait(Job job)
    {
        retryWaitJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfRetryWait(Job job)
    {
        retryWaitJobsNum.decrement(job.getSubmitterId());
    }

    public int getRetryWaitByCreator(Job job)
    {
        return retryWaitJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRetryWait()
    {
        return retryWaitJobsNum.getTotal();
    }

    public void tryToReadyJob(Job job)
    {
        if (getTotalReady() >= getMaxReadyJobs()) {
            // can't add any more jobs to ready state
            return;
        }
        try {
            job.setState(State.READY, "Execution succeeded.");
        } catch (IllegalStateTransition ist) {
            //nothing more we can do here
            LOGGER.error("Illegal State Transition : {}", ist.getMessage());
        }
    }

    private boolean threadQueue(Job job)
    {
        if (getTotalRequests() < getMaxRequests()) {
            requestQueue.put(job);
            workSupplyService.distributeWork();
            return true;
        }
        return false;
    }

    private int getTotalRequests()
    {
        return getTotalTQueued() + getTotalInprogress() + getTotalRQueued();
    }

    private int getTotalInprogress()
    {
        return getTotalAsyncWait() + getTotalPriorityTQueued() + getTotalRunningState() + getTotalRunningWithoutThreadState();
    }

    private void readyQueue(Job job)
    {
        readyQueue.put(job);
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
            while (true) {
                ModifiableQueue.ValueCalculator calc =
                        new ModifiableQueue.ValueCalculator()
                        {
                            private final JobPriorityPolicyInterface jobAppraiser = getJobAppraiser();
                            private final int maxRunningByOwner = getMaxRunningByOwner();

                            @Override
                            public int calculateValue(
                                    int queueLength,
                                    int queuePosition,
                                    Job job)
                            {
                                int numOfRunningBySameCreator = getTotalRunningByCreator(job);
                                int value = jobAppraiser.evaluateJobPriority(
                                        queueLength, queuePosition,
                                        numOfRunningBySameCreator,
                                        maxRunningByOwner,
                                        job);
                                //logger.debug("updateThreadQueue calculateValue return value="+value+" for "+o);
                                return value;
                            }
                        };
                Job job = requestQueue.getGreatestValueObject(calc);

                if (job == null) {
                    //logger.debug("updateThreadQueue(), job is null, trying threadQueue.peek();");
                    job = requestQueue.peek();
                }

                if (job == null) {
                    //logger.debug("updateThreadQueue no jobs were found, breaking the update loop");
                    break;
                }

                /* Don't prepare more jobs if max allowed IN_PROGRESS jobs has been reached. */
                if (getTotalInprogress() > getMaxInProgress()) {
                    break;
                }

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

    private int getTotalRunningByCreator(Job job)
    {
        return getAsyncWaitByCreator(job) +
                getPriorityTQueuedByCreator(job) +
                getRunningStateByCreator(job) +
                getRunningWithoutThreadStateByCreator(job) +
                getRQueuedByCreator(job) +
                getReadyByCreator(job);
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
                        job.run();
                        LOGGER.trace("Scheduler(id={}) job.run() returned", getId());
                    } catch (NonFatalJobFailure e) {
                        job.wlock();
                        try {
                            if (job.getNumberOfRetries() < getMaxNumberOfRetries() &&
                                    job.getNumberOfRetries() < job.getMaxNumberOfRetries()) {
                                job.setState(State.RETRYWAIT,
                                        " nonfatal error [" + e.toString() + "] retrying");
                                startRetryTimer(job);
                            } else {
                                job.setState(State.FAILED,
                                        "Maximum number of retries exceeded: " + e.getMessage());
                            }
                        } catch (IllegalStateTransition ist) {
                            LOGGER.error("Illegal State Transition : " + ist.getMessage());
                        } finally {
                            job.wunlock();
                        }
                        return;
                    } catch (FatalJobFailure e) {
                        try {
                            job.setState(State.FAILED, e.getMessage());
                        } catch (IllegalStateTransition ist) {
                            LOGGER.error("Illegal State Transition : " + ist.getMessage());
                        }
                        return;
                    } catch (RuntimeException e) {
                        try {
                            LOGGER.error("Bug detected by SRM Scheduler", e);
                            job.setState(State.FAILED, "Internal error: " + e.toString());
                        } catch (IllegalStateTransition ist) {
                            // FIXME how should we fail a request that is
                            // already in a terminal state?
                            LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                        }
                        return;
                    }

                    job.wlock();
                    try {
                        if (job.getState() == State.RUNNING) {
                            // put blocks if ready queue is full
                            job.setState(State.RQUEUED, "Putting on a \"Ready\" Queue.");
                            readyQueue(job);
                        }
                    } catch (IllegalStateTransition e) {
                        LOGGER.error("Illegal State Transition : " + e.getMessage());
                    } finally {
                        job.wunlock();
                    }
                } catch (Throwable t) {
                    Thread thread = Thread.currentThread();
                    thread.getUncaughtExceptionHandler().uncaughtException(thread, t);
                } finally {
                    workSupplyService.distributeWork();
                }
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

    public void stateChanged(Job job, State oldState, State newState)
    {
        checkNotNull(job);

        switch (newState) {
        case TQUEUED:
            increaseNumberOfTQueued(job);
            break;
        case PRIORITYTQUEUED:
            increaseNumberOfPriorityTQueued(job);
            break;
        case RUNNING:
            increaseNumberOfRunningState(job);
            break;
        case RQUEUED:
            increaseNumberOfReadyQueued(job);
            break;
        case READY:
        case TRANSFERRING:
            increaseNumberOfReady(job);
            break;
        case ASYNCWAIT:
            increaseNumberOfAsyncWait(job);
            break;
        case RETRYWAIT:
            increaseNumberOfRetryWait(job);
            break;
        case RUNNINGWITHOUTTHREAD:
            increaseNumberOfRunningWithoutThreadState(job);
            break;
        }

        switch (oldState) {
        case TQUEUED:
            requestQueue.remove(job);
            decreaseNumberOfTQueued(job);
            break;
        case PRIORITYTQUEUED:
            decreaseNumberOfPriorityTQueued(job);
            break;
        case RUNNING:
            decreaseNumberOfRunningState(job);
            break;
        case RQUEUED:
            readyQueue.remove(job);
            decreaseNumberOfReadyQueued(job);
            break;
        case READY:
        case TRANSFERRING:
            decreaseNumberOfReady(job);
            break;
        case ASYNCWAIT:
            decreaseNumberOfAsyncWait(job);
            break;
        case RETRYWAIT:
            decreaseNumberOfRetryWait(job);
            break;
        case RUNNINGWITHOUTTHREAD:
            decreaseNumberOfRunningWithoutThreadState(job);
            break;
        }


        LOGGER.debug("state changed for job id {} from {} to {}", job.getId(), oldState, newState);
        if (oldState == State.RETRYWAIT && newState.isFinal()) {
            job.cancelRetryTimer();
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
     * Getter for property maxRunningByOwner.
     *
     * @return Value of property maxRunningByOwner.
     */
    public synchronized int getMaxRunningByOwner()
    {
        return maxRunningByOwner;
    }

    /**
     * Setter for property maxRunningByOwner.
     *
     * @param maxRunningByOwner New value of property maxRunningByOwner.
     */
    public synchronized void setMaxRunningByOwner(int maxRunningByOwner)
    {
        this.maxRunningByOwner = maxRunningByOwner;
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

    public synchronized void getInfo(StringBuilder sb)
    {
        sb.append("Scheduler id=").append(id).append('\n');
        sb.append("          asyncWaitJobsNum=").append(getTotalAsyncWait())
                .append('\n');
        sb.append("          maxAsyncWaitJobsNum=").append(maxInProgress)
                .append('\n');
        sb.append("          retryWaitJobsNum=").append(getTotalRetryWait())
                .append('\n');
        sb.append("          readyJobsNum=").append(getTotalReady())
                .append('\n');
        sb.append("          maxReadyJobs=").append(maxReadyJobs).append('\n');
        sb.append("          max number of jobs Running By the same owner=")
                .append(maxRunningByOwner).append('\n');
        sb.append("          total number of jobs in Running State =")
                .append(getTotalRunningState()).append('\n');
        sb.append("          total number of jobs in RunningWithoutThread State =")
                .append(getTotalRunningWithoutThreadState()).append('\n');
        sb.append("          threadPoolSize=").append(getThreadPoolSize())
                .append('\n');
        sb.append("          retryTimeout=").append(retryTimeout).append('\n');
        sb.append("          maxThreadQueueSize=").append(getMaxRequests())
                .append('\n');
        sb.append("          threadQueue size=").append(requestQueue.size())
                .append('\n');
        sb.append("          !!! threadQueued=").append(getTotalTQueued())
                .append('\n');
        sb.append("          !!! priorityThreadQueued=")
                .append(getTotalPriorityTQueued()).append('\n');
        sb.append("          readyQueue size=").append(readyQueue.size())
                .append('\n');
        sb.append("          !!! readyQueued=").append(getTotalRQueued())
                .append('\n');
        sb.append("          maxNumberOfRetries=").append(maxNumberOfRetries)
                .append('\n');
    }

    public void printThreadQueue(StringBuilder sb)
    {
        sb.append("ThreadQueue :\n");
        requestQueue.printQueue(sb);
    }

    public void printReadyQueue(StringBuilder sb)
    {
        sb.append("ReadyQueue :\n");
        readyQueue.printQueue(sb);
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

    public synchronized JobPriorityPolicyInterface getJobAppraiser()
    {
        return jobAppraiser;
    }

    public synchronized void setJobAppraiser(JobPriorityPolicyInterface jobAppraiser)
    {
        this.jobAppraiser = jobAppraiser;
    }

    public synchronized void setPriorityPolicyPlugin(String name)
    {
        priorityPolicyPlugin = name;
        String className = "org.dcache.srm.scheduler.policies." + priorityPolicyPlugin;
        try {
            Class<? extends JobPriorityPolicyInterface> appraiserClass =
                    Class.forName(className).asSubclass(JobPriorityPolicyInterface.class);
            jobAppraiser = appraiserClass.newInstance();
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException e) {
            LOGGER.error("failed to load {}", className);
            jobAppraiser = new DefaultJobAppraiser();
        }
    }

    public synchronized String getPriorityPolicyPlugin()
    {
        return priorityPolicyPlugin;
    }

    public Class<T> getType()
    {
        return (Class<T>) requestQueue.getType();
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


