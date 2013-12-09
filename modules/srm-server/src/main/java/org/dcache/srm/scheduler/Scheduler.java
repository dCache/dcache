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
import com.google.common.util.concurrent.AbstractService;
import com.google.common.util.concurrent.Service;
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
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.scheduler.policies.DefaultJobAppraiser;
import org.dcache.srm.scheduler.policies.JobPriorityPolicyInterface;
import org.dcache.srm.util.JDC;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public final class Scheduler
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Scheduler.class);

    public static final int ON_RESTART_FAIL_REQUEST = 1;
    public static final int ON_RESTART_RESTORE_REQUEST = 2;
    public static final int ON_RESTART_WAIT_FOR_UPDATE_REQUEST = 3;

    public int restorePolicy = ON_RESTART_WAIT_FOR_UPDATE_REQUEST;
    // thread queue variables
    private final ModifiableQueue threadQueue;
    private final CountByCreator threadQueuedJobsNum =
            new CountByCreator();

    // priority thread queue variables
    private final ModifiableQueue priorityThreadQueue;
    private final CountByCreator priorityThreadQueuedJobsNum =
            new CountByCreator();

    // running state related variables
    private int maxRunningByOwner = 10;
    private final CountByCreator runningStateJobsNum =
            new CountByCreator();

    // runningWithoutThread state related variables
    private int maxRunningWithoutThreadByOwner = 10;
    private final CountByCreator runningWithoutThreadStateJobsNum =
            new CountByCreator();

    // thread pool related variables
    private final ThreadPoolExecutor pooledExecutor;
    private final CountByCreator runningThreadsNum =
            new CountByCreator();

    // ready queue related variables
    private final CountByCreator readyQueuedJobsNum =
            new CountByCreator();

    // ready state related variables
    private int maxReadyJobs = 60;
    private final CountByCreator readyJobsNum =
            new CountByCreator();

    // async wait state related variables
    private int maxAsyncWaitJobs = 1000;
    private final CountByCreator asyncWaitJobsNum =
            new CountByCreator();

    // retry wait state related variables
    private int maxRetryWaitJobs = 1000;
    private int maxNumberOfRetries = 20;
    private long retryTimeout = 60 * 1000; //one minute
    private final CountByCreator retryWaitJobsNum =
            new CountByCreator();

    // retry wait state related variables
    private final CountByCreator restoredJobsNum =
            new CountByCreator();


    private final String id;
    private volatile boolean running;

    // private Object waitingGuard = new Object();
    // private int waitingJobNum;
    // this will not prevent jobs from getting into the waiting state but will
    // prevent the addition of the new jobs to the scheduler

    private final ModifiableQueue readyQueue;
    //
    // this timer is used for tracking the expiration of retry timeout
    private final Timer retryTimer;

    private boolean useFairness = true;
    //private boolean useJobPriority;
    //private boolean useCreatorPriority;

    // this will contain the number of
    private final long timeStamp = System.currentTimeMillis();
    private long queuesUpdateMaxWait = 60 * 1000;

    private static volatile Map<String, Scheduler> schedulers = ImmutableMap.of();

    private JobPriorityPolicyInterface jobAppraiser;
    private String priorityPolicyPlugin;

    private final WorkSupplyService workSupplyService;

    public static Scheduler getScheduler(String id)
    {
        return schedulers.get(id);
    }

    public static synchronized void addScheduler(String id, Scheduler scheduler)
    {
        schedulers = ImmutableMap.<String, Scheduler>builder()
                .putAll(schedulers)
                .put(id, scheduler)
                .build();
    }

    public Scheduler(String id, Class<? extends Job> type)
    {
        this.id = checkNotNull(id);
        checkArgument(!id.isEmpty(), "need non-empty string as an id");

        threadQueue = new ModifiableQueue("ThreadQueue", id, type);
        priorityThreadQueue = new ModifiableQueue("PriorityThreadQueue", id, type);
        readyQueue = new ModifiableQueue("ReadyQueue", id, type);

        jobAppraiser = new DefaultJobAppraiser();
        priorityPolicyPlugin = jobAppraiser.getClass().getSimpleName();

        workSupplyService = new WorkSupplyService();
        retryTimer = new Timer();
        pooledExecutor = new ThreadPoolExecutor(30, 30, 0, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(1));

        addScheduler(id, this);
    }

    public synchronized void start() throws IllegalStateException
    {
        checkState(!running, "Scheduler is running.");
        running = true;
        workSupplyService.startAsync().awaitRunning();
    }

    public synchronized void stop()
    {
        running = false;
        workSupplyService.stopAsync().awaitTerminated();
        retryTimer.cancel();
        pooledExecutor.shutdownNow();
    }


    public void schedule(Job job)
            throws IllegalStateException,
                   InterruptedException,
                   IllegalStateTransition
    {
        checkState(running, "scheduler is not running");
        LOGGER.trace("schedule is called for job with id={} in state={}", job.getId(), job.getState());

        job.wlock();
        try {
            switch (job.getState()) {
            case PENDING:
                job.setScheduler(this.id, timeStamp);
                // fall through
            case RESTORED:
                if (getTotalTQueued() >= getMaxThreadQueueSize()) {
                    job.setState(State.FAILED, "Too many jobs in the queue.");
                    return;
                }
                job.setState(State.TQUEUED, "Queued for execution.");
                if (!threadQueue(job)) {
                    LOGGER.warn("Thread queue limit reached.");
                    job.setState(State.FAILED, "Site busy: too many queued requests.");
                }
                break;
            case ASYNCWAIT:
            case RETRYWAIT:
            case RUNNINGWITHOUTTHREAD:
                LOGGER.trace("putting job in a priority thread queue, job#{}", job.getId());
                job.setState(State.PRIORITYTQUEUED, "queued for execution");
                if (!priorityQueue(job)) {
                    LOGGER.warn("Priority thread queue limit reached.");
                    job.setState(State.FAILED, "Site busy: too many queued requests.");
                }
                break;
            default:
                throw new IllegalStateException("cannot schedule job in state =" + job.getState());
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

    private void increaseNumberOfRunningThreads(Job job)
    {
        runningThreadsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfRunningThreads(Job job)
    {
        runningThreadsNum.decrement(job.getSubmitterId());
    }

    public int getRunningThreadsByCreator(Job job)
    {
        return runningThreadsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRunningThreads()
    {
        return runningThreadsNum.getTotal();
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

    private void increaseNumberOfRestored(Job job)
    {
        restoredJobsNum.increment(job.getSubmitterId());
    }

    private void decreaseNumberOfRestored(Job job)
    {
        restoredJobsNum.decrement(job.getSubmitterId());
    }

    public int getRestoredByCreator(Job job)
    {
        return restoredJobsNum.getValue(job.getSubmitterId());
    }

    public int getTotalRestored()
    {
        return restoredJobsNum.getTotal();
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

    private void updateReadyQueue()
            throws SRMInvalidRequestException
    {
        while (true) {

            if (getTotalReady() >= getMaxReadyJobs()) {
                // cann't add any more jobs to ready state
                return;
            }
            Job job = null;
            if (isFair()) {
                //logger.debug("updateReadyQueue(), using ValueCalculator to find next job");
                ModifiableQueue.ValueCalculator calc =
                        new ModifiableQueue.ValueCalculator()
                        {
                            private final JobPriorityPolicyInterface jobAppraiser = getJobAppraiser();
                            private final int maxReadyJobs = getMaxReadyJobs();

                            @Override
                            public int calculateValue(
                                    int queueLength,
                                    int queuePosition,
                                    Job job)
                            {
                                int numOfReadyBySameCreator =
                                        getReadyByCreator(job);
                                int value = jobAppraiser.evaluateJobPriority(
                                        queueLength, queuePosition,
                                        numOfReadyBySameCreator,
                                        maxReadyJobs,
                                        job);

                                // logger.debug("updateReadyQueue calculateValue return value="+value+" for "+o);

                                return value;
                            }
                        };
                job = readyQueue.getGreatestValueObject(calc);
            }
            if (job == null) {
                //logger.debug("updateReadyQueue(), job is null, trying readyQueue.peek();");
                job = readyQueue.peek();
            }

            if (job == null) {
                // no more jobs to add to the ready state set
                //logger.debug("updateReadyQueue no jobs were found, breaking the update loop");
                return;
            }

            LOGGER.debug("updateReadyQueue(), found job id {}", job.getId());
            tryToReadyJob(job);
        }
    }

    private boolean threadQueue(Job job)
    {
        if (threadQueue.offer(job)) {
            workSupplyService.distributeWork();
            return true;
        }
        return false;
    }

    private boolean priorityQueue(Job job)
    {
        if (priorityThreadQueue.offer(job)) {
            workSupplyService.distributeWork();
            return true;
        }
        return false;
    }

    private boolean readyQueue(Job job)
    {
        if (readyQueue.offer(job)) {
            workSupplyService.distributeWork();
            return true;
        }
        return false;
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
                        //logger.debug("Scheduler(id="+getId()+").run() updating Priority Thread queue...");
                        updatePriorityThreadQueue();
                        //logger.debug("Scheduler(id="+getId()+").run() updating Thread queue...");
                        updateThreadQueue();
                        // logger.debug("Scheduler(id="+getId()+").run() updating Ready queue...");
                        // Do not update ready queue, let users ask for statuses
                        // which will lead to the updates
                        // updateReadyQueue();
                        // logger.debug("Scheduler(id="+getId()+").run() done updating queues");

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

        private void updatePriorityThreadQueue() throws SRMInvalidRequestException,
                InterruptedException
        {
            while (true) {
                Job job = null;
                if (useFairness) {
                    //logger.debug("updatePriorityThreadQueue(), using ValueCalculator to find next job");
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
                                    int numOfRunningBySameCreator =
                                            getRunningStateByCreator(job) +
                                                    getRunningWithoutThreadStateByCreator(job);

                                    int value = jobAppraiser.evaluateJobPriority(
                                            queueLength, queuePosition,
                                            numOfRunningBySameCreator,
                                            maxRunningByOwner,
                                            job);
                                    if (job instanceof FileRequest) {
                                        LOGGER.trace("UPDATEPRIORITYTHREADQUEUE ca {}",
                                                ((FileRequest<?>) job).getCredential());
                                    }
                                    return value;
                                }
                            };
                    job = priorityThreadQueue.getGreatestValueObject(calc);
                }
                if (job == null) {
                    //logger.debug("updatePriorityThreadQueue(), job is null, trying priorityThreadQueue.peek();");
                    job = priorityThreadQueue.peek();
                }

                if (job == null) {
                    //logger.debug("updatePriorityThreadQueue no jobs were found, breaking the update loop");
                    break;
                }

                //we consider running and runningWithoutThreadStateJobsNum as occupying slots in the
                // thread pool, even if the runningWithoutThreadState jobs are not actually running,
                // but waiting for the notifications
                if (getTotalRunningThreads() + getTotalRunningWithoutThreadState() > getThreadPoolSize()) {
                    break;
                }

                LOGGER.trace("updatePriorityThreadQueue(), found job id {}", job.getId());

                if (job.getState() != org.dcache.srm.scheduler.State.PRIORITYTQUEUED) {
                    // someone has canceled the job or
                    // its lifetime has expired
                    LOGGER.error("updatePriorityThreadQueue() : found a job in priority thread queue with a state different from PRIORITYTQUEUED, job id={} state={}",
                            job.getId(), job.getState());
                    priorityThreadQueue.remove(job);
                    continue;
                }
                try {
                    LOGGER.trace("updatePriorityThreadQueue ()  executing job id={}", job.getId());
                    JobWrapper wrapper = new JobWrapper(job);
                    pooledExecutor.execute(wrapper);
                    LOGGER.trace("updatePriorityThreadQueue() waiting startup");
                    wrapper.waitStartup();
                    LOGGER.trace("updatePriorityThreadQueue() job started");
                    /** let the stateChanged() always remove the jobs from the queue
                     */
                    // the job is running in a separate thread by this time
                    // when  ThreadPoolExecutor can not accept new Job,
                    // RejectedExecutionException will be thrown
                } catch (RejectedExecutionException ree) {
                    LOGGER.debug("updatePriorityThreadQueue() cannot execute job id={} at this time: RejectedExecutionException", job.getId());
                    break;
                } catch (RuntimeException re) {
                    LOGGER.debug("updatePriorityThreadQueue() cannot execute job id={} at this time", job.getId());
                    break;
                }
            }
        }

        private void updateThreadQueue() throws SRMInvalidRequestException,
                InterruptedException
        {
            while (true) {
                Job job = null;
                if (isFair()) {
                    //logger.debug("updateThreadQueue(), using ValueCalculator to find next job");
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

                                    int numOfRunningBySameCreator =
                                            getRunningStateByCreator(job) +
                                                    getRunningWithoutThreadStateByCreator(job);
                                    int value = jobAppraiser.evaluateJobPriority(
                                            queueLength, queuePosition,
                                            numOfRunningBySameCreator,
                                            maxRunningByOwner,
                                            job);
                                    //logger.debug("updateThreadQueue calculateValue return value="+value+" for "+o);
                                    return value;
                                }
                            };
                    job = threadQueue.getGreatestValueObject(calc);
                }

                if (job == null) {
                    //logger.debug("updateThreadQueue(), job is null, trying threadQueue.peek();");
                    job = threadQueue.peek();
                }

                if (job == null) {
                    //logger.debug("updateThreadQueue no jobs were found, breaking the update loop");
                    break;
                }
                //we consider running and runningWithoutThreadStateJobsNum as occupying slots in the
                // thread pool, even if the runningWithoutThreadState jobs are not actually running,
                // but waiting for the notifications
                if (getTotalRunningThreads() + getTotalRunningWithoutThreadState() > getThreadPoolSize()) {
                    break;
                }
                LOGGER.trace("updateThreadQueue(), found job id {}", job.getId());

                org.dcache.srm.scheduler.State state = job.getState();
                if (state != org.dcache.srm.scheduler.State.TQUEUED) {
                    // someone has canceled the job or
                    // its lifetime has expired
                    LOGGER.error("updateThreadQueue() : found a job in thread queue with a state different from TQUEUED, job id={} state={}",
                            job.getId(), job.getState());
                    threadQueue.remove(job);
                    continue;
                }

                try {
                    LOGGER.trace("updateThreadQueue() executing job id={}", job.getId());
                    JobWrapper wrapper = new JobWrapper(job);
                    pooledExecutor.execute(wrapper);
                    LOGGER.trace("updateThreadQueue() waiting startup");
                    wrapper.waitStartup();
                    LOGGER.trace("updateThreadQueue() job started");
                        /*
                         * let the stateChanged() always remove the jobs from the queue
                         */
                    // when  ThreadPoolExecutor can not accept new Job,
                    // RejectedExecutionException will be thrown
                } catch (RejectedExecutionException ree) {
                    LOGGER.debug("updatePriorityThreadQueue() cannot execute job id={} at this time: RejectedExecutionException", job.getId());
                    break;
                } catch (RuntimeException ie) {
                    LOGGER.error("updateThreadQueue() cannot execute job id={} at this time", job.getId());
                    break;
                }

            }
        }
    }

    private class JobWrapper implements Runnable
    {
        private final Job job;
        private boolean started;

        public JobWrapper(Job job)
        {
            this.job = job;
        }

        public synchronized void waitStartup() throws InterruptedException
        {
            for (int i = 0; i < 10; ++i) {
                if (started) {
                    return;
                }
                this.wait(1000);
            }
        }

        private synchronized void started()
        {
            started = true;
            notifyAll();
        }

        @Override
        public void run()
        {
            try (JDC ignored = job.applyJdc()) {
                try {
                    increaseNumberOfRunningThreads(job);
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
                                job.setState(State.RUNNING, "Processing request", false);
                                started();
                                job.saveJob();
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
                            if (!readyQueue(job)) {
                                LOGGER.warn("All ready slots are taken and ready queue is full.");
                                job.setState(State.FAILED, "Site busy: too many active requests.");
                            }
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
                    started();
                    decreaseNumberOfRunningThreads(job);
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
                    State s = job.getState();
                    if (s != State.RETRYWAIT) {
                        LOGGER.error("retryTimer expired, but job state is {}", s);
                        return;
                    }

                    try {
                        job.setState(State.PRIORITYTQUEUED, "Queuing request for retry.");
                        if (!priorityQueue(job)) {
                            job.setState(State.FAILED, "Site busy: too many queued requests.");
                        }
                        //schedule(job);
                    } catch (IllegalStateTransition ist) {
                        LOGGER.error("can not retry: Illegal State Transition : " +
                                ist.getMessage());
                        try {
                            job.setState(State.FAILED, "Scheduling failure.");
                        } catch (IllegalStateTransition ist1) {
                            LOGGER.error("Illegal State Transition : {}", ist1.getMessage());
                        }
                    }
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
        case RESTORED:
            decreaseNumberOfRestored(job);
            break;
        case TQUEUED:
            threadQueue.remove(job);
            decreaseNumberOfTQueued(job);
            break;
        case PRIORITYTQUEUED:
            priorityThreadQueue.remove(job);
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
     * Getter for property useFairness.
     *
     * @return Value of property useFairness.
     */
    public synchronized boolean isFair()
    {
        return useFairness;
    }

    /**
     * Setter for property useFairness.
     *
     * @param useFairness New value of property useFairness.
     */
    public synchronized void setUseFairness(boolean useFairness)
    {
        this.useFairness = useFairness;
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
    public synchronized int getMaxThreadQueueSize()
    {
        return threadQueue.getCapacity();
    }

    public synchronized void setMaxThreadQueueSize(int maxThreadQueueSize)
    {
        threadQueue.setCapacity(maxThreadQueueSize);
        priorityThreadQueue.setCapacity(maxThreadQueueSize);
    }

    public synchronized int getMaxAsyncWaitJobNum()
    {
        return maxAsyncWaitJobs;
    }

    public synchronized void setMaxWaitingJobNum(int maxAsyncWaitJobs)
    {
        this.maxAsyncWaitJobs = maxAsyncWaitJobs;
    }

    public synchronized int getMaxRetryWaitJobNum()
    {
        return maxRetryWaitJobs;
    }

    public synchronized void setMaxRetryWaitJobNum(int maxRetryWaitJobs)
    {
        this.maxRetryWaitJobs = maxRetryWaitJobs;
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

    public int getMaxReadyQueueSize()
    {
        return readyQueue.getCapacity();
    }

    public void setMaxReadyQueueSize(int maxReadyQueueSize)
    {
        readyQueue.setCapacity(maxReadyQueueSize);
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
        sb.append("          useFairness=").append(useFairness).append('\n');
        sb.append("          asyncWaitJobsNum=").append(getTotalAsyncWait())
                .append('\n');
        sb.append("          maxAsyncWaitJobsNum=").append(maxAsyncWaitJobs)
                .append('\n');
        sb.append("          retryWaitJobsNum=").append(getTotalRetryWait())
                .append('\n');
        sb.append("          maxRetryWaitJobsNum=").append(maxRetryWaitJobs)
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
        sb.append("          total number of threads running =")
                .append(getTotalRunningThreads()).append('\n');
        sb.append("          retryTimeout=").append(retryTimeout).append('\n');
        sb.append("          maxThreadQueueSize=").append(getMaxThreadQueueSize())
                .append('\n');
        sb.append("          threadQueue size=").append(threadQueue.size())
                .append('\n');
        sb.append("          !!! threadQueued=").append(getTotalTQueued())
                .append('\n');
        sb.append("          priorityThreadQueue size=")
                .append(priorityThreadQueue.size()).append('\n');
        sb.append("          !!! priorityThreadQueued=")
                .append(getTotalPriorityTQueued()).append('\n');
        sb.append("          maxReadyQueueSize=").append(getMaxReadyQueueSize())
                .append('\n');
        sb.append("          readyQueue size=").append(readyQueue.size())
                .append('\n');
        sb.append("          !!! readyQueued=").append(getTotalRQueued())
                .append('\n');
        sb.append("          maxNumberOfRetries=").append(maxNumberOfRetries)
                .append('\n');
        sb.append("          number of restored but not scheduled=").
                append(getTotalRestored()).append('\n');
        sb.append("          restorePolicy");
        switch (restorePolicy) {
        case ON_RESTART_FAIL_REQUEST: {
            sb.append(" fail saved request on restart\n");
            break;
        }
        case ON_RESTART_RESTORE_REQUEST: {
            sb.append(" restore saved request on restart\n");
            break;
        }
        case ON_RESTART_WAIT_FOR_UPDATE_REQUEST: {
            sb.append(" wait for client update before restoring of saved requests on restart\n");
            break;
        }
        }
    }

    public void printThreadQueue(StringBuilder sb)
    {
        sb.append("ThreadQueue :\n");
        threadQueue.printQueue(sb);
    }

    public void printPriorityThreadQueue(StringBuilder sb)
    {
        sb.append("PriorityThreadQueue :\n");
        priorityThreadQueue.printQueue(sb);
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

    public Class<? extends Job> getType()
    {
        return threadQueue.getType();
    }
}

