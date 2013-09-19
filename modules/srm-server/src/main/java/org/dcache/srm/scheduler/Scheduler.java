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

/*
 * Scheduler.java
 *
 * Created on March 22, 2004, 2:27 PM
 */

package org.dcache.srm.scheduler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
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
/**
 *
 * @author  timur
 */
public final class Scheduler implements Runnable  {
    private static final Logger logger =
            LoggerFactory.getLogger(Scheduler.class);

	public static final int ON_RESTART_FAIL_REQUEST=1;
	public static final int ON_RESTART_RESTORE_REQUEST=2;
	public static final int ON_RESTART_WAIT_FOR_UPDATE_REQUEST=3;

	public int restorePolicy=ON_RESTART_WAIT_FOR_UPDATE_REQUEST;
	// thread queue variables
	private int maxThreadQueueSize=1000; //used for both thread and priority thread queue
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
	private int threadPoolSize=30;
	private ThreadPoolExecutor pooledExecutor;
    private final CountByCreator runningThreadsNum =
            new CountByCreator();

	// ready queue related variables
	private int maxReadyQueueSize=1000;
    private final CountByCreator readyQueuedJobsNum =
            new CountByCreator();

	// ready state related variables
	private int maxReadyJobs=60;
    private final CountByCreator readyJobsNum =
            new CountByCreator();

	// async wait state related variables
	private int maxAsyncWaitJobs=1000;
    private final CountByCreator asyncWaitJobsNum =
            new CountByCreator();

	// retry wait state related variables
	private int maxRetryWaitJobs=1000;
	private int maxNumberOfRetries=20;
	private long retryTimeout=60*1000; //one minute
    private final CountByCreator retryWaitJobsNum =
            new CountByCreator();

	// retry wait state related variables
    private final CountByCreator restoredJobsNum =
            new CountByCreator();


	private String id;
	private volatile boolean running;

	private Thread thread;

        // private Object waitingGuard = new Object();
	// private int waitingJobNum;
	// this will not prevent jobs from getting into the waiting state but will
	// prevent the addition of the new jobs to the scheduler

	private final ModifiableQueue readyQueue;
	//
	// this timer is used for tracking the expiration of retry timeout
	private Timer retryTimer;

	private boolean useFairness = true;
	//private boolean useJobPriority;
	//private boolean useCreatorPriority;

	// this will contain the number of
	private long timeStamp = System.currentTimeMillis();
	private long queuesUpdateMaxWait = 60*1000; // one

        private final static Map<String,Scheduler> schedulers =
            new HashMap();
	private JobPriorityPolicyInterface jobAppraiser;
	private String priorityPolicyPlugin="DefaultJobAppraiser";

	public static Scheduler getScheduler(String id) {
            return schedulers.get(id);
	}

	public Scheduler(String id, Class<? extends Job> type) {
		if(id == null || id.equals("")) {
			throw new IllegalArgumentException(" need non-null non-empty string as an id");
		}
		this.id = id;
		schedulers.put(id, this);
                threadQueue = new ModifiableQueue("ThreadQueue", id, type, maxThreadQueueSize);
		priorityThreadQueue = new ModifiableQueue("PriorityThreadQueue", id, type, maxThreadQueueSize);
		readyQueue = new ModifiableQueue("ReadyQueue", id, type, maxReadyQueueSize);


		String className="org.dcache.srm.scheduler.policies."+priorityPolicyPlugin;
		try {
			Class<? extends JobPriorityPolicyInterface> appraiserClass =
                                Class.forName(className).asSubclass(JobPriorityPolicyInterface.class);
			jobAppraiser = appraiserClass.newInstance();
		}
		catch (Exception e) {
			logger.error("failed to load "+className);
			jobAppraiser = new DefaultJobAppraiser();
		}
	}

	public synchronized void start() throws IllegalStateException {
		if(thread != null) {
			throw new IllegalStateException(" Scheduler is running ");
		}
		pooledExecutor = new ThreadPoolExecutor(threadPoolSize,
                    threadPoolSize,
                    0,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(1));
		retryTimer = new Timer();
                thread = new Thread(this,"Scheduler-"+id);
                running = true;
		thread.start();
	}

	public synchronized void stop()
        {
            running = false;
            notified = true;
            retryTimer.cancel();
            pooledExecutor.shutdownNow();
            notify();
	}


	public void schedule(Job job)
		throws IllegalStateException,
		InterruptedException,
		IllegalStateTransition {
		logger.debug("schedule is called for job with id="+job.getId()+" in state="+job.getState());
		if(! running) {
			throw new IllegalStateException("scheduler is not running");
		}

		job.wlock();
        try {
			State state = job.getState();
			if(state != State.RESTORED &&
			   state != State.PENDING &&
			   state != State.ASYNCWAIT &&
			   state != State.RUNNINGWITHOUTTHREAD &&
			   state != State.RETRYWAIT) {
				throw new IllegalStateException("can not schedule job in state ="+job.getState());
			}
			if(state == State.PENDING) {
				job.setScheduler(this.id, timeStamp);
			}
			if(state == State.PENDING  || state == State.RESTORED) {
                if(getTotalTQueued() >= maxThreadQueueSize) {
                    job.setState(State.FAILED,"too many jobs in the queue");
                    return;
                }
                // now we try to add the job to the thread queue without blocking
                try {
                    job.setState(State.TQUEUED,"put on the thread queue");
                    if(threadQueue(job)) {
                        // offer returned true -> successfully added job to the queue
                        return;
                    }

                }
                catch(InterruptedException ie) {
                    job.setState(State.FAILED,"scheduler interrupted");
                    return;
                }
				// if offer returned false or if it threw an exception,
				// the job could not be scheduled, so it fails
				job.setState(State.FAILED,"Thread queue is full");
                        }
			// job is either ASYNCWAIT or RETRYWAIT
			else if(state == state.ASYNCWAIT || state == State.RETRYWAIT ||
				state == state.RUNNINGWITHOUTTHREAD ) {
				// put blocks if priorityThreadQueue is full
				// this will block the retry timer (or the event handler)
                logger.debug("putting job in a priority thread queue, which might block, job#"+job.getId());
                job.setState(State.PRIORITYTQUEUED, "in priority thread queue");
                if(!priorityQueue(job))
                {
                    job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                }
                logger.debug("done putting job in a priority thread queue");
                        }
			else {
				// should never get here
				logger.error("Job #"+job.getId()+" state is "+state+" can not schedule!!!");
				job.setState(State.FAILED,"Job state is "+state+" can not schedule!!!");
                        }
		} finally {
            job.wunlock();
        }
	}

	private void increaseNumberOfRunningState(Job job) {
            runningStateJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfRunningState(Job job) {
            runningStateJobsNum.decrement(job.getSubmitterId());
	}

	public int getRunningStateByCreator(Job job) {
            return runningStateJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalRunningState(){
            return runningStateJobsNum.getTotal();
	}

	private void increaseNumberOfRunningWithoutThreadState(Job job) {
            runningWithoutThreadStateJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfRunningWithoutThreadState(Job job) {
            runningWithoutThreadStateJobsNum.decrement(job.getSubmitterId());
	}

	public int getRunningWithoutThreadStateByCreator(Job job) {
            return runningWithoutThreadStateJobsNum.getValue(job.getSubmitterId());
        }

	public int getTotalRunningWithoutThreadState(){
            return runningWithoutThreadStateJobsNum.getTotal();
	}

	private void increaseNumberOfRunningThreads(Job job) {
            runningThreadsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfRunningThreads(Job job) {
            runningThreadsNum.decrement(job.getSubmitterId());
        }

	public int getRunningThreadsByCreator(Job job) {
            return runningThreadsNum.getValue(job.getSubmitterId());
	}

	public int getTotalRunningThreads(){
            return runningThreadsNum.getTotal();
	}

	private void increaseNumberOfTQueued(Job job) {
            threadQueuedJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfTQueued(Job job) {
            threadQueuedJobsNum.decrement(job.getSubmitterId());
	}

	public int getTQueuedByCreator(Job job) {
            return threadQueuedJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalTQueued(){
            return threadQueuedJobsNum.getTotal();
	}

	private void increaseNumberOfPriorityTQueued(Job job) {
            priorityThreadQueuedJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfPriorityTQueued(Job job) {
            priorityThreadQueuedJobsNum.decrement(job.getSubmitterId());
	}

	public int getPriorityTQueuedByCreator(Job job) {
            return priorityThreadQueuedJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalPriorityTQueued(){
            return priorityThreadQueuedJobsNum.getTotal();
	}

	private void increaseNumberOfReadyQueued(Job job) {
            readyQueuedJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfReadyQueued(Job job) {
            readyQueuedJobsNum.decrement(job.getSubmitterId());
	}

	public int getRQueuedByCreator(Job job) {
            return readyQueuedJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalRQueued(){
            return readyQueuedJobsNum.getTotal();
	}

	private void increaseNumberOfReady(Job job) {
            readyJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfReady(Job job) {
            readyJobsNum.decrement(job.getSubmitterId());
        }

	public int getReadyByCreator(Job job) {
            return readyJobsNum.getValue(job.getSubmitterId());
	}

        public int getTotalReady(){
            return readyJobsNum.getTotal();
	}

	private void increaseNumberOfRestored(Job job) {
            restoredJobsNum.increment(job.getSubmitterId());
	}

	private void decreaseNumberOfRestored(Job job) {
            restoredJobsNum.decrement(job.getSubmitterId());
	}

	public int getRestoredByCreator(Job job) {
            return restoredJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalRestored(){
            return restoredJobsNum.getTotal();
	}

	private void increaseNumberOfAsyncWait(Job job) {
            asyncWaitJobsNum.increment(job.getSubmitterId());
        }

	private void decreaseNumberOfAsyncWait(Job job) {
            asyncWaitJobsNum.decrement(job.getSubmitterId());
	}

	public int getAsyncWaitByCreator(Job job) {
            return asyncWaitJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalAsyncWait(){
            return asyncWaitJobsNum.getTotal();
	}

	private void increaseNumberOfRetryWait(Job job) {
            retryWaitJobsNum.increment(job.getSubmitterId());
        }

	private void decreaseNumberOfRetryWait(Job job) {
            retryWaitJobsNum.decrement(job.getSubmitterId());
        }

	public int getRetryWaitByCreator(Job job) {
            return retryWaitJobsNum.getValue(job.getSubmitterId());
	}

	public int getTotalRetryWait(){
            return retryWaitJobsNum.getTotal();
	}

	// this is supposed to be the only place that removes the jobs from
	private void updatePriorityThreadQueue()
                throws SRMInvalidRequestException,
                       InterruptedException
        {
		while(true) {
			Job job = null;
			if(useFairness) {
				//logger.debug("updatePriorityThreadQueue(), using ValueCalculator to find next job");
				ModifiableQueue.ValueCalculator calc =
					new ModifiableQueue.ValueCalculator() {
						@Override
                                                public int calculateValue(
							int queueLength,
							int queuePosition,
							Job job) {
							int numOfRunningBySameCreator =
								getRunningStateByCreator(job)+
								getRunningWithoutThreadStateByCreator(job);

							int value = jobAppraiser.evaluateJobPriority(
								queueLength, queuePosition,
								numOfRunningBySameCreator,
								maxRunningByOwner,
								job);
                                                        if (job instanceof FileRequest) {
                                                            logger.debug("UPDATEPRIORITYTHREADQUEUE ca " +
                                                                    ((FileRequest<?>) job).getCredential());
                                                        }
							return value;
						}
					};
				job = priorityThreadQueue.getGreatestValueObject(calc);
			}
			if(job == null) {
				//logger.debug("updatePriorityThreadQueue(), job is null, trying priorityThreadQueue.peek();");
				job = priorityThreadQueue.peek();
			}

			if(job == null) {
				//logger.debug("updatePriorityThreadQueue no jobs were found, breaking the update loop");
				break;
			}

			//we consider running and runningWithoutThreadStateJobsNum as occupying slots in the
			// thread pool, even if the runningWithoutThreadState jobs are not actually running,
			// but waiting for the notifications
			if(getTotalRunningThreads() + getTotalRunningWithoutThreadState() >getThreadPoolSize()) {
				break;
			}

			logger.debug("updatePriorityThreadQueue(), found job id "+job.getId());

            if(job.getState() != State.PRIORITYTQUEUED) {
                // someone has canceled the job or
                // its lifetime has expired
                logger.error("updatePriorityThreadQueue() : found a job in priority thread queue with a state different from PRIORITYTQUEUED, job id="+
                     job.getId()+" state="+job.getState());
                priorityThreadQueue.remove(job);
                continue;
            }
			try {
				logger.debug("updatePriorityThreadQueue ()  executing job id="+job.getId());
				JobWrapper wrapper = new JobWrapper(job);
                                pooledExecutor.execute(wrapper);
 				logger.debug("updatePriorityThreadQueue() waiting startup");
				wrapper.waitStartup();
				logger.debug("updatePriorityThreadQueue() job started");
				/** let the stateChanged() always remove the jobs from the queue
				 */
				// the job is running in a separate thread by this time
			// when  ThreadPoolExecutor can not accept new Job,
			// RejectedExecutionException will be thrown
                        } catch (RejectedExecutionException ree) {
                            logger.debug("updatePriorityThreadQueue() cannot execute job id="+
                                job.getId()+" at this time: RejectedExecutionException");
				return;
			}
			catch(RuntimeException re) {
				logger.debug("updatePriorityThreadQueue() cannot execute job id="+job.getId()+" at this time");
				return;
                        }
                }
	}

	// this is supposed to be the only place that removes the jobs from
	private void updateThreadQueue()
                throws SRMInvalidRequestException,
                       InterruptedException
        {

        while(true) {
            Job job = null;
            if(useFairness) {
                //logger.debug("updateThreadQueue(), using ValueCalculator to find next job");
                ModifiableQueue.ValueCalculator calc =
                new ModifiableQueue.ValueCalculator() {
                    @Override
                    public int calculateValue(
                    int queueLength,
                    int queuePosition,
                    Job job) {

                        int numOfRunningBySameCreator =
                        getRunningStateByCreator(job)+
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

            if(job == null) {
                 //logger.debug("updateThreadQueue(), job is null, trying threadQueue.peek();");
                 job = threadQueue.peek();
            }

            if(job == null) {
                //logger.debug("updateThreadQueue no jobs were found, breaking the update loop");
                break;
            }
            //we consider running and runningWithoutThreadStateJobsNum as occupying slots in the
            // thread pool, even if the runningWithoutThreadState jobs are not actually running,
            // but waiting for the notifications
            if(getTotalRunningThreads() + getTotalRunningWithoutThreadState() >getThreadPoolSize()) {
                break;
            }
           logger.debug("updateThreadQueue(), found job id "+job.getId());

            State state = job.getState();
            if(state != State.TQUEUED ) {
                // someone has canceled the job or
                // its lifetime has expired
                logger.error("updateThreadQueue() : found a job in thread queue with a state different from TQUEUED, job id="+
                    job.getId()+" state="+job.getState());
                threadQueue.remove(job);
                continue;
            }

            try {
                logger.debug("updateThreadQueue() executing job id="+job.getId());
                JobWrapper wrapper = new JobWrapper(job);
                pooledExecutor.execute(wrapper);
                logger.debug("updateThreadQueue() waiting startup");
                wrapper.waitStartup();
                logger.debug("updateThreadQueue() job started");
                    /*
                     * let the stateChanged() always remove the jobs from the queue
                     */
            // when  ThreadPoolExecutor can not accept new Job,
            // RejectedExecutionException will be thrown
            } catch (RejectedExecutionException ree) {
                    logger.debug("updatePriorityThreadQueue() cannot execute job id="+
                        job.getId()+" at this time: RejectedExecutionException");
                    return;
            }
            catch(RuntimeException ie) {
                logger.error("updateThreadQueue() cannot execute job id=" + job
                        .getId() + " at this time");
                return;
            }

        }
    }

    public void tryToReadyJob(Job job) {
        if(getTotalReady() >= maxReadyJobs) {
            // cann't add any more jobs to ready state
            return;
        }
        try {
            job.setState(State.READY,"execution succeeded");

        }
        catch(IllegalStateTransition ist) {
            //nothing more we can do here
            logger.error("Illegal State Transition : " +ist.getMessage());

        }
    }

    private void updateReadyQueue()
            throws SRMInvalidRequestException{
        while(true) {

            if(getTotalReady() >= maxReadyJobs) {
                // cann't add any more jobs to ready state
                return;
            }
            Job job = null;
            if(useFairness) {
                //logger.debug("updateReadyQueue(), using ValueCalculator to find next job");
                ModifiableQueue.ValueCalculator calc =
                new ModifiableQueue.ValueCalculator() {
                    @Override
                    public int calculateValue(
                    int queueLength,
                    int queuePosition,
                    Job job) {
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
            if(job == null) {
                 //logger.debug("updateReadyQueue(), job is null, trying readyQueue.peek();");
                job = readyQueue.peek();
            }

            if(job == null) {
                // no more jobs to add to the ready state set
                //logger.debug("updateReadyQueue no jobs were found, breaking the update loop");
                    return;
            }

            logger.debug("updateReadyQueue(), found job id "+job.getId());
            tryToReadyJob(job);
        }
    }

    private boolean notified;


    private boolean threadQueue(Job job)
            throws  InterruptedException
    {
            if( threadQueue.offer(job,0)) {
                jobAddedToQueue();
                return true;
            }
            return false;
    }

    private boolean priorityQueue(Job job)
            throws  InterruptedException
    {
            if( priorityThreadQueue.offer(job,0)) {
                jobAddedToQueue();
                return true;
            }
            return false;
    }

    private boolean readyQueue(Job job)
            throws  InterruptedException
    {
            if( readyQueue.offer(job,0)) {
                jobAddedToQueue();
                return true;
            }
            return false;
    }

    private void jobAddedToQueue() {
        synchronized(this) {
            notifyAll();
            notified = true;
        }
    }

    @Override
    public void run() {
        try {
            while(running) {
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
                        if (!notified) {
                            //logger.debug("Scheduler(id="+getId()+").run() waiting for events...");
                            wait(queuesUpdateMaxWait);
                        }
                        notified = false;
                    }
                } catch (SRMInvalidRequestException e) {
                    logger.error("Sheduler(id={}) detected an SRM error: {}", getId(), e.toString());
                } catch (RuntimeException e) {
                    logger.error("Sheduler(id=" + getId() + ") detected a bug", e);
                }
            }
        } catch(InterruptedException e) {
            logger.error("Sheduler(id=" + getId() +
                    ") terminating update thread, since it caught an InterruptedException",
                    e);
        }
        //we are interrupted,
        //terminating thread
    }

    private class JobWrapper implements Runnable {
        Job job;
        boolean started;
        public JobWrapper(Job job) {
            this.job = job;
        }

        public synchronized void waitStartup() throws InterruptedException{
            for(int i=0;i<10;++i) {
                if(started ) {
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
        public void run() {
            try(JDC ignored = job.applyJdc()) {
                try {
                    increaseNumberOfRunningThreads(job);
                    State state;
                    logger.debug("Scheduler(id="+getId()+") entering sync(job) block" );
                    job.wlock();
                    try {
                        logger.debug("Scheduler(id="+getId()+") entered sync(job) block" );
                        state =job.getState();
                        logger.debug("Scheduler(id="+getId()+") JobWrapper run() running job in state="+state);
                        if(state == State.CANCELED ||
                        state == State.FAILED) {
                        logger.debug("Scheduler(id="+getId()+") returning" );
                            return;

                        }
                        if(state == State.PENDING ||
                        state == State.TQUEUED ||
                        state == State.PRIORITYTQUEUED ) {
                            try {
                                 logger.debug("Scheduler(id="+getId()+") changing job state to runinng");
                                job.setState(State.RUNNING," executing ",false);
                                started();
                                job.saveJob();
                            }
                            catch( IllegalStateTransition ist) {
                                logger.error("Illegal State Transition : " +ist.getMessage());
                                return;
                            }
                        }
                        else if(state == State.ASYNCWAIT ||
                        state == State.RETRYWAIT ) {
                                try {
                                 logger.debug("Scheduler(id="+getId()+") changing job state to runinng");
                                    job.setState(State.RUNNING," executing ",false);
                                    started();
                                    job.saveJob();
                                }
                                catch( IllegalStateTransition ist) {
                                    logger.error("Illegal State Transition : " +ist.getMessage());
                                    return;
                                }
                        }
                        else {
                            logger.error("Scheduler(id="+getId()+") job is in state "+state+"; can not execute, returning");
                            return;
                        }
                    } finally {
                        job.wunlock();
                    }

                    logger.debug("Scheduler(id="+getId()+") exited sync block");
                    Exception exception = null;
                    try {
                       logger.debug("Scheduler(id="+getId()+") calling job.run()");
                       job.run();
                       logger.debug("Scheduler(id="+getId()+") job.run() returned");
                    } catch(NonFatalJobFailure | FatalJobFailure | RuntimeException e)
                    {
                        exception = e;
                    }
                    job.wlock();
                    try {
                        // if no exception was thrown
                        if(exception == null) {
                            state = job.getState();
                            if(state == State.DONE) {
                            }

                            else if(state == State.RUNNING) {
                                try {
                                    // put blocks if ready queue is full
                                    job.setState(State.RQUEUED, "putting on a \"Ready\" Queue");
                                    if(!readyQueue(job))
                                    {
                                           job.setState(State.FAILED,"All Ready slots are taken and Ready Thread Queue is full. Failing request");
                                    }

                                }
                                catch(InterruptedException ie) {
                                    try {
                                        job.setState(State.FAILED,"InterruptedException while putting on the ready queue");
                                    }
                                    catch( IllegalStateTransition ist) {
                                        logger.error("Illegal State Transition : " +ist.getMessage());
                                    }
                                }
                                catch( IllegalStateTransition ist) {
                                    logger.error("Illegal State Transition : " +ist.getMessage());
                                }
                            }

                        } else {

                            if(exception instanceof NonFatalJobFailure) {

                                NonFatalJobFailure failure = (NonFatalJobFailure)exception;
                                if(job.getNumberOfRetries() < maxNumberOfRetries &&
                                job.getNumberOfRetries() < job.getMaxNumberOfRetries()) {
                                    try {
                                            job.setState(State.RETRYWAIT,
                                            " nonfatal error ["+exception.toString()+"] retrying");
                                    }
                                    catch( IllegalStateTransition ist) {
                                        logger.error("Illegal State Transition : " +ist.getMessage());
                                        return;
                                    }
                                    Scheduler.this.startRetryTimer(job);
                                }
                                else {
                                    try {
                                        job.setState(State.FAILED,
                                        "number of retries exceeded: "+failure.toString());
                                    }
                                    catch( IllegalStateTransition ist) {
                                        logger.error("Illegal State Transition : " +ist.getMessage());
                                    }
                                }
                            }
                            else if(exception instanceof FatalJobFailure) {
                                FatalJobFailure failure = (FatalJobFailure)exception;

                                try {
                                    job.setState(State.FAILED, failure.getMessage());
                                }
                                catch( IllegalStateTransition ist) {
                                    logger.error("Illegal State Transition : " +ist.getMessage());
                                }
                            }
                            else
                            {
                                // Only possibility left is RuntimeException
                                try {
                                    logger.error("Bug detected by SRM Scheduler",
                                            exception);
                                    job.setState(State.FAILED, "Internal error: " +
                                            exception.toString());
                                }
                                catch( IllegalStateTransition ist) {
                                    // FIXME how should we fail a request that is
                                    // already in a terminal state?
                                    logger.error("Illegal State Transition : {}",
                                            ist.getMessage());
                                }
                            }


                        }
                    } finally {
                        job.wunlock();
                    }
                }
                catch(Throwable t) {
                    logger.error(t.toString());
                }
                finally {
                    started();
                    decreaseNumberOfRunningThreads(job);
                    //need to notify the Scheduler that one more thread is becoming available

                    synchronized(Scheduler.this) {
                        Scheduler.this.notifyAll();
                        notified = true;
                    }
                }
            }
        }
    }

    private void startRetryTimer(final Job job) {
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                job.wlock();
                try {
                    State s = job.getState();
                    if(s != State.RETRYWAIT)
                    {
                        logger.error("retryTimer expired, but job state is "+s);
                        return;
                    }

                    try {
                        job.setState(State.PRIORITYTQUEUED, "retrying job, putting on priority queue");
                        if(!priorityQueue(job))
                        {
                            job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                        }
                        //schedule(job);
                    }
                    catch(InterruptedException ie) {
                        try {
                            job.setState(State.FAILED,
                            "scheduler is interrupted");
                        }
                        catch(IllegalStateTransition ist) {
                            logger.error("Illegal State Transition : " +ist.getMessage());
                        }
                    }
                    catch(IllegalStateTransition ist) {
                        logger.error("can not retry: Illegal State Transition : " +
                                ist.getMessage());
                        try {
                                job.setState(State.FAILED,
                                "scheduler is interrupted");
                        }
                        catch(IllegalStateTransition ist1) {
                            logger.error("Illegal State Transition : " +ist1.getMessage());
                        }
                    }
                } finally {
                    job.wunlock();
                }

            }
        };
        job.setRetryTimer(task);
        retryTimer.schedule(task,retryTimeout);
    }

    public void stateChanged(Job job,State oldState,State newState)   {
        if(job == null ) {
            logger.error("stateChanged job is null!!!");
            return;
        }
        if(newState == State.TQUEUED) {
            increaseNumberOfTQueued(job);
        }
        else if(newState == State.PRIORITYTQUEUED) {
            increaseNumberOfPriorityTQueued(job);
        }
        else if(newState == State.RUNNING) {
            increaseNumberOfRunningState(job);
        }
        else if(newState == State.RQUEUED) {
            increaseNumberOfReadyQueued(job);
        }
        else if(newState == State.READY || newState == State.TRANSFERRING) {
            increaseNumberOfReady(job);
        }
        else if(newState == State.ASYNCWAIT) {
            increaseNumberOfAsyncWait(job);
        }
        else if(newState == State.RETRYWAIT) {
            increaseNumberOfRetryWait(job);
        }
        else if(newState == State.RUNNINGWITHOUTTHREAD) {
            increaseNumberOfRunningWithoutThreadState(job);
        }

        if(oldState == State.RESTORED) {
            decreaseNumberOfRestored(job);
        }
        else if(oldState == State.TQUEUED) {
            threadQueue.remove(job);
            decreaseNumberOfTQueued(job);
        }
        else if(oldState == State.PRIORITYTQUEUED) {
            priorityThreadQueue.remove(job);
            decreaseNumberOfPriorityTQueued(job);
        }
        else if(oldState == State.RUNNING) {
            decreaseNumberOfRunningState(job);
        }
        else if(oldState == State.RQUEUED) {
            readyQueue.remove(job);
            decreaseNumberOfReadyQueued(job);
        }
        else if(oldState == State.READY || oldState == State.TRANSFERRING) {
            decreaseNumberOfReady(job);
        }
        else if(oldState == State.ASYNCWAIT) {
            decreaseNumberOfAsyncWait(job);
        }
        else if(oldState == State.RETRYWAIT) {
            decreaseNumberOfRetryWait(job);
        }
        else if(oldState == State.RUNNINGWITHOUTTHREAD) {
            decreaseNumberOfRunningWithoutThreadState(job);
        }


        logger.debug("state changed for job id "+job.getId()+" from "+oldState+" to "+newState);
        if(newState == State.DONE ||
        newState == State.CANCELED ||
        newState == State.FAILED) {


            if(oldState == State.RETRYWAIT) {
                TimerTask task = job.getRetryTimer();
                if(task != null)
                 {
                    task.cancel();
                    job.setRetryTimer(null);
                 }

            }
        }
    }

    /** Getter for property useFairness.
     * @return Value of property useFairness.
     *
     */
    public boolean isUseFairness() {
        return useFairness;
    }

    /** Setter for property useFairness.
     * @param useFairness New value of property useFairness.
     *
     */
    public void setUseFairness(boolean useFairness) {
        this.useFairness = useFairness;
    }


    /** Setter for property jobAppraiser.
     * @param jobAppraiser New value of property jobAppraiser.
     *
     */
    public void setJobAppraiser(JobPriorityPolicyInterface  jobAppraiser) {
        this.jobAppraiser = jobAppraiser;
    }

    /** Getter for property id.
     * @return Value of property id.
     *
     */
    public String getId() {
        return id;
    }

    /**
     * Getter for property threadPoolSize.
     * @return Value of property threadPoolSize.
     */
    public synchronized int getThreadPoolSize() {
        return threadPoolSize;
    }

    /**
     * Setter for property threadPoolSize.
     * @param threadPoolSize New value of property threadPoolSize.
     */
    public synchronized void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
        if(thread!= null) {
            pooledExecutor.setMaximumPoolSize(threadPoolSize);
        }
    }

    /**
     * Getter for property maxReadyJobs.
     * @return Value of property maxReadyJobs.
     */
    public int getMaxReadyJobs() {
        return maxReadyJobs;
    }

    /**
     * Setter for property maxReadyJobs.
     * @param maxReadyJobs New value of property maxReadyJobs.
     */
    public void setMaxReadyJobs(int maxReadyJobs) {
        this.maxReadyJobs = maxReadyJobs;
    }

    /**
     * Getter for property maxRunningByOwner.
     * @return Value of property maxRunningByOwner.
     */
    public int getMaxRunningByOwner() {
        return maxRunningByOwner;
    }

    /**
     * Setter for property maxRunningByOwner.
     * @param maxRunningByOwner New value of property maxRunningByOwner.
     */
    public void setMaxRunningByOwner(int maxRunningByOwner) {
        this.maxRunningByOwner = maxRunningByOwner;
    }

    /**
     * Getter for property maxThreadQueueSize.
     * @return Value of property maxThreadQueueSize.
     */
    public int getMaxThreadQueueSize() {
        return maxThreadQueueSize;
    }

    /**
     * Setter for property maxThreadQueueSize.
     * @param maxThreadQueueSize New value of property maxThreadQueueSize.
     */
    public void setMaxThreadQueueSize(int maxThreadQueueSize) {
        this.maxThreadQueueSize = maxThreadQueueSize;
        threadQueue.setCapacity(maxThreadQueueSize);
        priorityThreadQueue.setCapacity(maxThreadQueueSize);
    }

    /**
     * Getter for property maxWaitingJobNum.
     * @return Value of property maxWaitingJobNum.
     */
    public int getMaxAsyncWaitJobNum() {
        return maxAsyncWaitJobs;
    }

    /**
     * Setter for property maxWaitingJobNum.
     */
    public void setMaxWaitingJobNum(int maxAsyncWaitJobs) {
        this.maxAsyncWaitJobs = maxAsyncWaitJobs;
    }

    /**
     * Getter for property maxWaitingJobNum.
     * @return Value of property maxWaitingJobNum.
     */
    public int getMaxRetryWaitJobNum() {
        return maxRetryWaitJobs;
    }

    /**
     * Setter for property maxWaitingJobNum.
     */
    public void setMaxRetryWaitJobNum(int maxRetryWaitJobs) {
        this.maxRetryWaitJobs = maxRetryWaitJobs;
    }

    /**
     * Getter for property maxNumberOfRetries.
     * @return Value of property maxNumberOfRetries.
     */
    public int getMaxNumberOfRetries() {
        return maxNumberOfRetries;
    }

    /**
     * Setter for property maxNumberOfRetries.
     * @param maxNumberOfRetries New value of property maxNumberOfRetries.
     */
    public void setMaxNumberOfRetries(int maxNumberOfRetries) {
        this.maxNumberOfRetries = maxNumberOfRetries;
    }

    /**
     * Getter for property retryTimeout.
     * @return Value of property retryTimeout.
     */
    public long getRetryTimeout() {
        return retryTimeout;
    }

    /**
     * Setter for property retryTimeout.
     * @param retryTimeout New value of property retryTimeout.
     */
    public void setRetryTimeout(long retryTimeout) {
        this.retryTimeout = retryTimeout;
    }

    /**
     * Getter for property maxReadyQueueSize.
     * @return Value of property maxReadyQueueSize.
     */
    public int getMaxReadyQueueSize() {
        return maxReadyQueueSize;
    }

    /**
     * Setter for property maxReadyQueueSize.
     * @param maxReadyQueueSize New value of property maxReadyQueueSize.
     */
    public void setMaxReadyQueueSize(int maxReadyQueueSize) {
        this.maxReadyQueueSize = maxReadyQueueSize;
        readyQueue.setCapacity(maxReadyQueueSize);
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        getInfo(sb);
        return sb.toString();
    }

    public void getInfo(StringBuilder sb) {
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
        sb.append("          maxThreadQueueSize=").append(maxThreadQueueSize)
                .append('\n');
        sb.append("          threadQueue size=").append(threadQueue.size())
                .append('\n');
        sb.append("          !!! threadQueued=").append(getTotalTQueued())
                .append('\n');
        sb.append("          priorityThreadQueue size=")
                .append(priorityThreadQueue.size()).append('\n');
        sb.append("          !!! priorityThreadQueued=")
                .append(getTotalPriorityTQueued()).append('\n');
        sb.append("          maxReadyQueueSize=").append(maxReadyQueueSize)
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
        switch(restorePolicy) {
            case ON_RESTART_FAIL_REQUEST:
            {
                sb.append(" fail saved request on restart\n");
                break;
            }
            case ON_RESTART_RESTORE_REQUEST:
            {
                sb.append(" restore saved request on restart\n");
                break;
            }
            case ON_RESTART_WAIT_FOR_UPDATE_REQUEST:
            {
                sb.append(" wait for client update before restoring of saved requests on restart\n");
                break;
            }
        }
    }

    public void printThreadQueue(StringBuilder sb) {
        sb.append("ThreadQueue :\n");
        threadQueue.printQueue(sb);
    }
    public void printPriorityThreadQueue(StringBuilder sb) {
        sb.append("PriorityThreadQueue :\n");
        priorityThreadQueue.printQueue(sb);
    }
    public void printReadyQueue(StringBuilder sb) {
        sb.append("ReadyQueue :\n");
        readyQueue.printQueue(sb);
    }

    /**
     * Getter for property queuesUpdateMaxWait.
     * @return Value of property queuesUpdateMaxWait.
     */
    public long getQueuesUpdateMaxWait() {
        return queuesUpdateMaxWait;
    }

    /**
     * Setter for property queuesUpdateMaxWait.
     * @param queuesUpdateMaxWait New value of property queuesUpdateMaxWait.
     */
    public void setQueuesUpdateMaxWait(long queuesUpdateMaxWait) {
        this.queuesUpdateMaxWait = queuesUpdateMaxWait;
    }

	public void setPriorityPolicyPlugin(String txt) {
		priorityPolicyPlugin=txt;
		String className="org.dcache.srm.scheduler.policies."+priorityPolicyPlugin;
		try {
			Class<? extends JobPriorityPolicyInterface> appraiserClass =
                                Class.forName(className).asSubclass(JobPriorityPolicyInterface.class);
			jobAppraiser = appraiserClass.newInstance();
		}
		catch (Exception e) {
			logger.error("failed to load "+className);
			jobAppraiser = new DefaultJobAppraiser();
		}
	}

	public String getPriorityPolicyPlugin() {
		return priorityPolicyPlugin;
	}

}

