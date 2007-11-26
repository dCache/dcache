// $Id: Scheduler.java,v 1.12 2007-08-22 20:32:04 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.11  2006/12/13 22:13:20  timur
// from production branch: name scheduler threads, allow to timeout the waitStartupt of job wrapper
//
// Revision 1.10  2006/10/24 07:44:38  litvinse
// moved JobAppraiser interface into separate package
// added handles to select scheduler priority policies
//
// Revision 1.9  2006/08/25 22:46:51  timur
// disable job restoration upon restart
//
// Revision 1.8  2006/04/21 22:58:27  timur
// we do not need a thread running when we start a remote transfer, but we want to control the number of the transfers, I hope the code accomplishes this now, though an addition of the new job state
//
// Revision 1.7  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.6  2006/04/12 23:16:24  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.5  2005/12/07 16:24:07  timur
// append in setError, and log errors even when retrying
//
// Revision 1.4  2005/05/19 05:39:06  timur
// less logging via scheduler
//
// Revision 1.3  2005/05/04 21:54:52  timur
// new scheduling policy on restart for put and get request - do not schedule the request if the user does not update its status
//
// Revision 1.2  2005/03/30 22:42:11  timur
// more database schema changes
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.19  2004/11/12 22:50:40  timur
// activate the fairness in scheduler
//
// Revision 1.18  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.17  2004/11/01 18:10:28  timur
// addind more message printing to the scheduler for debugging, state varible synchronization
//
// Revision 1.16  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.15  2004/09/14 01:47:52  timur
// check that timer exists before canceling it
//
// Revision 1.14  2004/08/31 18:12:30  timur
// use linked list to store the queue
//
// Revision 1.13  2004/08/30 19:01:24  timur
// make sure that update thread does not continue, untill the job it schedules is removed from the queue
//
// Revision 1.12  2004/08/30 17:14:49  timur
// stop updating the status on the remote machine when the copy request is canceled, handle the queues more correctly
//
// Revision 1.11  2004/08/27 02:37:22  timur
// make scheduler update queues periodically, no matter what
//
// Revision 1.10  2004/08/26 21:22:30  timur
// scheduler bug (not setting job value to null) in a loop serching for the next job to execute
//
// Revision 1.9  2004/08/17 17:17:24  timur
// increment number of retries to avoid infinite retries
//
// Revision 1.8  2004/08/17 16:01:14  timur
// simplifying scheduler, removing some bugs, and redusing the amount of logs
//
// Revision 1.7  2004/08/10 17:03:47  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.6  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.5.2.9  2004/07/12 21:52:07  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.5.2.8  2004/07/09 22:14:54  timur
// more synchronization problems resloved
//
// Revision 1.5.2.7  2004/07/02 20:10:25  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.5.2.6  2004/06/30 20:37:24  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.5.2.5  2004/06/28 21:54:10  timur
// added configuration options for the schedulers
//
// Revision 1.5.2.4  2004/06/22 01:38:07  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.5.2.3  2004/06/18 22:20:52  timur
// adding sql database storage for requests
//
// Revision 1.5.2.2  2004/06/16 19:44:34  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//

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

import EDU.oswego.cs.dl.util.concurrent.BoundedLinkedQueue;
import EDU.oswego.cs.dl.util.concurrent.PooledExecutor;
import EDU.oswego.cs.dl.util.concurrent.SynchronizedBoolean;
import java.util.Timer;
import java.util.TimerTask;
import java.beans.PropertyChangeListener;
import java.util.HashMap;
import java.util.Set;
import java.util.Iterator;
import java.sql.SQLException;
import org.dcache.srm.Logger;
import org.dcache.srm.request.*;
import org.dcache.srm.scheduler.policies.*;
/**
 *
 * @author  timur
 */
public final class Scheduler implements Runnable, PropertyChangeListener {
	public static final int ON_RESTART_FAIL_REQUEST=1;
	public static final int ON_RESTART_RESTORE_REQUEST=2;
	public static final int ON_RESTART_WAIT_FOR_UPDATE_REQUEST=3;
	
	public int restorePolicy=ON_RESTART_WAIT_FOR_UPDATE_REQUEST;
	// thread queue variables
	private int threadQueuedJobsNum;
	private int maxThreadQueueSize=1000; //used for both thread and priority thread queue
	private ModifiableQueue threadQueue;
	private HashMap tQueuedByCreator = new HashMap();
	
	// priority thread queue variables
	private int priorityThreadQueuedJobsNum;
	private ModifiableQueue priorityThreadQueue;
	private HashMap priorityTQueuedByCreator = new HashMap();
	
	// running state related variables
	private int runningStateJobsNum;
	private int maxRunningByOwner = 10;
	private HashMap runningStateByCreator = new HashMap();
	
	// runningWithoutThread state related variables
	private int runningWithoutThreadStateJobsNum;
	private int maxRunningWithoutThreadByOwner = 10;
	private HashMap runningWithoutThreadStateByCreator = new HashMap();
    
	// thread pool related variables
	private int runningThreadsNum;
	private int threadPoolSize=30;
	private HashMap runningThreadByCreator = new HashMap();
	private PooledExecutor pooledExecutor;
	
	// ready queue related variables
	private int readyQueuedJobsNum;
	private int maxReadyQueueSize=1000;
	private HashMap rQueuedByCreator = new HashMap();
	
	// ready state related variables
	private int readyJobsNum;
	private int maxReadyJobs=60;
	private HashMap readyByCreator = new HashMap();
	
	// async wait state related variables
	private int asyncWaitJobsNum;
	private int maxAsyncWaitJobs=1000;
	private HashMap asyncWaitByCreator = new HashMap();
	
	// retry wait state related variables
	private int retryWaitJobsNum;
	private int maxRetryWaitJobs=1000;
	private HashMap retryWaitByCreator = new HashMap();
	private int maxNumberOfRetries=20;
	private long retryTimeout=60*1000; //one minute
	
	// retry wait state related variables
	private int restoredJobsNum;
	private HashMap restoredByCreator = new HashMap();
	
	private String id;
	private volatile boolean running = false;
	
	private Thread thread;
    
	private Object waitingGuard = new Object();
	// private int waitingJobNum;
	// this will not prevent jobs from getting into the waiting state but will
	// prevent the addition of the new jobs to the scheduler
	
	private ModifiableQueue readyQueue;
	//
	// this timer is used for tracking the expiration of retry timeout
	private Timer retryTimer;
	
	private boolean useFairness = true;
	//private boolean useJobPriority;
	//private boolean useCreatorPriority;
	
	// this will contain the number of
	private long timeStamp = System.currentTimeMillis();
	private long queuesUpdateMaxWait = 60*1000; // one
	
	private static HashMap schedulers = new HashMap();
	private Logger logger;
	private JobPriorityPolicyInterface jobAppraiser=null;
	private String priorityPolicyPlugin="DefaultJobAppraiser";

	public static Scheduler getScheduler(String id) {
		if(!schedulers.containsKey(id)) {
			return null;
		}
		return (Scheduler)schedulers.get(id);
	}
	
	public Scheduler(String id,Logger logger) {
		if(id == null || id.equals("")) {
			throw new IllegalArgumentException(" need non-null non-empty string as an id");
		}
		this.id = id;
		this.logger = logger;
		schedulers.put(id, this);
		Job.addClassStateChangeListener(this);
		
		String className="org.dcache.srm.scheduler.policies."+priorityPolicyPlugin;
		try { 
			Class appraiserClass = Class.forName(className);
			jobAppraiser = (JobPriorityPolicyInterface)appraiserClass.newInstance();
		}
		catch (Exception e) { 
			esay("failed to load "+className);
			jobAppraiser = new DefaultJobAppraiser();
		}
	}
	
	public void say(String s){
		if(logger != null) {
			logger.log("Scheduler("+getId()+") : "+s);
		}
	}
	
	public void esay(String s){
		if(logger != null) {
			logger.elog(s);
		}
	}
	
	public void esay(Throwable t){
		if(logger != null) {
			logger.elog(t);
		}
	}
	
	public synchronized void start() throws IllegalStateException {
		if(thread != null) {
			throw new IllegalStateException(" Scheduler is running ");
		}
		threadQueue = new ModifiableQueue("ThreadQueue",id,maxThreadQueueSize);
		priorityThreadQueue = new ModifiableQueue("PriorityThreadQueue",id,maxThreadQueueSize);
		pooledExecutor = new PooledExecutor(threadPoolSize);
		pooledExecutor.abortWhenBlocked();
		readyQueue = new ModifiableQueue("ReadyQueue",id,maxReadyQueueSize);
		retryTimer = new Timer();
                thread = new Thread(this,"Scheduler-"+id);
		thread.start();
		running = true;
	}
    
	
	public void schedule(Job job) 
		throws IllegalStateException, 
		InterruptedException,
		IllegalStateTransition {
		say("schedule is called for job with id="+job.getId()+" in state="+job.getState());
		if(! running) {
			throw new IllegalStateException("scheduler is not running");
		}
		synchronized(job) { 
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
				synchronized(threadQueue) {
					if(getTotalTQueued() >= maxThreadQueueSize) {
						job.setState(State.FAILED,"too many jobs in the queue");
						return;
					}
					// now we try to add the job to the thread queue without blocking
					try {
						job.setState(State.TQUEUED,"put on the thread queue");
						if(threadQueue.offer(job, 0)) {
							// offer returned true -> successfully added job to the queue
							return;
						}
						
					}
					catch(InterruptedException ie) {
						job.setState(State.FAILED,"scheduler interrupted");
						return;
					}
				}
				// if offer returned false or if it threw an exception,
				// the job could not be scheduled, so it fails
				job.setState(State.FAILED,"Thread queue is full");
				return;
			}
			// job is either ASYNCWAIT or RETRYWAIT
			else if(state == state.ASYNCWAIT || state == State.RETRYWAIT ||
				state == state.RUNNINGWITHOUTTHREAD ) {
				// put blocks if priorityThreadQueue is full
				// this will block the retry timer (or the event handler)
				synchronized(priorityThreadQueue) {
					say("putting job in a priority thread queue, which might block, job#"+job.getId());
					job.setState(State.PRIORITYTQUEUED, "in priority thread queue");
					if(!priorityThreadQueue.offer(job,0))
                                        {
                                            job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                                        }
					say("done putting job in a priority thread queue");
				}
				return;
			}
			else {
				// should never get here
				esay("Job #"+job.getId()+" state is "+state+" can not schedule!!!");
				job.setState(State.FAILED,"Job state is "+state+" can not schedule!!!");
				return;
			}
		}
	}
	
	private void increaseNumberOfRunningState(Job job) {
		synchronized(runningStateByCreator ) {
			runningStateJobsNum++;
			increaseByCreator(job,runningStateByCreator);
		}
	}
	
	private void decreaseNumberOfRunningState(Job job) {
		synchronized( runningStateByCreator) {
			runningStateJobsNum--;
			decreaseByCreator(job,runningStateByCreator);
		}
	}
	
	public int getRunningStateByCreator(Job job) {
		return getValueByCreator(job, runningStateByCreator);
	}
	
	public int getTotalRunningState(){
		synchronized(runningStateByCreator ) {
			return runningStateJobsNum;
		}
	}
	
	private void increaseNumberOfRunningWithoutThreadState(Job job) {
		synchronized(runningWithoutThreadStateByCreator ) {
			runningWithoutThreadStateJobsNum++;
			increaseByCreator(job,runningWithoutThreadStateByCreator);
		}
	}
	
	private void decreaseNumberOfRunningWithoutThreadState(Job job) {
		synchronized( runningWithoutThreadStateByCreator) {
			runningWithoutThreadStateJobsNum--;
			decreaseByCreator(job,runningWithoutThreadStateByCreator);
		}
	}
	
	public int getRunningWithoutThreadStateByCreator(Job job) {
		return getValueByCreator(job, runningWithoutThreadStateByCreator);
	}
	
	public int getTotalRunningWithoutThreadState(){
		synchronized(runningStateByCreator ) {
			return runningStateJobsNum;
		}
	}
	
	private void increaseNumberOfRunningThreads(Job job) {
		synchronized(runningThreadByCreator ) {
			runningThreadsNum++;
			increaseByCreator(job,runningThreadByCreator);
		}
	}
	
	private void decreaseNumberOfRunningThreads(Job job) {
		synchronized( runningThreadByCreator) {
			runningThreadsNum--;
			decreaseByCreator(job,runningThreadByCreator);
		}
	}
	
	public int getRunningThreadsByCreator(Job job) {
		return getValueByCreator(job, runningThreadByCreator);
	}
	
	public int getTotalRunningThreads(){
		synchronized(runningThreadByCreator ) {
			return runningThreadsNum;
		}
	}
	
	private void increaseNumberOfTQueued(Job job) {
		synchronized(tQueuedByCreator ) {
			threadQueuedJobsNum++;
			increaseByCreator(job,tQueuedByCreator);
		}
	}
	
	private void decreaseNumberOfTQueued(Job job) {
		synchronized(tQueuedByCreator ) {
			threadQueuedJobsNum--;
			decreaseByCreator(job,tQueuedByCreator);
		}
	}
	
	public int getTQueuedByCreator(Job job) {
		return getValueByCreator(job, tQueuedByCreator);
	}
	
	public int getTotalTQueued(){
		synchronized(tQueuedByCreator ) {
			return threadQueuedJobsNum;
		}
	}
	
	private void increaseNumberOfPriorityTQueued(Job job) {
		synchronized(priorityTQueuedByCreator ) {
	     priorityThreadQueuedJobsNum++;
	     increaseByCreator(job,priorityTQueuedByCreator);
		}
	}
	
	private void decreaseNumberOfPriorityTQueued(Job job) {
		synchronized(priorityTQueuedByCreator ) {
			priorityThreadQueuedJobsNum--;
			decreaseByCreator(job,priorityTQueuedByCreator);
		}
	}
	
	public int getPriorityTQueuedByCreator(Job job) {
		return getValueByCreator(job, priorityTQueuedByCreator);
	}
	
	public int getTotalPriorityTQueued(){
		synchronized(priorityTQueuedByCreator ) {
			return priorityThreadQueuedJobsNum;
		}
	}
	
	private void increaseNumberOfReadyQueued(Job job) {
		synchronized( rQueuedByCreator) {
			readyQueuedJobsNum++;
			increaseByCreator(job,rQueuedByCreator);
		}
	}
	
	private void decreaseNumberOfReadyQueued(Job job) {
		synchronized( rQueuedByCreator) {
			readyQueuedJobsNum--;
			decreaseByCreator(job,rQueuedByCreator);
		}
	}
	
	public int getRQueuedByCreator(Job job) {
		return getValueByCreator(job, rQueuedByCreator);
	}
	
	public int getTotalRQueued(){
		synchronized(rQueuedByCreator ) {
			return readyQueuedJobsNum;
		}
	}
	
	private void increaseNumberOfReady(Job job) {
		synchronized( readyByCreator) {
			readyJobsNum++;
			increaseByCreator(job,readyByCreator);
		}
	}
	
	private void decreaseNumberOfReady(Job job) {
		synchronized( readyByCreator) {
			readyJobsNum--;
			decreaseByCreator(job,readyByCreator);
		}
	}
	
	public int getReadyByCreator(Job job) {
		return getValueByCreator(job, readyByCreator);
	}
	
	public int getTotalReady(){
		synchronized(readyByCreator ) {
			return readyJobsNum;
		}
	}
	
	private void increaseNumberOfRestored(Job job) {
		synchronized( restoredByCreator) {
			restoredJobsNum++;
			increaseByCreator(job,restoredByCreator);
		}
	}
	
	private void decreaseNumberOfRestored(Job job) {
		synchronized( restoredByCreator) {
			restoredJobsNum--;
			decreaseByCreator(job,restoredByCreator);
		}
	}
	
	public int getRestoredByCreator(Job job) {
		return getValueByCreator(job, restoredByCreator);
	}
	
	public int getTotalRestored(){
		synchronized(restoredByCreator ) {
			return restoredJobsNum;
		}
	}
	
	private void increaseNumberOfAsyncWait(Job job) {
		synchronized( asyncWaitByCreator) {
			asyncWaitJobsNum++;
			increaseByCreator(job,asyncWaitByCreator);
		}
	}
	
	private void decreaseNumberOfAsyncWait(Job job) {
		synchronized( asyncWaitByCreator) {
			asyncWaitJobsNum--;
			decreaseByCreator(job,asyncWaitByCreator);
		}
	}
	
	public int getAsyncWaitByCreator(Job job) {
		return getValueByCreator(job, asyncWaitByCreator);
	}
	
	public int getTotalAsyncWait(){
		synchronized(asyncWaitByCreator ) {
			return asyncWaitJobsNum;
		}
	}
	
	private void increaseNumberOfRetryWait(Job job) {
		synchronized( retryWaitByCreator) {
			retryWaitJobsNum++;
			increaseByCreator(job,retryWaitByCreator);
		}
	}
	
	private void decreaseNumberOfRetryWait(Job job) {
		synchronized( retryWaitByCreator) {
			retryWaitJobsNum--;
			decreaseByCreator(job,retryWaitByCreator);
		}
	}
	
	public int getRetryWaitByCreator(Job job) {
		return getValueByCreator(job, retryWaitByCreator);
	}
	
	public int getTotalRetryWait(){
		synchronized(retryWaitByCreator ) {
	     return retryWaitJobsNum;
		}
	}

	
	private static void increaseByCreator(Job job, HashMap parameter) {
		JobCreator creator = job.getCreator();
		if(parameter.containsKey(creator)) {
			Integer value = (Integer)parameter.get(creator);
			parameter.put(creator, new Integer(1+value.intValue()));
		}
		else {
			parameter.put(creator,new Integer(1));
		}
	}
	
	private static void decreaseByCreator(Job job, HashMap parameter) {
		JobCreator creator = job.getCreator();
		Integer value = (Integer)parameter.get(creator);
		if(value != null) {
			parameter.put(creator, new Integer(value.intValue()-1));
		}
	}
	
	private static int getValueByCreator(JobCreator creator, HashMap parameter) {
		synchronized(parameter) {
			Integer value = (Integer)parameter.get(creator);
			if(value != null) {
				return value.intValue();
			}
		}
		return 0;
	}
	
	private static int getValueByCreator(Job job, HashMap parameter) {
		JobCreator creator = job.getCreator();
		return getValueByCreator(creator, parameter);
	}
	
	// this is supposed to be the only place that removes the jobs from
	private void updatePriorityThreadQueue()  throws java.sql.SQLException{
		while(true) {
			Job job = null;
			if(useFairness) {
				//say("updatePriorityThreadQueue(), using ValueCalculator to find next job");
				ModifiableQueue.ValueCalculator calc =
					new ModifiableQueue.ValueCalculator() {
						public int calculateValue(
							int queueLength,
							int queuePosition,
							Job job) {
							int numOfPriorityTQueuedBySameCreator =
								getPriorityTQueuedByCreator(job);
							int numOfRunningBySameCreator =
								getRunningStateByCreator(job)+
								getRunningWithoutThreadStateByCreator(job);
							
							int value = jobAppraiser.evaluateJobPriority(
								queueLength, queuePosition,
								numOfRunningBySameCreator,
								maxRunningByOwner,
								job);
							try { 
								FileRequest req = (FileRequest) job;
								say("UPDATEPRIORITYTHREADQUEUE ca " + req.getCredential());
							}
							catch (ClassCastException cce) { 
								esay("Failed to cast job to FileRequest");
								esay(cce);
							}

// 							say("UPDATEPRIORITYTHREADQUEUE calculateValue(l="+ 
// 							    queueLength+
// 							    ",p="+
// 							    queuePosition+
// 							    ") returns value="+
// 							    value+
// 							    " id "+
// 							    job.getCreator()+" priority "+
// 							    job.getCreator().getPriority());
							return value;
						}
					};
				//say("updatePriorityThreadQueue: calling getGreatestValueObject()");
				job = priorityThreadQueue.getGreatestValueObject(calc);
				//say("updatePriorityThreadQueue: getGreatestValueObject() returned "+o);
			}
			if(job == null) {
				//say("updatePriorityThreadQueue(), job is null, trying priorityThreadQueue.peek();");
				job = priorityThreadQueue.peek();
			}
			
			if(job == null) {
				//say("updatePriorityThreadQueue no jobs were found, breaking the update loop");
				break;
			}
			
			//we consider running and runningWithoutThreadStateJobsNum as occupying slots in the 
			// thread pool, even if the runningWithoutThreadState jobs are not actually running, 
			// but waiting for the notifications
			if(getTotalRunningThreads() + runningWithoutThreadStateJobsNum >threadPoolSize) {
				break;
			}
            
			say("updatePriorityThreadQueue(), found job id "+job.getId());
			synchronized (job) {
				State state = job.getState();
				if(state != State.PRIORITYTQUEUED) {
					synchronized(priorityThreadQueue) {
						// someone has canceled the job or
						// its lifetime has expired
						esay("updatePriorityThreadQueue() : found a job in priority thread queue with a state different from PRIORITYTQUEUED, job id="+
						     job.getId()+" state="+job.getState());
						priorityThreadQueue.remove(job);
						continue;
					}
				}
			}
			try {
				say("updatePriorityThreadQueue ()  executing job id="+job.getId());
				JobWrapper wrapper = new JobWrapper(job);
				pooledExecutor.execute(wrapper);
				say("updatePriorityThreadQueue() waiting startup");
				wrapper.waitStartup();
				say("updatePriorityThreadQueue() job started");
				/** let the stateChanged() always remove the jobs from the queue
				 */
				// the job is running in a separate thread by this time
			}
			// we set an aboort when blocked mode to the pooled executer
			// so the InterruptedException will be thrown when it can not
			// execute a job right away
			catch(RuntimeException re) {
				say("updatePriorityThreadQueue() cannot execute job id="+job.getId()+" at this time");
				return;
			}
			catch(InterruptedException ie) {
				say("updatePriorityThreadQueue() cannot execute job id="+job.getId()+" at this time");
				return;
			}
		}
	}
	
	// this is supposed to be the only place that removes the jobs from
	private void updateThreadQueue() throws java.sql.SQLException {
       
        while(true) {
            Job job = null;
            if(useFairness) {
                //say("updateThreadQueue(), using ValueCalculator to find next job");
                ModifiableQueue.ValueCalculator calc =
                new ModifiableQueue.ValueCalculator() {
                    public int calculateValue(
                    int queueLength,
                    int queuePosition,
                    Job job) {
                        
                        int numOfTQueuedBySameCreator =
                        getTQueuedByCreator(job);
                        int numOfRunningBySameCreator =
                        getRunningStateByCreator(job)+
                        getRunningWithoutThreadStateByCreator(job);
			int value = jobAppraiser.evaluateJobPriority(
				queueLength, queuePosition,
				numOfRunningBySameCreator,
				maxRunningByOwner,
				job);
                        //say("updateThreadQueue calculateValue return value="+value+" for "+o);
                        return value;
                    }
                };
                job = threadQueue.getGreatestValueObject(calc);
            }
            
            if(job == null) {
                 //say("updateThreadQueue(), job is null, trying threadQueue.peek();");
                 job = threadQueue.peek();
            }
            
            if(job == null) {
                //say("updateThreadQueue no jobs were found, breaking the update loop");
                break;
            }
            //we consider running and runningWithoutThreadStateJobsNum as occupying slots in the 
            // thread pool, even if the runningWithoutThreadState jobs are not actually running, 
            // but waiting for the notifications
            if(getTotalRunningThreads() + runningWithoutThreadStateJobsNum >threadPoolSize) {
                break;
            }
           say("updateThreadQueue(), found job id "+job.getId());

            synchronized(job)
            {
                State state = job.getState();
                if(state != State.TQUEUED ) {
                    synchronized(threadQueue)
                    {
                        // someone has canceled the job or
                        // its lifetime has expired
                        esay("updateThreadQueue() : found a job in thread queue with a state different from TQUEUED, job id="+
                            job.getId()+" state="+job.getState());
                        threadQueue.remove(job);
                        continue;
                    }
                }
            }
            
            try {
                say("updateThreadQueue() executing job id="+job.getId());
                JobWrapper wrapper = new JobWrapper(job);
                pooledExecutor.execute(wrapper);
                say("updateThreadQueue() waiting startup");
                wrapper.waitStartup();
                say("updateThreadQueue() job started");
                    /*
                     * let the stateChanged() always remove the jobs from the queue
                     */
            }
            // we set an aboort when blocked mode to the pooled executer
            // so the InterruptedException will be thrown when it can not
            // execute a job right away
            catch(InterruptedException ie) {
                esay("updateThreadQueue() cannot execute job id="+job.getId()+" at this time");
                return;
            }
            catch(RuntimeException re) {
                esay("updateThreadQueue() cannot execute job id="+job.getId()+" at this time");
                return;
            }
            
        }
    }
    
    private void updateReadyQueue()  throws java.sql.SQLException{
        while(true) {
            
            if(readyJobsNum >= maxReadyJobs) {
                // cann't add any more jobs to ready state
                return;
            }
            Job job = null;
            if(useFairness) {
                //say("updateReadyQueue(), using ValueCalculator to find next job");
                ModifiableQueue.ValueCalculator calc =
                new ModifiableQueue.ValueCalculator() {
                    public int calculateValue(
                    int queueLength,
                    int queuePosition,
                    Job job) {
                        int numOfRQueuedBySameCreator =
                        getRQueuedByCreator(job);
                        int numOfReadyBySameCreator =
                        getReadyByCreator(job);
			int value = jobAppraiser.evaluateJobPriority(
				queueLength, queuePosition,
				numOfReadyBySameCreator,
				maxReadyJobs,
				job);

                        // say("updateReadyQueue calculateValue return value="+value+" for "+o);

                        return value;
                    }
                };
                job = readyQueue.getGreatestValueObject(calc);
            }
            if(job == null) {
                 //say("updateReadyQueue(), job is null, trying readyQueue.peek();");
                job = readyQueue.peek();
            }

            if(job == null) {
                // no more jobs to add to the ready state set
                //say("updateReadyQueue no jobs were found, breaking the update loop");
                    return;
            }
            
            say("updateReadyQueue(), found job id "+job.getId());
            synchronized(job)
            {
                /*
                 ** let the stateChanged() always remove the jobs from the queue
                 */
                State state = job.getState();
                if(state != State.RQUEUED ) {
                    esay("updateReadyQueue() : found a job in ready queue with a state different from RQUEUED, job id="+
                        job.getId()+" state="+job.getState());
                    readyQueue.remove(job);
                }
                try {
                    job.setState(State.READY,"execution succeeded");

                }
                catch(IllegalStateTransition ist) {
                    //nothing we can do here
                }
            }            
        }
    }
    
    private boolean notified;
    
    public void run() {
        while(true) {
            try {
                
                synchronized(this) {
                    if(!notified) {
                        //say("Scheduler(id="+getId()+").run() waiting for events...");
                        wait(queuesUpdateMaxWait);
                    }
                    notified =false;
                }
                //say("Scheduler(id="+getId()+").run() updating Priority Thread queue...");
                updatePriorityThreadQueue();
                //say("Scheduler(id="+getId()+").run() updating Thread queue...");
                updateThreadQueue();
                //say("Scheduler(id="+getId()+").run() updating Ready queue...");
                updateReadyQueue();
                //say("Scheduler(id="+getId()+").run() done updating queues");
                
            }
            catch(InterruptedException ie) {
                esay(ie);
                esay("Sheduler(id="+getId()+") terminating update thread, since it caught an InterruptedException !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                
            }
            catch( java.sql.SQLException sqle) {
                esay(sqle);
            }
            catch(Throwable t)
            {
                esay("Sheduler(id="+getId()+") update thread caught an exception !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                esay("Sheduler(id="+getId()+") update thread caught an exception !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                esay("Sheduler(id="+getId()+") update thread caught an exception !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
                esay(t);
            }
        }
        //we are interrupted,
        //terminating thread
        
    }
    
    private class JobWrapper implements Runnable {
        Job job;
        boolean started = false;
        public JobWrapper(Job job) {
            this.job = job;
            
        }
        
        public synchronized void waitStartup() throws InterruptedException{
            for(int i=0;i<10;++i) {
                if(started ) return;
                this.wait(1000);
            }
        }
        private synchronized void started()
        {
            started = true;
            notifyAll();
        }
        
        public void run() {
            try {
                increaseNumberOfRunningThreads(job);
                State state;
                say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") entering sync(job) block" );
                synchronized(job)
                {
                    say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") entered sync(job) block" );
                    state =job.getState();
                    say("Scheduler(id="+getId()+") JobWrapper run() running job with id="+job.getId()+" in state="+state);
                    if(state == State.CANCELED ||
                    state == State.FAILED) {
                    say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") returning" );
                        return;

                    }
                    if(state == State.PENDING ||
                    state == State.TQUEUED ||
                    state == State.PRIORITYTQUEUED ) {
                        try {
                             say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") changing job state to runinng");
                            job.setState(State.RUNNING," executing ",false);
                            started();
                            job.saveJob();
                        }
                        catch( IllegalStateTransition ist) {
                            esay(ist.toString());
                            return;
                        }
                    }
                    else if(state == State.ASYNCWAIT ||
                    state == State.RETRYWAIT ) {
                            try {
                             say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") changing job state to runinng");
                                job.setState(State.RUNNING," executing ",false);
                                started();
                                job.saveJob();
                            }
                            catch( IllegalStateTransition ist) {
                                esay(ist.toString());
                                return;
                            }
                    }
                    else {
                        esay("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") job is in state "+state+"; can not execute, returning");
                        return;
                    }
                }                
            
                say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") exited sync block");
                Throwable t = null;
                try {
                   say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") calling job.run()");
                   job.run();
                   say("Scheduler(id="+getId()+") JobWrapper("+job.getId()+") job.run() returned");
                }
                catch(Throwable t1)
                {
                    esay(t1);
                    t = t1;
                }
                synchronized(job)
                {
                    // if no exception was thrown
                    if(t == null) {
                        state = job.getState();
                        if(state == State.DONE) {
                            return;
                        }

                        else if(state == State.RUNNING) {
                            try {
                                // put blocks if ready queue is full
                                job.setState(State.RQUEUED, "putting on a \"Ready\" Queue");
                                if(!readyQueue.offer(job,0))
                                {
                                       job.setState(State.FAILED,"All Ready slots are taken and Ready Thread Queue is full. Failing request");
                                }

                            }
                            catch(InterruptedException ie) {
                                try {
                                    job.setState(State.FAILED,"InterruptedException while putting on the ready queue");
                                }
                                catch( IllegalStateTransition ist) {
                                    esay(ist.toString());
                                    return;
                                }
                            }
                            catch( IllegalStateTransition ist) {
                                esay(ist.toString());
                                return;
                            }
                        }
                    
                    } else {

                        if(t instanceof NonFatalJobFailure) {

                            NonFatalJobFailure failure = (NonFatalJobFailure)t;
                            if(job.getNumberOfRetries() < maxNumberOfRetries &&
                            job.getNumberOfRetries() < job.getMaxNumberOfRetries()) {
                                try {
                                        job.setState(State.RETRYWAIT,
                                        " nonfatal error ["+t.toString()+"] retrying");
                                }
                                catch( IllegalStateTransition ist) {
                                    esay(ist.toString());
                                    return;
                                }
                                Scheduler.this.startRetryTimer(job);
                                return;
                            }
                            else {
                                try {
                                    job.setState(State.FAILED,
                                    "number of retries exceeded: "+failure.toString());
                                }
                                catch( IllegalStateTransition ist) {
                                    esay(ist.toString());
                                    return;
                                }
                                return;
                            }
                        }
                        else if(t instanceof FatalJobFailure) {
                            FatalJobFailure failure = (FatalJobFailure)t;

                            try {
                                job.setState(State.FAILED,
                                "non retriable error"+failure.toString());

                            }
                            catch( IllegalStateTransition ist) {
                                esay(ist.toString());
                                return;
                            }
                            return;
                        }
                        else
                        {
                            try {

                                esay(t);
                                job.setState(State.FAILED,t.toString());

                            }
                            catch( IllegalStateTransition ist) {
                                esay(ist.toString());
                                return;
                            }
                            return;
                        }
                        

                    }
                }
            }
            catch(Throwable t) {
                esay(t);
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

    private void startRetryTimer(final Job job) {
        TimerTask task = new TimerTask() {
            public void run() {
                synchronized(job){ 
                    State s = job.getState();
                    if(s != State.RETRYWAIT)
                    {
                        esay("retryTimer expired, but job state is "+s);
                        return;
                    }

                    try {
                        job.setState(State.PRIORITYTQUEUED, "retrying job, putting on priority queue");
                        if(!priorityThreadQueue.offer(job,0))
                        {
                            job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                        }
                        //schedule(job);
                    }
                    catch(InterruptedException ie) {
                        synchronized (job) {
                            try {
                                job.setState(State.FAILED,
                                "scheduler is interrupted");
                            }
                            catch(IllegalStateTransition ist) {
                                esay(ist);
                            }
                        }
                    }
                    catch(IllegalStateTransition ist) {
                        synchronized(job) {
                            esay("can not retry:");
                            esay(ist);
                            try {
                                    job.setState(State.FAILED,
                                    "scheduler is interrupted");
                            }
                            catch(IllegalStateTransition ist1) {
                                esay(ist1);
                            }
                        }
                    }
                }
                
            }
        };
        job.setRetryTimer(task);
        retryTimer.schedule(task,retryTimeout);
    }
    
    
    public void propertyChange(java.beans.PropertyChangeEvent evt) {
        if(evt instanceof JobStorageAddedEvent) {
            JobStorageAddedEvent jobStorageAdded = (JobStorageAddedEvent)evt;
            JobStorage jobStorage = (JobStorage)jobStorageAdded.getSource();
            try {
                jobStorageAdded(jobStorage);
            }catch(java.sql.SQLException  sqle) {
                esay(sqle);
            }
        }
        else {
            esay("unknown type of event " +evt);
            return;
        }
        
    }
    
    public void jobStorageAdded( JobStorage jobStorage) throws java.sql.SQLException{
        say("Job Storage added:"+jobStorage);
        if(true) return;
        Set jobs = jobStorage.getJobs(this.id);
        for(Iterator i = jobs.iterator(); i.hasNext();) {
            Job job = (Job) i.next();
            if(job.getSchedulerTimeStamp() != timeStamp) {
                synchronized(job)
                {
                    try {
                        say("found a job belonging to this scheduler:");
                        say("job ="+job);
                        // this means that this is a job from one of the previous runs
                        // we want to put it in the current scheduler
                        State state = job.getState();
                        if(state == State.CANCELED ||
                        state == State.DONE ||
                        state == State.FAILED) {
                            continue;
                        }
                        
                        
                        if(state == State.RETRYWAIT ) {
                            increaseNumberOfRetryWait(job);
                        } else if(state == State.ASYNCWAIT ) {
                            increaseNumberOfAsyncWait(job);
                        } else if(state == State.RUNNING) {
                            increaseNumberOfRunningState(job);
                        } else if(state == State.RUNNINGWITHOUTTHREAD) {
                            increaseNumberOfRunningWithoutThreadState(job);
                        } else if(state == State.PRIORITYTQUEUED) {
                            if(!priorityThreadQueue.offer(job,0))
                            {
                                job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                            } else {
                                increaseNumberOfPriorityTQueued(job);
                            }
                        } else if(state == State.TQUEUED) {
                            increaseNumberOfTQueued(job);
                        } else if(state == State.RQUEUED) {
                            increaseNumberOfReadyQueued(job);
                        } else if(state == State.READY || state == State.TRANSFERRING) {
                            increaseNumberOfReady(job);
                        }
                        
                        if(restorePolicy == ON_RESTART_FAIL_REQUEST) {
                            
                            job.setState(State.FAILED,"FAIL_ON_RESTART policy is on");
                            continue;
                        }

                        
                        
                        if(restorePolicy == ON_RESTART_WAIT_FOR_UPDATE_REQUEST) {
                            job.setState(State.RESTORED,
                                "Restored job is put in RESTORED state,"+
                                " waiting for user update before continuing");
                            increaseNumberOfRestored(job);
                            continue;
                            
                        }
                        
                        if(state == State.PENDING) {
                            say("job is pending, scheduling");
                            schedule(job);
                            continue;
                        } 
                        
                        if(state == State.RETRYWAIT ) {
                            say("job is Retrywait, scheduling");
                            startRetryTimer(job);
                            continue;
                        }
                        
                        if(state == State.ASYNCWAIT ) {
                            say("job is Asysncwait");
                            // the notification will probably not come
                            // so set it to be retried
                            say("number of async wait is "+getTotalAsyncWait());
                            say("setting job state to RETRYWAIT, current number of retry wait is "+getTotalRetryWait());
                            job.setState(State.RETRYWAIT,"Restored job was in AsyncWait state, it is put in RetryWait state");
                            say("after set state value of asyncwait is "+getTotalAsyncWait()+ " number of retry wait is "+getTotalRetryWait());
                            say("starting RetryTimer");
                            startRetryTimer(job);
                            continue;
                        } 
                        
                        if(state == State.RUNNING) {
                            say("job was Running");
                            // the notification will probably not come
                            // so set it to be retried
                            say("setting job state to RETRYWAIT, starting RetryTimer");
                            job.setState(State.RETRYWAIT,"Restored job was in Running state, it is put in RetryWait state");
                            startRetryTimer(job);
                            continue;
                        } 
                        
                        if(state == State.RUNNINGWITHOUTTHREAD) {
                            say("job was RunningWithoutThread");
                            // the notification will probably not come
                            // so set it to be retried
                            say("setting job state to RETRYWAIT, starting RetryTimer");
                            job.setState(State.RETRYWAIT,"Restored job was in Running state, it is put in RetryWait state");
                            startRetryTimer(job);
                            continue;
                        } 
                        
                        if(state == State.PRIORITYTQUEUED) {

                            say("job state is  PRIORITYTQUEUED, putting in the priority queue");
                            // put blocks if priorityThreadQueue is full
                            // this will block the retry timer (or the event handler)
                            if(!priorityThreadQueue.offer(job,0))
                            {
                                job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                            }
                            continue;

                        } 
                        
                        if(state == State.TQUEUED) {

                            say("job state is  TQUEUED, putting in the thread queue");
                            // put blocks if priorityThreadQueue is full
                            // this will block the retry timer (or the event handler)
                            if(!threadQueue.offer(job,0))
                            {
                                job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                            }
                            continue;
                        }  
                        
                        if(state == State.RQUEUED) {
                            say("job state is  RQUEUED, putting in the ready queue");
                            // put blocks if ready queue is full
                            if(!readyQueue.offer(job,0))
                            {
                                job.setState(State.FAILED,"Priority Thread Queue is full. Failing request");
                            }
                            continue;
                        }
                        
                        if(state == State.READY || state == State.TRANSFERRING) {
                            say("job is  READY (or TRANSFERRING)");
                            continue;
                        }
                        
                        job.setScheduler(id,timeStamp);

                    }
                    catch(Exception e) {
                        esay(e);
                        esay(e.toString()+
                        "while re-scheduling  the saved job id= "+job.getId());
                        try {
                            job.setState(State.FAILED,
                            "Exception "+e.toString()+
                            " while re scheduling  the saved job ");
                        }
                        catch( IllegalStateTransition ist) {
                            esay(ist);
                        }
                    }
                }
            }
        }
    }
    
    public void stateChanged(Job job,State oldState,State newState)   {
        if(job == null ) {
            esay("stateChanged job is null!!!");
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
        else if(newState == State.READY) {
            increaseNumberOfReady(job);
        }
        else if(newState == State.ASYNCWAIT) {
            increaseNumberOfAsyncWait(job);
        }
        else if(newState == State.RETRYWAIT || newState == State.TRANSFERRING) {
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
        
        
        say("state changed for job id "+job.getId()+" from "+oldState+" to "+newState);
        if(newState == State.DONE ||
        newState == State.CANCELED ||
        newState == State.FAILED) {
            
            
            if(oldState == State.RETRYWAIT) {
                java.util.TimerTask task = job.getRetryTimer();
                if(task != null)
                 {
			task.cancel();
                 }
            }
        }
        
        
        /*
         *
         if(oldState == State.RUNNING ||
        oldState == State.READY ||
        oldState == State.TRANSFERRING) {
         */
        synchronized(this) {
            say("StateChanged notify");
            notifyAll();
            notified = true;
        }
        //}
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
    public void setJobAppraiser(org.dcache.srm.scheduler.policies.JobPriorityPolicyInterface  jobAppraiser) {
        this.jobAppraiser = jobAppraiser;
    }
    
    /** Getter for property id.
     * @return Value of property id.
     *
     */
    public java.lang.String getId() {
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
     * @param maxWaitingJobNum New value of property maxWaitingJobNum.
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
     * @param maxWaitingJobNum New value of property maxWaitingJobNum.
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
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer();
        getInfo(sb);
        return sb.toString();
    }
    
    public void getInfo(StringBuffer sb) {
        sb.append("Scheduler id="+id).append('\n');
        sb.append("          useFairness="+useFairness).append('\n');
        sb.append("          asyncWaitJobsNum="+getTotalAsyncWait()).append('\n');
        sb.append("          maxAsyncWaitJobsNum="+maxAsyncWaitJobs).append('\n');
        sb.append("          retryWaitJobsNum="+getTotalRetryWait()).append('\n');
        sb.append("          maxRetryWaitJobsNum="+maxRetryWaitJobs).append('\n');
        sb.append("          readyJobsNum="+getTotalReady()).append('\n');
        sb.append("          maxReadyJobs="+maxReadyJobs).append('\n');
        sb.append("          max number of jobs Running By the same owner="+maxRunningByOwner).append('\n');
        sb.append("          total number of jobs in Running State ="+getTotalRunningState()).append('\n');
        sb.append("          total number of jobs in RunningWithoutThread State ="+getTotalRunningWithoutThreadState()).append('\n');
        sb.append("          threadPoolSize="+threadPoolSize).append('\n');
        sb.append("          total number of threads running ="+getTotalRunningThreads()).append('\n');
        sb.append("          retryTimeout="+retryTimeout).append('\n');
        sb.append("          maxThreadQueueSize="+maxThreadQueueSize).append('\n');
        sb.append("          threadQueue size="+threadQueue.size()).append('\n');
        sb.append("          !!! threadQueued="+getTotalTQueued()).append('\n');
        sb.append("          priorityThreadQueue size="+priorityThreadQueue.size()).append('\n');
        sb.append("          !!! priorityThreadQueued="+getTotalPriorityTQueued()).append('\n');
        sb.append("          maxReadyQueueSize="+maxReadyQueueSize).append('\n');
        sb.append("          readyQueue size="+readyQueue.size()).append('\n');
        sb.append("          !!! readyQueued="+getTotalRQueued()).append('\n');
        sb.append("          maxNumberOfRetries="+maxNumberOfRetries).append('\n');
        sb.append("          number of restored but not scheduled=").append(restoredJobsNum).append('\n');
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
    
    public void printThreadQueue(StringBuffer sb) throws SQLException {
        sb.append("ThreadQueue :\n");
        threadQueue.printQueue(sb);
    }
    public void printPriorityThreadQueue(StringBuffer sb)  throws SQLException {
        sb.append("PriorityThreadQueue :\n");
        priorityThreadQueue.printQueue(sb);
    }
    public void printReadyQueue(StringBuffer sb)  throws SQLException {
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
			Class appraiserClass = Class.forName(className);
			jobAppraiser = (JobPriorityPolicyInterface)appraiserClass.newInstance();
		}
		catch (Exception e) { 
			esay("failed to load "+className);
			jobAppraiser = new DefaultJobAppraiser();
		}
	}

	public String getPriorityPolicyPlugin() { 
		return priorityPolicyPlugin;
	}
   
}

