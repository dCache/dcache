// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.23  2007/10/25 22:49:05  timur
// separate retry timers and lifetime timers
//
// Revision 1.22  2007/10/24 04:15:24  timur
// merging improvments from production branch for eliminating the duplicate key constrain violation error
//
// Revision 1.21  2007/08/03 20:20:40  timur
// implementing some of the findbug bugs and recommendations, avoid selfassignment, possible nullpointer exceptions, syncronization issues, etc
//
// Revision 1.20  2007/08/03 15:47:59  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.19  2007/06/18 21:39:23  timur
// shorter error message
//
// Revision 1.18  2007/03/08 23:36:56  timur
// merging changes from the 1-7 branch related to database performance and reduced usage of database when monitoring is not used
//
// Revision 1.17  2007/02/09 21:24:23  timur
// srmExtendFileLifeTime is about 70% done
//
// Revision 1.16  2007/01/06 00:23:56  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.15  2006/12/06 19:49:06  timur
// make History.getDescription() return string that does not contain ' symbol
//
// Revision 1.14  2006/09/11 22:28:04  timur
// reduce number of sql communications
//
// Revision 1.13  2006/06/13 21:54:32  timur
// synchronize on the history object
//
// Revision 1.12  2006/04/26 17:17:56  timur
// store the history of the state transitions in the database
//
// Revision 1.11  2006/04/21 22:58:27  timur
// we do not need a thread running when we start a remote transfer, but we want to control the number of the transfers, I hope the code accomplishes this now, though an addition of the new job state
//
// Revision 1.10  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.9  2006/04/12 23:16:24  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.8  2006/01/20 17:01:47  timur
// inserted missing return in expire job
//
// Revision 1.7  2005/12/07 16:24:06  timur
// append in setError, and log errors even when retrying
//
// Revision 1.6  2005/11/20 02:40:11  timur
// SRM PrepareToGet and srmStatusOfPrepareToGet functions
//
// Revision 1.5  2005/09/30 21:43:47  timur
// hope this will eliminate duplicate key error
//
// Revision 1.4  2005/05/04 21:54:52  timur
// new scheduling policy on restart for put and get request - do not schedule the request if the user does not update its status
//
// Revision 1.3  2005/03/30 22:42:11  timur
// more database schema changes
//
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:15  timur
// moving general srm code in a separate repository
//
// Revision 1.14  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.13  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.12  2004/11/01 20:41:16  timur
//  fixed the problem causing the exhaust of the jdbc connections
//
// Revision 1.11  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.10  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.9  2004/08/30 19:01:24  timur
// make sure that update thread does not continue, untill the job it schedules is removed from the queue
//
// Revision 1.8  2004/08/30 17:14:49  timur
// stop updating the status on the remote machine when the copy request is canceled, handle the queues more correctly
//
// Revision 1.7  2004/08/17 17:17:24  timur
// increment number of retries to avoid infinite retries
//
// Revision 1.6  2004/08/06 19:35:25  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.5.2.11  2004/07/29 22:17:30  timur
// Some functionality for disk srm is working
//
// Revision 1.5.2.10  2004/07/14 18:58:51  timur
// get/put requests status' are set to done when all file requests are done
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
// Revision 1.5.2.5  2004/06/23 21:56:02  timur
// Get Requests are now stored in database, Request Credentials are now stored in database too
//
// Revision 1.5.2.4  2004/06/22 01:38:07  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.5.2.3  2004/06/18 22:20:52  timur
// adding sql database storage for requests
//
// Revision 1.5.2.2  2004/06/16 19:44:33  timur
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
 * Job.java
 *
 * Created on March 22, 2004, 11:30 AM
 */

package org.dcache.srm.scheduler;
import java.util.concurrent.Semaphore;
import java.util.concurrent.CopyOnWriteArrayList;
import java.beans.PropertyChangeSupport;
import java.beans.PropertyChangeListener;
import java.util.Timer;
import java.util.TimerTask;
import java.util.HashMap;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.Map;
import java.lang.ref.WeakReference;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.sql.SQLException;
import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.util.JDC;
/**
 *
 * @author  timur
 */
public abstract class Job  {

    private static final org.apache.log4j.Logger _log =
        org.apache.log4j.Logger.getLogger(Job.class);

    private static final long DEFAULT_JOB_LIFETIME_MILLIS= 12*60*60*1000; //12 hours

    // this is the map from jobIds to jobs
    // job ids are referenced from jobs
    // jobs are wrapped into WeakReferences to prevent
    // creation hard references to jobIdsA

    private static final Map<Long, WeakReference<Job>> weakJobStorage =
            new WeakHashMap<Long, WeakReference<Job>>();

    private final Semaphore lock = new Semaphore(1);

    //this is used to build the queue of jobs.
    protected Long nextJobId;

    protected final Long id;

    private static PropertyChangeSupport jobsSupport = new PropertyChangeSupport(Job.class);

    private volatile State state = State.PENDING;
    protected StringBuffer errorMessage=new StringBuffer();

    protected int priority =0;
    protected String schedulerId;
    protected long schedulerTimeStamp;


    protected long creationTime = System.currentTimeMillis();

    protected long lifetime;

    protected int numberOfRetries = 0;
    protected int maxNumberOfRetries;
    private long lastStateTransitionTime = System.currentTimeMillis();

    private static final CopyOnWriteArrayList<JobStorage> jobStorages =
        new CopyOnWriteArrayList<JobStorage>();

    private final List<JobHistory> jobHistory = new ArrayList<JobHistory>();
    private transient JobIdGenerator generator;

    private TimerTask retryTimer;

    private static boolean storeInSharedMemoryCache =true;
    private static final SharedMemoryCache sharedMemoryCache =
            new SharedMemoryCache();

    public static final void registerJobStorage(JobStorage jobStorage) {
        synchronized(jobStorages) {
            jobStorages.add(jobStorage);
        }
        //jobsSupport.firePropertyChange(new JobStorageAddedEvent(jobStorage));
    }


    // this constructor is used for restoring the job from permanent storage
    // should be called through the Job.getJob only, otherwise the expireRestoredJobOrCreateExperationTimer
    // will never be called
    // we can not call it from the constructor, since this may lead to recursive job restoration
    // leading to the exhaust of the pool of database connections
    protected Job(Long id, Long nextJobId,JobStorage jobStorage, long creationTime,
    long lifetime,int stateId,String errorMessage,
    String schedulerId,
    long schedulerTimestamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray
    ) {
        if(jobStorage == null) {
            throw new NullPointerException(" job storage is null");
        }
        this.jobStorage = jobStorage;
        if(id == null) {
            throw new NullPointerException(" job id is null");
        }
        this.id = id;
        this.nextJobId = nextJobId;
        this.creationTime = creationTime;
        this.lifetime = lifetime;
        if(state == null) {
            throw new NullPointerException(" job state is null");
        }
        this.state = State.getState(stateId);
        this.errorMessage.append(errorMessage);
        this.schedulerId = schedulerId;
        this.schedulerTimeStamp = schedulerTimestamp;
        this.numberOfRetries = numberOfRetries;
        this.maxNumberOfRetries = maxNumberOfRetries;
        this.lastStateTransitionTime = lastStateTransitionTime;
        if(jobHistoryArray != null) {
            java.util.Arrays.sort(jobHistoryArray,new java.util.Comparator<JobHistory>(){
                 public int compare(JobHistory jobHistory1 , JobHistory jobHistory2) {
                     long  transitionTime1 = jobHistory1.getTransitionTime();
                     long  transitionTime2 = jobHistory2.getTransitionTime();
                     if(transitionTime1<transitionTime2) { return -1  ;}
                     if(transitionTime1==transitionTime2) { return 0  ;}
                     return 1;
                 } });
            for(int i = 0;i<jobHistoryArray.length;++i){
                jobHistory.add(jobHistoryArray[i]);
            }
        }
    }

    /**
     * NEED TO CALL THIS METHOD FROM THE CONCRETE SUBCLASS
     * RESTORE CONSTRUCTOR
     */
    private synchronized final void expireRestoredJobOrCreateExperationTimer()
    {
        if (state != State.CANCELED &&
            state != State.DONE &&
            state != State.FAILED) {
            long expiration_time = creationTime + lifetime;
            long new_lifetime = expiration_time - System.currentTimeMillis();

            /* We schedule a timer even if the job has already
             * expired.  This is to avoid a restore loop in which
             * expiring a job during restore causes the job to be read
             * recursively.
             */
            LifetimeExpiration.schedule(id, Math.max(0, new_lifetime));
        }
    }

    /** Creates a new instance of Job */

    public Job(long lifetime,
              JobStorage jobStorage,
              int maxNumberOfRetries) {

        if(jobStorage == null) {
            throw new NullPointerException(" job storage is null");
        }
        this.jobStorage = jobStorage;
        id = nextId();

        this.lifetime = lifetime;
        this.maxNumberOfRetries = maxNumberOfRetries;

        LifetimeExpiration.schedule(id, lifetime);
        synchronized (weakJobStorage) {
            weakJobStorage.put(id, new WeakReference<Job>(this));
        }
        jobHistory.add( new JobHistory(nextLong(),state,"created",lastStateTransitionTime));
    }

    protected void storeInSharedMemory () {
        sharedMemoryCache.updateSharedMemoryChache(this);
    }

    private  JobStorage jobStorage;

    private static final long serialVersionUID = 2690583464813886836L;

    // this method is called whenever the state of the job changes, or when the job's
    // place in queue changes, so the
    private boolean savedInFinalState = false;

	private JDC jdc;

    public void saveJob() {
        saveJob(false);
    }

    public void saveJob(boolean force)  {
        //  by making sure that the saving of the job in final state happens
        // only once
        // we hope to eliminate the dubplicate key error
        if(savedInFinalState){
            return;
        }
        try {
            boolean isFinalState = State.isFinalState(this.getState());
            jobStorage.saveJob(this, isFinalState || force);
            savedInFinalState = isFinalState;
        } catch(Throwable t) {
            // if saving fails we do not want to fail the request

            esay(t);

        }
    }

    public void say(String s) {
        _log.debug(" Job id="+id+" :"+ s);
        }

    public void esay(String s) {
        _log.error(" Job id="+id+" error :"+ s);
        }

    public void esay(Throwable t) {
        _log.error(" Job id="+id+" exception", t);
        }

    public static final Job getJob(Long jobId) throws SRMInvalidRequestException{
         return getJob ( jobId, null);
    }

    public static final Job getJob(Long jobId, Connection _con)
            throws SRMInvalidRequestException  {
        synchronized(weakJobStorage) {
            WeakReference<Job> ref = weakJobStorage.get(jobId);
            if(ref!= null) {
                Job o1 = ref.get();
                if(o1 != null) {
                    return o1;
                }
            }
        }

        Job job = null;

        //
        // This will allow to retrieve a job put in
        // a shared cache by a different instance of SRM
        // in a cluster
        job = sharedMemoryCache.getJob(jobId);

        boolean restoredFromDb=false;
        if (job == null) {
            for (JobStorage storage: jobStorages) {
                try {
                    if(_con == null) {
                        job = storage.getJob(jobId);
                    } else {
                        job = storage.getJob(jobId, _con);
                    }
                } catch (SQLException e){
                    _log.error("Failed to read job", e);
                }
                if(job != null) {
                    restoredFromDb = true;
                    break;
                }
            }
        }
        //since we do not synchronize  on the jobStorages or the job class
        // in this method, some other thread could have got to the same point, and created
        // an instance of the job for the same job id
        // but we always want the same instance to be available to ewveryone
        // only one of them  should win in storing his object in weakJobStorage
        // the object stored by the winner is the one we want to return
        //System.out.println("checking weakJobStorage");
        synchronized(weakJobStorage) {
            WeakReference<Job> ref = weakJobStorage.get(jobId);
            if(ref!= null) {
                Job o1 = ref.get();
                if(o1 != null) {
                //System.out.println("found job in weakJobStorage, returning");
                    return o1;
                }
            }

            if (job != null)
            {
                //System.out.println("storring job in weakJobStorage, ");
                weakJobStorage.put(job.id,new WeakReference<Job>(job));
                sharedMemoryCache.updateSharedMemoryChache(job);
            }
        }

        if(job != null && restoredFromDb) {
            //System.out.println("calling job.expireRestoredJobOrCreateExperationTimer();" );
            job.expireRestoredJobOrCreateExperationTimer();
            //System.out.println("returning job ");
            return job;
        }
        throw new SRMInvalidRequestException("jobId = "+jobId+" does not correspond to any known job");
    }


    public Job(JobStorage jobStorage,
        int maxNumberOfRetries) {
        this(DEFAULT_JOB_LIFETIME_MILLIS,jobStorage,maxNumberOfRetries);

    }


    public static void addClassStateChangeListener(PropertyChangeListener listener) {
        jobsSupport.addPropertyChangeListener(listener);
    }

    /** Performs state transition checking the legality first.
     * @param state
     */
    public State getState() {
        return state;
    }

    /**this is not thread safe, whoever calls this, should synchronize on the job
    */
    public void setState(State state,String description) throws
    IllegalStateTransition {
        setState(state,description,true);

    }
    //if save is false
    // we do not save state in database
    // the caller of the save state
    // needs to call save then
    //this is not thread safe, whoever calls this, should synchronize on the job
    public void setState(State state,String description, boolean save) throws
    IllegalStateTransition {
        State old;
            if(state == this.state) {
                return;
            }

            if(this.state == State.PENDING) {
                if(
                state != State.DONE &&
                state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RUNNING &&
                state != State.TQUEUED &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "a illegal state transition from "+this.state+" to "+state);
                }

            }
            if(this.state == State.TQUEUED) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RUNNING &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "b illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.PRIORITYTQUEUED) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RUNNING &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "b illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.RUNNING) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RETRYWAIT &&
                state != State.ASYNCWAIT &&
                state != State.RQUEUED &&
                state != State.READY &&
                state != State.DONE &&
                state != State.RUNNINGWITHOUTTHREAD &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "c illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.ASYNCWAIT) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RUNNING &&
                state != State.PRIORITYTQUEUED &&
                state != State.DONE &&
                // this is added to support the reading of the job from the database
                // the jon in database in asyncwait state will be put in retrywait state
                // since the notification will likely not come
                state != State.RETRYWAIT &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "z illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.RETRYWAIT) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.RUNNING &&
                state != State.PRIORITYTQUEUED &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "d illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.RQUEUED) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.READY &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "l illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.READY) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.TRANSFERRING &&
                state != State.DONE &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "e illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.TRANSFERRING) {
                if(state != State.CANCELED &&
                state != State.FAILED &&
                state != State.DONE &&
                state != State.RESTORED ) {
                    throw new IllegalStateTransition(
                    "f illegal state transition from "+this.state+" to "+state);
                }
            }
            if(this.state == State.FAILED ||
            this.state == State.DONE ||
            this.state == State.CANCELED) {
                throw new IllegalStateTransition(
                "g illegal state transition from "+this.state+" to "+state);
            }
            old = this.state;
            this.state = state;
            lastStateTransitionTime = System.currentTimeMillis();

            synchronized(jobHistory) {
                jobHistory.add( new JobHistory(nextLong(),state,description,lastStateTransitionTime));
            }

            if( errorMessage.length()== 0) {
                  errorMessage.append(description);
            } else {
                 errorMessage.append("\nat ");
                 errorMessage.append(new java.util.Date(lastStateTransitionTime));
                 errorMessage.append(" appended:\n");
                 errorMessage.append(description);
            }

        if(state == State.RETRYWAIT)
        {
            inclreaseNumberOfRetries();
        }

        if(schedulerId != null) {
            Scheduler scheduler =   Scheduler.getScheduler(schedulerId);
            if(scheduler != null) {
                scheduler.stateChanged(this, old, state);
            }
        }

        if(state == State.FAILED ||
        state == State.DONE ||
        state == State.CANCELED) {
            LifetimeExpiration.cancel(id);
            schedulerId = null;
        }
        else {
            if(schedulerId == null) {
                throw new IllegalStateTransition("Scheduler ID is null");
            }

        }
        if(!old.isFinalState() && state.isFinalState()) {
            sharedMemoryCache.updateSharedMemoryChache(this);
        }

        stateChanged(old);
        if(save) {
            saveJob();
        }
    }

    public void tryToReady() {
      if(schedulerId != null) {
            Scheduler scheduler =   Scheduler.getScheduler(schedulerId);
            if(scheduler != null) {
                scheduler.tryToReadyJob(this);
            }
        }
    }

    /** Getter for property errorMessage.
     * @return Value of property errorMessage.
     *
     */

    public String getErrorMessage() {

        StringBuffer errorsb = new StringBuffer();
        synchronized(jobHistory) {
            if(!jobHistory.isEmpty()) {
                JobHistory nextHistoryElement = jobHistory.get(jobHistory.size() -1);
                State nexthistoryElState = nextHistoryElement.getState();
                errorsb.append(" at ");
                errorsb.append(new java.util.Date(nextHistoryElement.getTransitionTime()));
                errorsb.append(" state ").append(nexthistoryElState);
                errorsb.append(" : ");
                errorsb.append(nextHistoryElement.getDescription());
            }
                /*
                for( Iterator i = jobHistory.iterator(); i.hasNext();) {
                    JobHistory nextHistoryElement = (JobHistory)i.next();
                    State nexthistoryElState = nextHistoryElement.getState();
                    if(nexthistoryElState == State.FAILED ||
                    nexthistoryElState == State.CANCELED ||
                    nexthistoryElState == State.RETRYWAIT ||
                    nexthistoryElState == State.DONE  ) {
                     errorMessage.append(" at ");
                     errorMessage.append(new java.util.Date(nextHistoryElement.getTransitionTime()));
                     errorMessage.append(" state ").append(nexthistoryElState);
                     errorMessage.append(" : ");
                     errorMessage.append(nextHistoryElement.getDescription());
                     errorMessage.append('\n');
                    }
                }*/
       }
       return errorsb.toString();
    }

    public void addHistoryEvent(String description){
        synchronized(jobHistory) {
            jobHistory.add( new JobHistory(nextLong(),state,description, System.currentTimeMillis()));
        }

    }
     public String getHistory() {
        StringBuffer historyString = new StringBuffer();
        synchronized(jobHistory) {
            for( JobHistory nextHistoryElement: jobHistory ) {
                 historyString.append(" at ");
                 historyString.append(new java.util.Date(nextHistoryElement.getTransitionTime()));
                 historyString.append(" state ").append(nextHistoryElement.getState());
                 historyString.append(" : ");
                 historyString.append(nextHistoryElement.getDescription());
                 historyString.append('\n');
            }
        }
       return historyString.toString();
    }


     public Iterator getHistoryIterator() {

        synchronized(jobHistory) {
            return new ArrayList(jobHistory).iterator();
        }
    }

    public abstract void run() throws NonFatalJobFailure, FatalJobFailure;

    //implementation should not block in this method
    // this method should make sure that the job is saved in the
    // job's storage (instance of Jon.JobStorage (possibly in a database )
    protected abstract void stateChanged(State oldState);



    /** Getter for property numberOfRetries.
     * @return Value of property numberOfRetries.
     *
     */
    public final int getNumberOfRetries() {
        return numberOfRetries;
    }

    /** Setter for property numberOfRetries.
     * @param numberOfRetries New value of property numberOfRetries.
     *
     */
    private final void inclreaseNumberOfRetries() {
        numberOfRetries++;
    }

    /** Getter for property retry_timer.
     * @return Value of property retry_timer.
     *
     */
    java.util.TimerTask getRetryTimer() {
        return retryTimer;
    }


    /** Getter for property creator.
     * @return Value of property creator.
     *
     */
    public abstract String getSubmitterId();

    /** Getter for property priority.
     * @return Value of property priority.
     *
     */
    public int getPriority() {
        return priority;
    }

    /** Setter for property priority.
     * @param priority New value of property priority.
     *
     */
    public void setPriority(int priority) {
        if(priority <0) {
            throw new IllegalArgumentException(
            "priority should be greater than or equal to zero");
        }
        this.priority = priority;
    }

    public String toString() {
        return "Job ID="+id+" state="+state+
        " created on "+
        (new java.util.Date(creationTime)).toString()+
        " by ["+getSubmitterId()+"]";
    }

    /** Getter for property id.
     * @return Value of property id.
     *
     */
    public Long getId() {
        // never let the reference to actual id to escape
        return new Long(id.longValue());
    }
    /** Getter for property nextJobId.
     * @return Value of property nextJobId.
     *
     */
    public Long getNextJobId() {
        return nextJobId;
    }

    /** Setter for property nextJobId.
     * @param nextJobId New value of property nextJobId.
     *
     */
    public void setNextJobId(Long nextJobId) {
        this.nextJobId = nextJobId;
        saveJob();
    }

    /** Getter for property schedulerId.
     * @return Value of property schedulerId.
     *
     */
    public String getSchedulerId() {
        return schedulerId;
    }

    /** Setter for property schedulerId.
     * @param schedulerId New value of property schedulerId.
     *
     */
    public void setScheduler(String schedulerId,long schedulerTimeStamp) {
        //  check if the values have indeed changed
        // If they are the same, we do not need to do anythign.
        if(this.schedulerTimeStamp != schedulerTimeStamp ||
           this.schedulerId != null && schedulerId == null ||
           schedulerId != null && !schedulerId.equals(this.schedulerId)) {

            this.schedulerTimeStamp = schedulerTimeStamp;
            this.schedulerId = schedulerId;

            // we need to save job every time the scheduler is set
            // even if the jbbc monitoring log is disabled,
            // as we use scheduler id to identify who this job belongs to.
            try {
                this.jobStorage.saveJob(this,true);
            }catch (java.sql.SQLException sqle) {
                esay(sqle);
            }
        }
    }

    /** Getter for property schedulerTimeStamp.
     * @return Value of property schedulerTimeStamp.
     *
     */
    public long getSchedulerTimeStamp() {
        return schedulerTimeStamp;
    }

    public synchronized long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        if(State.isFinalState(state)){
            if(state == State.CANCELED) {
                throw new SRMAbortedException("can't extend lifetime, job was aborted");
            } else if(state == State.DONE) {
                throw new SRMReleasedException("can't extend lifetime, job has finished");
            } else {
                throw new SRMException("can't extend lifetime, job state is "+state);
            }
        }

        long remainingLifetime = getRemainingLifetime();
        if(remainingLifetime >=newLifetimeInMillis) {
            return remainingLifetime;
        }

        if (!LifetimeExpiration.cancel(id)) {
            throw new SRMException (" job expiration has started already ");
        }
        LifetimeExpiration.schedule(id, newLifetimeInMillis);

        return 0;
    }

    /**
     * @return the generator
     */
    private JobIdGenerator getGenerator() {
        if(generator == null) {
              generator = JobIdGeneratorFactory.getJobIdGeneratorFactory().getJobIdGenerator();
        }
        return generator;
    }

    /**we use long values internally
     *but to remain complaint with srm v1
     * we use the generator's next id method
     * which returns longs limited to int range
     * @return next Long id
     */
    private Long nextId() {
        return getGenerator().getNextId();
    }

    /**
     *
     * @return next long value
     */
    private long nextLong() {
        return getGenerator().nextLong();
    }




    private static class LifetimeExpiration extends TimerTask
    {
        static private Map<Long,LifetimeExpiration> _instances =
            new HashMap<Long,LifetimeExpiration>();

        static private Timer _timer = new Timer();

        private Long _id;

        static synchronized public void schedule(Long id, long time)
        {
            if (!_instances.containsKey(id)) {
                LifetimeExpiration task = new LifetimeExpiration(id);
                _instances.put(id, task);
                _timer.schedule(task, time);
            }
        }

        static synchronized public boolean contains(Long id)
        {
            return _instances.containsKey(id);
        }

        static synchronized public boolean cancel(Long id)
        {
            LifetimeExpiration task = _instances.remove(id);
            if (task == null) {
                return false;
            } else {
                return task.cancel();
            }
        }

        static synchronized private void remove(Long id)
        {
            _instances.remove(id);
        }

        private LifetimeExpiration(Long id)
        {
            _id = id;
        }

        public void run()
        {
            remove(_id);
            try {
                expireJob(Job.getJob(_id));
            } catch (IllegalArgumentException e) {
                // Job is already gone
            } catch (Exception e) {
                _log.error("Unexpected exception during job timeout", e);
            }
        }
    }

    public static final void expireJob(Job job) {
        try {
            synchronized(job)
            {
                job.say("expiring job id="+job.getId());
                if(job.state == State.READY ||
                job.state == State.TRANSFERRING) {
                    job.setState(State.DONE,"lifetime expired");
                    return;
                }
                if(State.isFinalState(job.state)) {
                    return;
                }
                job.setState(State.FAILED,"lifetime expired");
            }
        }
        catch(IllegalStateTransition ist) {
            return;
        }
    }

    /** Getter for property maxNumberOfRetries.
     * @return Value of property maxNumberOfRetries.
     *
     */
    public int getMaxNumberOfRetries() {
        return maxNumberOfRetries;
    }

    /** Setter for property maxNumberOfRetries.
     * @param maxNumberOfRetries New value of property maxNumberOfRetries.
     *
     */
    public void setMaxNumberOfRetries(int maxNumberOfRetries) {
        this.maxNumberOfRetries = maxNumberOfRetries;
    }

    /**
     * Getter for property creationTime.
     * @return Value of property creationTime.
     */
    public long getCreationTime() {
        return creationTime;
    }

    /**
     * Getter for property lifetime.
     * @return Value of property lifetime.
     */
    public long getLifetime() {
        return lifetime;
    }

    public long getRemainingLifetime() {
        if(State.isFinalState(this.state)) {
            return 0;
        }
        long remianingLifetime = creationTime+
                lifetime - System.currentTimeMillis();
        return remianingLifetime >0?remianingLifetime:0;
    }

    /**
     * if the job that has been scheduled for execution at some point in the past\
     * and then was restored and put in the restored state
     * then this method will triger rescheduling of this job
     *
     * This is needed for the types of the jobs that do not need to be resheduled
     * unless something else triggers the rescheduling (like clients updating the status
     * of the job, thus confirming their existance).
     */
    public void scheduleIfRestored() {
         if(getState() == State.RESTORED) {
                if(schedulerId != null) {
                    Scheduler scheduler = Scheduler.getScheduler(schedulerId);
                    if(scheduler != null) {
                        try
                        {
                            scheduler.schedule(this);
                        }
                        catch(Exception ie) {
                            ie.printStackTrace();
                        }
                    }
                }
            }


    }


    public long getLastStateTransitionTime(){
        return lastStateTransitionTime;
    }

    public Semaphore getLock() {
          return lock;
    }

    public static class JobHistory implements java.lang.Comparable<JobHistory> {
        private long id;
        private State state;
        private long transitionTime;
        private String description;
        private boolean saved; //false by default
        public JobHistory(long id, State state, String description, long transitionTime) {
            this.id = id;
            this.state = state;
            this.description = description;
            this.transitionTime = transitionTime;
        }

        /**
         * Getter for property state.
         * @return Value of property state.
         */
        public org.dcache.srm.scheduler.State getState() {
            return state;
        }

        /**
         * Getter for property id.
         * @return Value of property id.
         */
        public long getId() {
            return id;
        }

       /**
         * Getter for property transitionTime.
         * @return Value of property transitionTime.
         */
        public long getTransitionTime() {
            return transitionTime;
        }

        /**
         * Getter for property description.
         * @return Value of property description.
         */
        public String getDescription() {
            if(description.indexOf('\'') != -1 ) {
                description = description.replace('\'','`');
            }
            return description;
        }

        public int compareTo(JobHistory o)  {

            long oTransitionTime = o.getTransitionTime();
            return transitionTime < oTransitionTime?
                    -1:
                    (transitionTime == oTransitionTime? 0: 1);
        }

        public boolean equals(Object o) {
            if(o == null || !(o instanceof JobHistory)) {
                return false;
            }
            JobHistory jobHistory = (JobHistory) o;
            return jobHistory.id == id;
        }

   /**
     * Returns a hash code for this <code>Long</code>. The result is
     * the exclusive OR of the two halves of the primitive
     * <code>long</code> id of this <code>JobHistory</code>
     * object. That is, the hashcode is the value of the expression:
     * <blockquote><pre>
     * (int)(this.getId()^(this.getId()&gt;&gt;&gt;32))
     * </pre></blockquote>
     *
     * implementation is based on <code>Long</code> implementation of
     * <code>hashCode()</code>
     * @return  a hash code value for this object.
     */
        public int hashCode() {
            return (int)(id ^ (id >>> 32));
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("JobHistory[");
            sb.append(new java.util.Date(transitionTime)).append(',');
            sb.append(state).append(',');
            sb.append(description).append(']');
            return sb.toString();
        }

        public boolean isSaved() {
            return saved;
        }

        public void setSaved() {
            this.saved = true;
        }

    }

    public void setRetryTimer(TimerTask retryTimer) {
        this.retryTimer = retryTimer;
    }

	public JDC getJdc() {
        return this.jdc;
	}

	public void setJdc(JDC jdc) {
        this.jdc = jdc;
	}


    /**
     * This is the initial call to schedule the job for execution
     */
    public synchronized void schedule() throws InterruptedException,IllegalStateTransition
    {
            if(!State.PENDING.equals(state)) {
                throw new IllegalStateException("State is not pending");
            }
            Scheduler scheduler = SchedulerFactory.getSchedulerFactory().getScheduler(this);
            this.setScheduler(scheduler.getId(), 0);
            scheduler.schedule(this);
    }



    public static Set<Job> getActiveJobs(Class<? extends Job> type) {
        return sharedMemoryCache.getJobs(type);
    }

}
