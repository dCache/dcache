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

package org.dcache.srm.request;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobIdGenerator;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.SchedulerFactory;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.JDC;


/**
 * The base class for all scheduled activity within SRM.  An instance of this
 * class represents either a complete SRM operation (Request), or an individual
 * file within an operation (FileRequest).
 */
public abstract class Job  {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    private static final String ISO8601_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXX";

    //this is used to build the queue of jobs.
    protected Long nextJobId;

    protected final long id;

    private volatile State state = State.PENDING;
    protected StringBuilder errorMessage=new StringBuilder();

    protected int priority;
    protected String schedulerId;
    protected long schedulerTimeStamp;


    protected final long creationTime;

    protected long lifetime;

    protected int numberOfRetries;
    protected int maxNumberOfRetries;
    private long lastStateTransitionTime = System.currentTimeMillis();

    private final List<JobHistory> jobHistory = new ArrayList<>();
    private transient JobIdGenerator generator;

    private transient TimerTask retryTimer;

    private transient boolean savedInFinalState;

    protected transient JDC jdc;

    private final ReentrantReadWriteLock reentrantReadWriteLock =
            new ReentrantReadWriteLock();
    private final WriteLock writeLock = reentrantReadWriteLock.writeLock();

    // this constructor is used for restoring the job from permanent storage
    // should be called through the Job.getJob only, otherwise the expireRestoredJobOrCreateExperationTimer
    // will never be called
    // we can not call it from the constructor, since this may lead to recursive job restoration
    // leading to the exhaust of the pool of database connections
    protected Job(long id, Long nextJobId, long creationTime,
    long lifetime,int stateId,String errorMessage,
    String schedulerId,
    long schedulerTimestamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray
    ) {
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
        this.jdc = new JDC();
        if(jobHistoryArray != null) {
            Arrays.sort(jobHistoryArray,new Comparator<JobHistory>(){
                 @Override
                 public int compare(JobHistory jobHistory1 , JobHistory jobHistory2) {
                     long  transitionTime1 = jobHistory1.getTransitionTime();
                     long  transitionTime2 = jobHistory2.getTransitionTime();
                     if(transitionTime1<transitionTime2) { return -1  ;}
                     if(transitionTime1==transitionTime2) { return 0  ;}
                     return 1;
                 } });
            Collections.addAll(jobHistory, jobHistoryArray);
        }
    }

    /** Creates a new instance of Job */

    public Job(long lifetime,
              int maxNumberOfRetries) {

        id = nextId();
        creationTime = System.currentTimeMillis();
        this.lifetime = lifetime;
        this.maxNumberOfRetries = maxNumberOfRetries;
        this.jdc = new JDC();
        jobHistory.add(new JobHistory(nextLong(), state, "created", lastStateTransitionTime));
    }

    protected JobStorage<Job> getJobStorage() {
        return JobStorageFactory.getJobStorageFactory().getJobStorage(this);
    }


    public void saveJob() {
        saveJob(false);
    }

    public void saveJob(boolean force)  {
        wlock();
        try {
            //  by making sure that the saving of the job in final state happens
            // only once
            // we hope to eliminate the dubplicate key error
            if(savedInFinalState){
                return;
            }
            boolean isFinalState = this.getState().isFinalState();
            getJobStorage().saveJob(this, isFinalState || force);
            savedInFinalState = isFinalState;
        } catch (SQLException e) {
            // if saving fails we do not want to fail the request
            logger.error("Failed to save SQL request to database: {}", e.toString());
        } catch (RuntimeException e) {
            // if saving fails we do not want to fail the request
            logger.error("Failed to save SQL request to database. Please report to support@dcache.org.", e);
        } finally {
            wunlock();
       }
    }

    /**
     * Return the Job (or subclass thereof).  This may involve retrieving the
     * information from an external storage.  If the job cannot be found then
     * SRMInvalidRequestException is thrown.
     * <p>
     * The returned type is determined by the type parameter and must be Job
     * or a subclass of Job.  If the job doesn't have the right type then
     * SRMInvalidRequestException is thrown.
     * @param id which job to fetch.
     * @param type the desired class to represent this job
     * @return the requested class for the job with requested id
     * @throws SRMInvalidRequestException if the job cannot be found or if
     * the job has the wrong type.
     */
    public static final <T extends Job> T getJob(long id, Class<T> type)
            throws SRMInvalidRequestException
    {
        return getJob(id, type, null);
    }

    /**
     * Fetch the Job associated with the supplied ID.  If no such job exists
     * then throw SRMInvalidRequestException.  This method never returns null.
     * @param jobId the ID of the Job
     * @param _con either a valid database Connection or null
     * @return a object representing this job.
     * @throws SRMInvalidRequestException if the job cannot be found
     */
    public static <T extends Job> T getJob(long jobId, Class<T> type, Connection _con)
            throws SRMInvalidRequestException
    {
        for (Map.Entry<Class<? extends Job>, JobStorage<?>> entry: JobStorageFactory.getJobStorageFactory().getJobStorages().entrySet()) {
            if (type.isAssignableFrom(entry.getKey())) {
                try {
                    Job job;
                    if (_con == null) {
                        job = entry.getValue().getJob(jobId);
                    } else {
                        job = entry.getValue().getJob(jobId, _con);
                    }
                    if (job != null) {
                        return type.cast(job);
                    }
                } catch (SQLException e){
                    logger.error("Failed to read job", e);
                }
            }
        }
        throw new SRMInvalidRequestException("jobId = " + jobId + " does not correspond to any known job");
    }

    public static <T extends Job> Set<T> getActiveJobs(Class<T> type) throws SQLException
    {
        JobStorage<T> jobStorage = JobStorageFactory.getJobStorageFactory().getJobStorage(type);
        return (jobStorage == null) ? Collections.<T>emptySet() : jobStorage.getActiveJobs();
    }

    /** Performs state transition checking the legality first.
     */
    public State getState() {
        rlock();
        try {
            return state;
        } finally {
            runlock();
        }
    }

    /**
     *  Changes the state of this job to a new state
     * @param state
     * @param description
     * @throws IllegalStateTransition
     */
    public final void setState(State state,String description) throws
    IllegalStateTransition {
        setState(state,description,true);

    }

    /**
     * Changes the state of this job to a new state
     * @param state
     * @param description
     * @param save
     * if save is false we do not save state in database
     * the caller of the save state needs to call saveJob then
     * @throws IllegalStateTransition
     */
    public final void setState(State state,String description, boolean save) throws
       IllegalStateTransition {
        wlock();
        try {
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

            jobHistory.add( new JobHistory(nextLong(),state,description,lastStateTransitionTime));

            if( errorMessage.length()== 0) {
                  errorMessage.append(description);
            } else {
                 errorMessage.append("\nat ");
                 errorMessage.append(new Date(lastStateTransitionTime));
                 errorMessage.append(" appended:\n");
                 errorMessage.append(description);
            }

            if(state == State.RETRYWAIT) {
                inclreaseNumberOfRetries();
            }

            notifySchedulerOfStateChange(old, state);

            if (!state.isFinalState() && schedulerId == null) {
                throw new IllegalStateTransition("Scheduler ID is null");
            }
            stateChanged(old);
            if(save || !old.isFinalState() && state.isFinalState()) {
                saveJob();
            }
        } finally {
            wunlock();
        }
    }
    /**
     * Try to change the state of the job to READY. the request into the ready state
     */
    public void tryToReady() {
        rlock();
        try {
            if(state != State.RQUEUED) {
                return;
            }
        } finally {
            runlock();
        }

        /*
         * The job should be readied, only if the job's scheduler's
         * count of the "ready" jobs is bellow the maximum allowed number of the
         * Ready jobs. If the job is replicated in multiple jvm's,
         * the job might not have a scheduler associated with it in this jvm,
         * then in this jvm nothing needs to happen. Whatever clastering
         * mechanizm used needs to makes sure that if this method is called in
         * an instance that is different from the one where the job was
         * originally scheduled leads to the call of the invocation of the same
         * method in the instance of jvm where the job was orignally
         * scheduled, and where the scheduler is  non-null.
         * In case of terracotta it is achived by including this method in the
         * "distributed-method" section of the configuration file  :
         *       <distributed-methods>
         *          <method-expression>
         *            void Job.tryToReady()
         *          </method-expression>
         *       <method-expression>
         */
        if(schedulerId != null) {
            Scheduler scheduler =   Scheduler.getScheduler(schedulerId);
            // we excpect that the scheduler in not null only in the jvm
            // that created and scheduled the job in the first place
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

        StringBuilder errorsb = new StringBuilder();
        rlock();
        try {
            if(!jobHistory.isEmpty()) {
                JobHistory nextHistoryElement = jobHistory.get(jobHistory.size() -1);
                State nexthistoryElState = nextHistoryElement.getState();
                errorsb.append(" at ");
                errorsb.append(new Date(nextHistoryElement.getTransitionTime()));
                errorsb.append(" state ").append(nexthistoryElState);
                errorsb.append(" : ");
                errorsb.append(nextHistoryElement.getDescription());
            }
       } finally {
           runlock();
       }
       return errorsb.toString();
    }

    public void addHistoryEvent(String description){
        wlock();
        try {
            jobHistory.add( new JobHistory(nextLong(),state,description, System.currentTimeMillis()));
        } finally {
            wunlock();
        }

    }
     public String getHistory() {
        StringBuilder historyStringBuillder = new StringBuilder();
        rlock();
        try {
            SimpleDateFormat format = new SimpleDateFormat(ISO8601_FORMAT);
            for( JobHistory nextHistoryElement: jobHistory ) {
                 historyStringBuillder.append(" at ");
                 historyStringBuillder.append(format
                         .format(new Date(nextHistoryElement.getTransitionTime())));
                 historyStringBuillder.append(" state ").append(nextHistoryElement.getState());
                 historyStringBuillder.append(" : ");
                 historyStringBuillder.append(nextHistoryElement.getDescription());
                 historyStringBuillder.append('\n');
            }
        } finally {
            runlock();
        }
       return historyStringBuillder.toString();
    }

     public List<JobHistory> getJobHistory() {
        rlock();
        try {
            return new ArrayList<>(jobHistory);
        } finally {
            runlock();
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
        wlock();
        try {
            return numberOfRetries;
        } finally {
            wunlock();
        }
    }

    /** Setter for property numberOfRetries.
     *
     */
    private void inclreaseNumberOfRetries() {
        wlock();
        try {
            numberOfRetries++;
        } finally {
            wunlock();
        }
    }

    /** Getter for property retry_timer.
     * @return Value of property retry_timer.
     *
     */
    public TimerTask getRetryTimer() {
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
        rlock();
        try {
            return priority;
        } finally {
            runlock();
        }
    }

    /** Setter for property priority.
     * @param priority New value of property priority.
     *
     */
    public void setPriority(int priority) {
        wlock();
        try {
            if(priority <0) {
                throw new IllegalArgumentException(
                "priority should be greater than or equal to zero");
            }
            this.priority = priority;
        } finally {
            wunlock();
        }
    }

    /** Getter for property id.
     * @return Value of property id.
     *
     */
    public long getId() {
        return id;
    }
    /** Getter for property nextJobId.
     * @return Value of property nextJobId.
     *
     */
    public Long getNextJobId() {
        rlock();
        try {
            return nextJobId;
        } finally {
            runlock();
        }
    }

    /** Setter for property nextJobId.
     * @param nextJobId New value of property nextJobId.
     *
     */
    public void setNextJobId(Long nextJobId) {
        wlock();
        try {
            this.nextJobId = nextJobId;
            saveJob();
        } finally {
            wunlock();
        }
    }

    /** Getter for property schedulerId.
     * @return Value of property schedulerId.
     *
     */
    public String getSchedulerId() {
        rlock();
        try {
            return schedulerId;
        } finally {
            runlock();
        }
    }

    /** Setter for property schedulerId.
     * @param schedulerId New value of property schedulerId.
     *
     */
    public void setScheduler(String schedulerId,long schedulerTimeStamp) {
        wlock() ;
        try {
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
                        getJobStorage().saveJob(this,true);
                }catch (SQLException sqle) {
                    logger.error(sqle.toString());
                }
            }
        } finally {
            wunlock();
        }
    }

    /** Getter for property schedulerTimeStamp.
     * @return Value of property schedulerTimeStamp.
     *
     */
    public long getSchedulerTimeStamp() {
        rlock();
        try {
            return schedulerTimeStamp;
        } finally {
            runlock();
        }
    }

    public long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        wlock();
        try {
            if (state.isFinalState()){
                switch (state) {
                case CANCELED:
                    throw new SRMAbortedException("can't extend lifetime, job was aborted");
                case DONE:
                    throw new SRMReleasedException("can't extend lifetime, job has finished");
                default:
                    throw new SRMException("can't extend lifetime, job state is " + state);
                }
            }

            long now = System.currentTimeMillis();
            long remainingLifetime = creationTime + lifetime - now;
            if (remainingLifetime >= newLifetimeInMillis) {
                return remainingLifetime;
            }

            lifetime = now + newLifetimeInMillis - creationTime;
            saveJob();
            return newLifetimeInMillis;
        } finally {
            wunlock();
        }
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
    private long nextId() {
        return getGenerator().getNextId();
    }

    /**
     *
     * @return next long value
     */
    private long nextLong() {
        return getGenerator().nextLong();
    }

    public void checkExpiration()
    {
        wlock();
        try {
            if (creationTime + lifetime < System.currentTimeMillis()) {
                logger.debug("expiring job #{}", getId());
                if (state == State.READY || state == State.TRANSFERRING) {
                    setState(State.DONE, "lifetime expired");
                } else if (!state.isFinalState()) {
                    setState(State.FAILED, "lifetime expired");
                }
            }
        } catch (IllegalStateTransition e) {
            logger.error("Illegal state transition while expiring job: {}", e.toString());
        } finally {
            wunlock();
        }
    }

    /** Getter for property maxNumberOfRetries.
     * @return Value of property maxNumberOfRetries.
     *
     */
    public int getMaxNumberOfRetries() {
        rlock();
        try {
            return maxNumberOfRetries;
        } finally {
            runlock();
        }
    }

    /** Setter for property maxNumberOfRetries.
     * @param maxNumberOfRetries New value of property maxNumberOfRetries.
     *
     */
    public void setMaxNumberOfRetries(int maxNumberOfRetries) {
        wlock();
        try {
            this.maxNumberOfRetries = maxNumberOfRetries;
        } finally {
            wunlock();
        }
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
        rlock();
        try {
            return lifetime;
        } finally {
            runlock();
        }
    }

    public long getRemainingLifetime() {
        rlock();
        try {
            if (state.isFinalState()) {
                return 0;
            }
            long remainingLifetime = creationTime + lifetime - System.currentTimeMillis();
            return remainingLifetime > 0 ? remainingLifetime : 0;
        } finally {
            runlock();
        }
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
        wlock();
        try {
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
        } finally {
            wunlock();
        }
    }


    public long getLastStateTransitionTime(){
        rlock();
        try {
            return lastStateTransitionTime;
        } finally {
            runlock();
        }
    }

    public static class JobHistory implements Comparable<JobHistory> {
        private final long id;
        private final State state;
        private final long transitionTime;
        private final String description;
        private boolean saved; //false by default

        public JobHistory(long id, State state, String description, long transitionTime) {
            this.id = id;
            this.state = state;
            this.description = description.replace('\'','`');
            this.transitionTime = transitionTime;
        }

        /**
         * Getter for property state.
         * @return Value of property state.
         */
        public State getState() {
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
            return description;
        }

        @Override
        public int compareTo(JobHistory o)  {

            long oTransitionTime = o.getTransitionTime();
            return transitionTime < oTransitionTime?
                    -1:
                    (transitionTime == oTransitionTime? 0: 1);
        }

        @Override
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
        @Override
        public int hashCode() {
            return (int)(id ^ (id >>> 32));
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("JobHistory[");
            sb.append(new Date(transitionTime)).append(',');
            sb.append(state).append(',');
            sb.append(description).append(']');
            return sb.toString();
        }

        public synchronized boolean isSaved() {
            return saved;
        }

        public synchronized void setSaved() {
            this.saved = true;
        }

    }

    public void setRetryTimer(TimerTask retryTimer) {
        this.retryTimer = retryTimer;
    }

    public JDC applyJdc()
    {
        JDC current = jdc.apply();
        JDC.appendToSession(String.valueOf(id));
        return current;
    }

    /**
     * This is the initial call to schedule the job for execution
     */
    public void schedule() throws InterruptedException,IllegalStateTransition
    {
        wlock();
        try{
            if(!State.PENDING.equals(state)) {
                throw new IllegalStateException("State is not pending");
            }
            Scheduler scheduler = SchedulerFactory.getSchedulerFactory().getScheduler(this);
            this.setScheduler(scheduler.getId(), 0);
            scheduler.schedule(this);
        } finally {
            wunlock();
        }
    }

    /**
     * Notifies the scheduler of the this job of a  change
     * of the state from old to new
     * @param oldState
     * @param newState
     */
    private void notifySchedulerOfStateChange(State oldState, State newState) {
        // we need to lock again, even if the call is from the setState method
        // where weh have locked already. The method is configured to run in
        // multiple jvms though "dsitributed method" invocation, which means
        // the method can be called on its own by terracotta framework
        wlock();
        try {
            /*
             * The state change needs to be correctly accounted by the scheduler that
             * executes this job. This is done by call to scheduler's stateChanged
             * method. If the job is replicated in multiple jvm's, the job might have
             * a scheduler associated with it in different jvm.
             * Then in this jvm call to stateChanged should not take place.
             * Insted whatever clastering mechanizm is used needs to makes sure that
             * if this method is also called in an instance where the job was
             * originally scheduled and where the Scheduler.getScheduler(schedulerId)
             * will return non null result.
             * In case of terracotta it is achived by including this method in the
             * "distributed-method" section of the configuration file  :
             *       <distributed-methods>
             *          <method-expression>
             *            void Job.notifySchedulerOfStateChange(..)
             *          </method-expression>
             *       <method-expression>
             *
             */

             if (schedulerId != null) {
                Scheduler scheduler = Scheduler.getScheduler(schedulerId);
                // we excpect that the scheduler in not null only in the jvm
                // that created and scheduled the job in the first place
                if (scheduler != null) {
                    // code bellow is executed only in one SRM in the cluster
                    // the SRM that created this job
                    logger.debug("notifySchedulerOfStateChange calls " +
                            "scheduler.stateChanged()");
                    scheduler.stateChanged(this, oldState, newState);
                    // set schedulerId to null only in jvm
                    // that has scheduler that "owns" this job
                    if(state.isFinalState()) {
                        schedulerId = null;
                    }
                }
            }
        } finally {
            wunlock();
        }
    }

    public final void wlock() {
        writeLock.lock();
    }

    public final void wunlock() {
        writeLock.unlock();
    }

    public final void rlock() {
        // Terracotta currently does not support upgrading read lock to
        // write lock. So we use write logs everywhere.
        // See bug at
        // https://jira.terracotta.org/jira/browse/CDV-787
        writeLock.lock();
    }

    public final void runlock() {
        // Terracotta currently does not support upgrading read lock to
        // write lock. So we use write logs everywhere.
        // See bug at
        // https://jira.terracotta.org/jira/browse/CDV-787
        writeLock.unlock();
    }

    @Override
    public final String toString() {
        return toString(false);
    }

     public final String toString(boolean longformat) {
        StringBuilder sb = new StringBuilder();
        toString(sb,longformat);
        return sb.toString();
    }

    public abstract void toString(StringBuilder sb, boolean longformat);
}
