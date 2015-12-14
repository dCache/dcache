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
package org.dcache.srm.request;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import javax.annotation.Nonnull;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.dcache.srm.SRMAbortedException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobIdGenerator;
import org.dcache.srm.scheduler.JobIdGeneratorFactory;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.JobStorageFactory;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.util.TimeUtils;
import org.dcache.util.TimeUtils.TimeUnitFormat;

import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The base class for all scheduled activity within SRM.  An instance of this
 * class represents either a complete SRM operation (Request), or an individual
 * file within an operation (FileRequest).
 */
public abstract class Job  {

    private static final Logger logger = LoggerFactory.getLogger(Job.class);

    protected static final String TIMESTAMP_FORMAT = "yyyy-MM-dd' 'HH:mm:ss.SSS";

    //this is used to build the queue of jobs.
    protected Long nextJobId;

    protected final long id;

    /**
     * Status code from version 2.2
     * provides a better description of
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;

    private volatile State state = State.UNSCHEDULED;

    protected int priority;
    protected String schedulerId;
    protected long schedulerTimeStamp;


    protected final long creationTime;

    protected long lifetime;

    private long lastStateTransitionTime = System.currentTimeMillis();

    private final List<JobHistory> jobHistory = new ArrayList<>();
    private transient JobIdGenerator generator;

    private transient boolean savedInFinalState;

    protected transient JDC jdc;

    private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock();

    // this constructor is used for restoring the job from permanent storage
    // should be called through the Job.getJob only, otherwise the expireRestoredJobOrCreateExperationTimer
    // will never be called
    // we can not call it from the constructor, since this may lead to recursive job restoration
    // leading to the exhaust of the pool of database connections
    protected Job(long id, Long nextJobId, long creationTime,
                  long lifetime, int stateId, String errorMessage,
                  String schedulerId,
                  long schedulerTimestamp,
                  int numberOfRetries,
                  long lastStateTransitionTime,
                  JobHistory[] jobHistoryArray,
                  String statusCodeString) {
        this.id = id;
        this.nextJobId = nextJobId;
        this.creationTime = creationTime;
        this.lifetime = lifetime;
        if(state == null) {
            throw new NullPointerException(" job state is null");
        }
        this.state = State.getState(stateId);
        this.schedulerId = schedulerId;
        this.schedulerTimeStamp = schedulerTimestamp;
        this.lastStateTransitionTime = lastStateTransitionTime;
        this.jdc = new JDC();
        if(jobHistoryArray != null) {
            Collections.addAll(jobHistory, jobHistoryArray);
        } else {
            jobHistory.add(new JobHistory(nextLong(), state, "Request restored from database", System.currentTimeMillis()));
        }
        this.statusCode = statusCodeString==null
                ?null
                :TStatusCode.fromString(statusCodeString);
    }

    /** Creates a new instance of Job */

    public Job(long lifetime) {

        id = nextId();
        creationTime = System.currentTimeMillis();
        this.lifetime = lifetime;
        this.jdc = new JDC();
        jobHistory.add(new JobHistory(nextLong(), state, "Request created", lastStateTransitionTime));
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
            boolean isFinalState = this.getState().isFinal();
            getJobStorage().saveJob(this, isFinalState || force);
            savedInFinalState = isFinalState;
        } catch (DataAccessException e) {
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
    @Nonnull
    public static final <T extends Job> T getJob(long id, Class<T> type)
            throws SRMInvalidRequestException
    {
        for (Map.Entry<Class<? extends Job>, JobStorage<?>> entry: JobStorageFactory.getJobStorageFactory().getJobStorages().entrySet()) {
            if (type.isAssignableFrom(entry.getKey())) {
                try {
                    Job job = entry.getValue().getJob(id);
                    if (job != null) {
                        return type.cast(job);
                    }
                } catch (DataAccessException e) {
                    logger.error("Failed to read job", e);
                }
            }
        }
        throw new SRMInvalidRequestException("Id " + id + " does not correspond to any known job");
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
     * Changes the state of this job to a new state.
     */
    public final void setState(State newState, String description)
            throws IllegalStateTransition
    {
        wlock();
        try {
            if (newState == this.state) {
                return;
            }
            if (!isValidTransition(this.state, newState)) {
                throw new IllegalStateTransition(
                        "Illegal state transition from " + this.state + " to " + newState,
                        this.state, newState);
            }
            State oldState = this.state;
            this.state = newState;
            lastStateTransitionTime = System.currentTimeMillis();

            jobHistory.add( new JobHistory(nextLong(),newState,description,lastStateTransitionTime));

            notifySchedulerOfStateChange(oldState, newState);

            if (!newState.isFinal() && schedulerId == null) {
                throw new IllegalStateTransition("Scheduler ID is null");
            }
            stateChanged(oldState);
            saveJob(state == State.RQUEUED);
        } finally {
            wunlock();
        }
    }

    private boolean isValidTransition(State currentState, State newState)
            throws IllegalStateTransition
    {
        switch (currentState) {
        case UNSCHEDULED:
        case RESTORED:
            return newState == State.DONE
                    || newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.QUEUED;
        case QUEUED:
            return newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.INPROGRESS
                    || newState == State.UNSCHEDULED;
        case INPROGRESS:
            return newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.QUEUED
                    || newState == State.RQUEUED
                    || newState == State.READY
                    || newState == State.DONE;
        case RQUEUED:
            return newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.READY;
        case READY:
            return newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.TRANSFERRING
                    || newState == State.DONE;
        case TRANSFERRING:
            return newState == State.CANCELED
                    || newState == State.FAILED
                    || newState == State.DONE;
        case FAILED:
        case DONE:
        case CANCELED:
            return false;
        }
        return true;
    }

    /**
     * Try to change the state of the job into the READY state.
     */
    public void tryToReady() {
        wlock();
        try {
            if (state == State.RQUEUED && schedulerId != null) {
                Scheduler scheduler = Scheduler.getScheduler(schedulerId);
                if (scheduler != null) {
                    scheduler.tryToReadyJob(this);
                }
            }
        } finally {
            wunlock();
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
            jobHistory.add(new JobHistory(nextLong(), state, description, System.currentTimeMillis()));
        } finally {
            wunlock();
        }

    }

     public CharSequence getHistory() {
         return getHistory("");
     }

     public CharSequence getHistory(String padding) {
        StringBuilder historyStringBuillder = new StringBuilder();
        long previousTransitionTime = 0;
        State previousTransitionState = State.UNSCHEDULED;
        rlock();
        try {
            SimpleDateFormat format = new SimpleDateFormat(TimeUtils.TIMESTAMP_FORMAT);
            for( JobHistory nextHistoryElement: jobHistory ) {
                 if (historyStringBuillder.length() != 0) {
                     appendDuration(historyStringBuillder, nextHistoryElement.getTransitionTime() -
                             previousTransitionTime, "").append('\n');
                 }
                 previousTransitionTime = nextHistoryElement.getTransitionTime();
                 historyStringBuillder.append(padding);
                 historyStringBuillder.append("   ").append(format
                         .format(new Date(nextHistoryElement.getTransitionTime())));
                 historyStringBuillder.append(" ").append(nextHistoryElement.getState());
                 historyStringBuillder.append(": ");
                 historyStringBuillder.append(nextHistoryElement.getDescription());
                 previousTransitionState = nextHistoryElement.getState();
            }
        } finally {
            runlock();
        }
        if (historyStringBuillder.length() != 0) {
            if (!previousTransitionState.isFinal()) {
                long duration = System.currentTimeMillis() - previousTransitionTime;
                appendDuration(historyStringBuillder, duration, ", so far");
            }
            historyStringBuillder.append('\n');
        }
       return historyStringBuillder;
    }

    private StringBuilder appendDuration(StringBuilder sb, long duration, String extra)
    {
        sb.append(" (").append(TimeUtils.duration(duration, MILLISECONDS, TimeUnitFormat.SHORT));
        sb.append(extra);
        sb.append(")");
        return sb;
    }

     public List<JobHistory> getJobHistory() {
        rlock();
        try {
            return new ArrayList<>(jobHistory);
        } finally {
            runlock();
        }
    }

    @Nonnull
    public JobHistory getLastJobChange()
    {
        rlock();
        try {
            return Iterables.getLast(jobHistory);
        } finally {
            runlock();
        }
    }

    public abstract void run() throws SRMException, IllegalStateTransition;

    //implementation should not block in this method
    // this method should make sure that the job is saved in the
    // job's storage (instance of Jon.JobStorage (possibly in a database )
    protected abstract void stateChanged(State oldState);

    public TStatusCode getStatusCode() {
        rlock();
        try {
            return statusCode;
        } finally {
            runlock();
        }
    }

    public void setStatusCode(TStatusCode statusCode) {
        wlock();
        try {
            this.statusCode = statusCode;
        } finally {
            wunlock();
        }
    }

    public String getStatusCodeString() {
         rlock();
         try {
            return statusCode==null ? null:statusCode.getValue() ;
         } finally {
             runlock();
         }
    }

    public void setStateAndStatusCode(
            State state,
            String description,
            TStatusCode statusCode)  throws IllegalStateTransition  {
        wlock();
        try {
            setState(state, description);
            setStatusCode(statusCode);
        } finally {
            wunlock();
        }
    }

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
                saveJob(true);
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

    protected long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        wlock();
        try {
            if (state.isFinal()){
                /* [ SRM 2.2, 5.16.2 ]
                 *
                 * h) Lifetime cannot be extended on the released files, aborted files, expired
                 *    files, and suspended files. For example, pin lifetime cannot be extended
                 *    after srmPutDone is requested on SURLs for srmPrepareToPut request. In
                 *    such case, SRM_INVALID_REQUEST at the file level must be returned, and
                 *    SRM_PARTIAL_SUCCESS or SRM_FAILURE must be returned at the request level.
                 *
                 * [ SRM 2.2, 5.16.3 ]
                 *
                 * SRM_ABORTED
                 * §  The requested file has been aborted.
                 * SRM_RELEASED
                 * §  The requested file has been released.
                 * SRM_INVALID_REQUEST
                 * §  Attempt to extend pin lifetimes on TURLs that have been already expired.
                 *
                 * ----
                 *
                 * We interpret the above to mean that attempting to extend the lifetime of
                 * any request that is in a final state should either result in SRM_ABORTED,
                 * SRM_RELEASED, or SRM_INVALID_REQUEST. Specifically a request that failed
                 * does not cause lifetime extension to return SRM_FAILURE - SRM_FAILURE is
                 * only return if the lifetime extension request itself fails.
                 */
                switch (state) {
                case CANCELED:
                    throw new SRMAbortedException("can't extend lifetime, job was aborted");
                case DONE:
                    throw new SRMReleasedException("can't extend lifetime, job has finished");
                case FAILED:
                    throw new SRMInvalidRequestException("can't extend lifetime, job has failed");
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
            saveJob(true);
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
            if (creationTime + lifetime < System.currentTimeMillis() && !state.isFinal()) {
                logger.info("expiring job #{}", getId());
                setState(State.FAILED, "Request lifetime expired.");
            }
        } catch (IllegalStateTransition e) {
            logger.error("Illegal state transition while expiring job: {}", e.toString());
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
            if (state.isFinal()) {
                return 0;
            }
            long remainingLifetime = creationTime + lifetime - System.currentTimeMillis();
            return remainingLifetime > 0 ? remainingLifetime : 0;
        } finally {
            runlock();
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
            return "JobHistory[" + new Date(transitionTime) + ',' + state + ',' + description + ']';
        }

        public synchronized boolean isSaved() {
            return saved;
        }

        public synchronized void setSaved() {
            this.saved = true;
        }

    }

    public JDC applyJdc()
    {
        JDC current = jdc.apply();
        JDC.appendToSession(String.valueOf(id));
        return current;
    }

    public Class<? extends Job> getSchedulerType()
    {
        return getClass();
    }

    /**
     * This is the initial call to schedule the job for execution
     */
    public void scheduleWith(Scheduler scheduler) throws InterruptedException,
            IllegalStateTransition
    {
        wlock();
        try{
            if(state != State.UNSCHEDULED) {
                throw new IllegalStateException("Job " +
                        getClass().getSimpleName() + " [" + this.getId() +
                        "] has state " + state + "(not UNSCHEDULED)");
            }
            setScheduler(scheduler.getId(), scheduler.getTimestamp());
            scheduler.queue(this);
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
         if (schedulerId != null) {
            Scheduler scheduler = Scheduler.getScheduler(schedulerId);
            if (scheduler != null) {
                logger.debug("notifySchedulerOfStateChange calls scheduler.stateChanged()");
                scheduler.stateChanged(this, oldState, newState);
                if (state.isFinal()) {
                    schedulerId = null;
                }
            }
        }
    }

    public final void wlock() {
        lock.writeLock().lock();
    }

    public final void wunlock() {
        lock.writeLock().unlock();
    }

    /* Note that a read lock cannot be upgraded to a write lock. */
    public final void rlock() {
        lock.readLock().lock();
    }

    public final void runlock() {
        lock.readLock().unlock();
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


    /**
     * Method called when the SRM is started and a job has been restored from
     * some JobStorage (such as a DatabaseJobStorage) and the job is in a
     * non-final state.
     */
    public void onSrmRestart(Scheduler scheduler, boolean shouldFailJobs)
    {
        wlock();
        try {
            if (state.isFinal()) {
                return;
            }

            setScheduler(scheduler.getId(), scheduler.getTimestamp());
            notifySchedulerOfStateChange(State.RESTORED, state);

            if (shouldFailJobs) {
                setState(State.FAILED, "Aborted due to SRM service restart.");
                return;
            }

            if (getRemainingLifetime() == 0) {
                setState(State.FAILED, "Expired during SRM service restart.");
                return;
            }

            switch (state) {
            // Unscheduled or queued jobs were never worked on before the SRM restart; we
            // simply queue them now.
            case UNSCHEDULED:
            case QUEUED:
                addHistoryEvent("Restored from database.");
                scheduler.queue(this);
                break;

            // Jobs in RQUEUED, READY or TRANSFERRING states require no further
            // processing. We can leave them for the client to discover the TURL
            // or place the job into the DONE state, respectively.
            case RQUEUED:
            case READY:
            case TRANSFERRING:
                break;

            // Other job states need request-specific recovery process.
            default:
                onSrmRestartForActiveJob(scheduler);
                break;
            }
        } catch (IllegalStateTransition e) {
            logger.error("Failed to restore job: " + e.getMessage());
        } finally {
            wunlock();
        }
    }

    /**
     * Provide request-specific recovery for jobs that were being processed
     * when SRM was restarted.  This corresponds to jobs in state INPROGRESS.
     *
     * In general, such jobs require some request-specific procedure.
     * Subclasses are expected to override this method to provide this
     * procedure.
     */
    protected void onSrmRestartForActiveJob(Scheduler scheduler)
            throws IllegalStateTransition
    {
        // By default, simply fail such requests.
        setState(State.FAILED, "Aborted due to SRM service restart.");
    }
}
