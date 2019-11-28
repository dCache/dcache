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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.transaction.TransactionException;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.GuardedBy;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
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

import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.TimeUnit.MILLISECONDS;


/**
 * The base class for all scheduled activity within SRM.  An instance of this
 * class represents either a complete SRM operation (Request), or an individual
 * file within an operation (FileRequest).
 */
public abstract class Job  {

    private static final Logger LOGGER = LoggerFactory.getLogger(Job.class);

    //this is used to build the queue of jobs.
    protected Long nextJobId;

    private final long id;

    /**
     * Status code from version 2.2
     * provides a better description of
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;

    private volatile State state = State.UNSCHEDULED;

    protected String schedulerId;
    private long schedulerTimeStamp;


    protected final long creationTime;

    private long lifetime;

    private long lastStateTransitionTime = System.currentTimeMillis();

    private final List<JobHistory> jobHistory = new ArrayList<>();
    private transient JobIdGenerator generator;

    private transient boolean savedInFinalState;

    protected transient JDC jdc;

    private final ReentrantReadWriteLock lock =
            new ReentrantReadWriteLock();

    private final List<JobStateChangeAware> stateChangeListeners = new CopyOnWriteArrayList<>();

    // this constructor is used for restoring the job from permanent storage
    // should be called through the Job.getJob only, otherwise the expireRestoredJobOrCreateExperationTimer
    // will never be called
    // we can not call it from the constructor, since this may lead to recursive job restoration
    // leading to the exhaust of the pool of database connections
    protected Job(long id, Long nextJobId, long creationTime,
                  long lifetime, int stateId,
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

    /**
     * Subscribe listener to learn about changes to this Job's state.
     * The listener's method is called while the Job's read lock is acquired.
     * @param listener the object to be notified
     */
    public void subscribe(JobStateChangeAware listener)
    {
        stateChangeListeners.add(listener);
    }

    /**
     * No longer receive notification about state changes.
     * It is safe to call this method from a JobStateChangeAware method.
     * @param listener the object that should not receive further notification.
     * @return true if listener was already subscribed.
     */
    public boolean unsubscribe(JobStateChangeAware listener)
    {
        return stateChangeListeners.remove(listener);
    }

    private JobStorage<Job> getJobStorage() {
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
        } catch (TransactionException e) {
            // if saving fails we do not want to fail the request
            LOGGER.error("Failed to save SQL request to database: {}", e.toString());
        } catch (RuntimeException e) {
            // if saving fails we do not want to fail the request
            LOGGER.error("Failed to save SQL request to database. Please report to support@dcache.org.", e);
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
                    LOGGER.error("Failed to read job", e);
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
        boolean isDowngraded = false;
        try {
            State oldState = state;
            try {
                if (newState == state) {
                    return;
                }
                if (!isValidTransition(state, newState)) {
                    throw new IllegalStateTransition(
                            "Illegal state transition from " + state + " to " + newState,
                            state, newState);
                }
                processStateChange(newState, description);
                saveJob(state == State.RQUEUED);

                // Downgrade from write lock to read lock
                rlock();
                isDowngraded = true;
            } finally {
                wunlock();
            }

            notifyListeners(oldState, description);
        } finally {
            if (isDowngraded) {
                runlock();
            }
        }
    }

    /**
     * Job-internal processing of a state change. Subclasses should override
     * this method to do any internal processing.  When this method returns,
     * the Job object is fully updated based on this state change.
     */
    @GuardedBy("lock")
    protected void processStateChange(State newState, String description)
    {
        this.state = newState;
        lastStateTransitionTime = System.currentTimeMillis();

        jobHistory.add( new JobHistory(nextLong(),newState,description,lastStateTransitionTime));

        checkState(newState.isFinal() || schedulerId != null,
                "Scheduler ID is null");
    }

    /**
     * Notify all subscribers that the job's state has changed.  This method
     * must only be called with the current thread has obtained either read- or
     * write-lock.  Obtaining the read-lock is preferred, as this promotes
     * concurrency.
     */
    @GuardedBy("lock")
    private void notifyListeners(State oldState, String description)
    {
        for (JobStateChangeAware listener : stateChangeListeners) {
            try {
                listener.jobStateChanged(this, oldState, description);
            } catch (RuntimeException e) {
                LOGGER.error("Bug found: please report to support@dcache.org", e);
            }
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

    /**
     * Provide the latest JobHistory description.  If the job is in a
     * terminal error state the string should hint as to what triggered the
     * failure.
     * @return the String argument last supplied to addHistoryEvent.
     * <p/>
     * See {@link #addHistoryEvent(java.lang.String) }
     */
    @Nonnull
    public String latestHistoryEvent() {
        rlock();
        try {
            if (jobHistory.isEmpty()) {
                return "initial state";
            } else {
                JobHistory latest = jobHistory.get(jobHistory.size() -1);
                return latest.getDescription();
            }
       } finally {
           runlock();
       }
    }

    protected void addHistoryEvent(String description){
        wlock();
        try {
            jobHistory.add(new JobHistory(nextLong(), state, description, System.currentTimeMillis()));
        } finally {
            wunlock();
        }

    }

     protected CharSequence getHistory(String padding) {
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

    public abstract void run() throws SRMException, IllegalStateTransition;

    public TStatusCode getStatusCode() {
        rlock();
        try {
            return statusCode;
        } finally {
            runlock();
        }
    }

    protected void setStatusCode(TStatusCode statusCode) {
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

    /**
     * Associate this job with the supplied scheduler.
     */
    private void setScheduler(Scheduler scheduler) {
        wlock() ;
        try {
            //  check if the values have indeed changed
            // If they are the same, we do not need to do anythign.
            if (schedulerTimeStamp != scheduler.getTimestamp()
                    || !Objects.equals(schedulerId, scheduler.getId())) {
                schedulerTimeStamp = scheduler.getTimestamp();
                schedulerId = scheduler.getId();

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

    protected long getRemainingLifetime() {
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

    protected int getRemainingLifetimeIn(TimeUnit unit) {
        return (int) Math.min(unit.convert(getRemainingLifetime(), TimeUnit.MILLISECONDS), Integer.MAX_VALUE);
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
    public void scheduleWith(Scheduler scheduler) throws IllegalStateTransition
    {
        wlock();
        try {
            if (state != State.UNSCHEDULED) {
                throw new IllegalStateException("Job " +
                        getClass().getSimpleName() + " [" + id +
                        "] has state " + state + " (not UNSCHEDULED)");
            }
            setScheduler(scheduler);
            scheduler.queue(this);
        } finally {
            wunlock();
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

            setScheduler(scheduler);
            notifyListeners(State.RESTORED, "Restored from database.");

            if (shouldFailJobs) {
                setState(State.FAILED, "Aborted due to SRM service restart.");
                return;
            }

            if (getRemainingLifetime() == 0) {
                setState(State.FAILED, "Expired during SRM service restart.");
                return;
            }

            if (state == State.INPROGRESS) {
                onSrmRestartForActiveJob();
            }

            if (state.isFinal()) {
                return;
            }

            addHistoryEvent("Restored from database.");
            scheduler.inherit(this);
        } catch (IllegalStateTransition e) {
            LOGGER.error("Failed to restore job: {}", e.getMessage());
        } finally {
            wunlock();
        }

    }

    /**
     * Provide request-specific recovery for jobs that were being processed
     * when SRM was stopped.  This corresponds to jobs in the
     * {@literal INPROGRESS} state.
     * <p>
     * If dCache is able to recover an active Job then this method will do any
     * necessary activity and leaves the Job in the {@literal INPROGRESS} state.
     * The Job will be schedule after this method returns.
     * <p>
     * If dCache is unable to recover an active job then this method should
     * simply set the state to {@literal State.FAILED}.
     */
    protected void onSrmRestartForActiveJob() throws IllegalStateTransition
    {
        // By default, simply fail such requests.
        setState(State.FAILED, "Aborted due to SRM service restart.");
    }
}
