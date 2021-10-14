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
package org.dcache.services.bulk.job;

import static java.util.Objects.requireNonNull;

import java.util.concurrent.Future;
import javax.security.auth.Subject;
import org.dcache.auth.attributes.Restriction;
import org.dcache.services.bulk.BulkJobExecutionException;
import org.dcache.services.bulk.handlers.BulkJobCompletionHandler;
import org.dcache.vehicles.FileAttributes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The basic bulk job definition.
 * <p>
 * This class should not be extended directly to add new types of activity (use the SingleTargetJob
 * instead).  It is public only because it needs to be visible to other internal packages.
 */
public abstract class BulkJob implements Runnable, Comparable<BulkJob> {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BulkJob.class);

    private static final String INVALID_STATE_TRANSITION =
          "%s: invalid bulk job state transition %s to %s; "
                + "please report this to dcache.org.";

    public enum State {
        CREATED, INITIALIZED, STARTED, WAITING, CANCELLED, COMPLETED, FAILED
    }

    protected final BulkJobKey key;
    protected final BulkJobKey parentKey;
    protected final String activity;

    /**
     * The 'target' specified by the bulk request.  This usually a path, either a directory or a
     * regular file.
     */
    protected String target;

    protected FileAttributes attributes;
    protected Subject subject;
    protected Restriction restriction;
    protected State state = State.CREATED;
    protected Throwable errorObject;

    protected BulkJobCompletionHandler completionHandler;
    protected Future future;

    private long startTime;
    private boolean valid;

    protected BulkJob(BulkJobKey key, BulkJobKey parentKey, String activity) {
        this.key = key;
        this.parentKey = parentKey;
        this.activity = activity;
        this.valid = true;
    }

    public synchronized boolean cancel() {
        switch (state) {
            case CREATED:
            case INITIALIZED:
            case STARTED:
            case WAITING:
                state = State.CANCELLED;
                if (future != null) {
                    future.cancel(true);
                }

                if (errorObject == null) {
                    errorObject = new BulkJobExecutionException(key.toString()
                          + ":" + state);
                }

                doOnCancellation();
                return true;
            default:
                /*
                 * Already terminated.
                 */
                return false;
        }
    }

    @Override
    public int compareTo(BulkJob other) {
        if (other == null) {
            return -1;
        }

        return key.toString().compareTo(other.getKey().toString());
    }

    public String getActivity() {
        return activity;
    }

    public BulkJobCompletionHandler getCompletionHandler() {
        return completionHandler;
    }

    public Throwable getErrorObject() {
        return errorObject;
    }

    public BulkJobKey getKey() {
        return key;
    }

    public BulkJobKey getParentKey() {
        return parentKey;
    }

    public Restriction getRestriction() {
        return restriction;
    }

    public long getStartTime() {
        return startTime;
    }

    public synchronized State getState() {
        return state;
    }

    public Subject getSubject() {
        return subject;
    }

    public String getTarget() {
        return target;
    }

    public void initialize() {
        LOGGER.trace("BulkJob, initialize() called ...");
        requireNonNull(completionHandler,
              "Job completion handler "
                    + "was not set!  This is a bug.");

        doInitialize();

        synchronized (this) {
            state = State.INITIALIZED;
        }
    }

    public synchronized BulkJob invalidate() {
        valid = false;
        return this;
    }

    public synchronized boolean isReady() {
        switch (state) {
            case CREATED:
                return true;
            default:
                return false;
        }
    }

    public synchronized boolean isTerminated() {
        switch (state) {
            case FAILED:
            case CANCELLED:
            case COMPLETED:
                return true;
            default:
                return false;
        }
    }

    public synchronized boolean isValid() {
        return valid;
    }

    public synchronized boolean isWaiting() {
        return state == State.WAITING;
    }

    public void run() {
        LOGGER.trace("{}, run() called ...", key);

        setState(State.STARTED);

        doRun();
    }

    public void setAttributes(FileAttributes attributes) {
        this.attributes = attributes;
    }

    public void setCompletionHandler(BulkJobCompletionHandler completionHandler) {
        this.completionHandler = completionHandler;
    }

    public void setErrorObject(Throwable errorObject) {
        this.errorObject = errorObject;
    }

    public void setFuture(Future future) {
        this.future = future;
    }

    public void setRestriction(Restriction restriction) {
        this.restriction = restriction;
    }

    public synchronized void setState(State state) {
        requireNonNull(state);

        switch (this.state) {
            case CANCELLED:
            case COMPLETED:
            case FAILED:
                break;
            case WAITING:
                switch (state) {
                    case CANCELLED:
                    case COMPLETED:
                    case FAILED:
                        this.state = state;
                        postCompletion();
                        break;
                    default:
                        throw new IllegalStateException(
                              String.format(INVALID_STATE_TRANSITION,
                                    key.getKey(),
                                    this.state,
                                    state));
                }
                break;
            case STARTED:
                switch (state) {
                    case CANCELLED:
                    case COMPLETED:
                    case FAILED:
                        postCompletion();
                    case WAITING:
                        this.state = state;
                        break;
                    default:
                        throw new IllegalStateException(
                              String.format(INVALID_STATE_TRANSITION,
                                    key.getKey(),
                                    this.state,
                                    state));
                }
                break;
            case INITIALIZED:
                switch (state) {
                    case STARTED:
                        startTime = System.currentTimeMillis();
                        this.state = state;
                        break;
                    case CANCELLED:
                    case COMPLETED:
                    case FAILED:
                        this.state = state;
                        postCompletion();
                        break;
                    case WAITING:
                    default:
                        throw new IllegalStateException(
                              String.format(INVALID_STATE_TRANSITION,
                                    key.getKey(),
                                    this.state,
                                    state));
                }
                break;
            case CREATED:
            default:
                this.state = state;
        }
    }

    public void setSubject(Subject subject) {
        this.subject = subject;
    }

    public void setTarget(String target) {
        this.target = target;
    }

    protected abstract void doRun();

    protected abstract void postCompletion();

    protected void doInitialize() {
        // Optional
    }

    protected void doOnCancellation() {
        completionHandler.jobCancelled(this);
    }
}
