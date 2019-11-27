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
 * ContainerRequest.java
 *
 * Created on July 5, 2002, 12:03 PM
 */

package org.dcache.srm.request;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.function.LongFunction;

import org.dcache.util.AtomicCounter;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.TRequestSummary;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static org.dcache.srm.handler.ReturnStatuses.*;

import static org.dcache.util.TimeUtils.relativeTimestamp;

/**
 * This abstract class represents an "SRM request"
 * We currently support "get","put", and "copy" requests
 * which are the subclasses of this class
 * Each Requests contains a set (array) of FileRequests
 * each ContainerRequest is identified by its requestId
 * and each FileRequest within ContainerRequest is identified by its fileRequestId
 * The actual FileRequest arrays are in subclasses too.
 *
 * @author timur
 * @version 1.0
 */

public abstract class ContainerRequest<R extends FileRequest<?>> extends Request {
    private static final Logger LOGGER = LoggerFactory.getLogger(ContainerRequest.class);
    // dcache  requires that once client created a connection to a dcache door,
    // it uses the same door to make all following dcap transfers
    // therefore we need to synchronize the recept of dcap turls
    private URI previousTurl;

    private final ImmutableList<R> fileRequests;

    /**
     * Counter used for notification between file requests and the
     * parent request. The counter is incremented whenever when one of
     * the file requests changes to one of a set of predefined states.
     */
    protected final transient AtomicCounter _stateChangeCounter = new AtomicCounter();


    /*
     * public constructors
     */
    /**
     * Create a  new request
     * @param user
     *  srm user
     * @param configuration
     *   srm configuration
     */
    public ContainerRequest(@Nonnull String srmId, SRMUser user,
            long max_update_period, long lifetime, @Nullable String description,
            String client_host, LongFunction<ImmutableList<R>> factory)
    {
         super(srmId, user, max_update_period, lifetime, description, client_host);
         fileRequests = factory.apply(getId());
    }


    /**
     * this constructor is used for restoring the previously
     * saved ContainerRequest from persitance storage
     */
    protected ContainerRequest(@Nonnull String srmId, long id, Long nextJobId,
            long creationTime, long lifetime, int stateId, SRMUser user,
            String scheduelerId, long schedulerTimeStamp, int numberOfRetries,
            long lastStateTransitionTime, JobHistory[] jobHistoryArray,
            ImmutableList<R> fileRequests, int retryDeltaTime,
            boolean should_updateretryDeltaTime, String description,
            String client_host, String statusCodeString) {
        super(srmId, id, nextJobId, creationTime, lifetime, stateId, user, scheduelerId,
                schedulerTimeStamp, numberOfRetries, lastStateTransitionTime,
                jobHistoryArray, retryDeltaTime, should_updateretryDeltaTime,
                description, client_host, statusCodeString);
        this.fileRequests = fileRequests;
    }

    protected final R getFileRequest(long fileRequestId){
        for (R fileRequest: fileRequests) {
            if (fileRequest.getId() == fileRequestId) {
                return fileRequest;
            }
        }
        return null;
    }

    /*
     * public abstract instance methods
     */

    /**
     *  gets a number of file requests  in this request
     * @return
     * a number of file requests
     */
    protected int getNumOfFileRequest()
    {
        return fileRequests.size();
    }

    protected URI getPreviousTurl() {
        rlock();
        try {
            return previousTurl;
        } finally {
            runlock();
        }
    }

    protected void setPreviousTurl(URI s) {
        wlock();
        try {
            previousTurl = s;
        } finally {
            wunlock();
        }
    }

    @Override
    public TReturnStatus abort(String reason)
    {
        boolean hasSuccess = false;
        boolean hasFailure = false;
        wlock();
        try {
            /* [ SRM 2.2, 5.11.2 ]
             *
             * a) srmAbortRequest terminates all files in the request regardless of the file
             *    state. Remove files from the queue, and release cached files if a limited
             *    lifetime is associated with the file.
             * c) Abort must be allowed to all requests with requestToken.
             * i) When duplicate abort request is issued on the same request, SRM_SUCCESS
             *    may be returned to all duplicate abort requests and no operations on
             *    duplicate abort requests are performed.
             */
            updateStatus();
            if (!getState().isFinal()) {
                for (R file : getFileRequests()) {
                    try {
                        file.abort(reason);
                        hasSuccess = true;
                    } catch (SRMException | IllegalStateTransition e) {
                        hasFailure = true;
                    }
                }
                updateStatus();
            }
        } finally {
            wunlock();
        }
        return getSummaryReturnStatus(hasFailure, hasSuccess);
    }

    public final void updateStatus() {
        wlock();
        try {
            // we used to synchronize on this container request here, but
            // it does not make sence as the file requests are being processed
            // by multiple threads,  and synchronizing here can cause a deadlock
            // once all field access is made sycnhronized and file request need to
            // access container request fields
            // we can rely on the fact that
            // once file request reach their final state, this state does not change
            // so the combined logic
            updateRetryDeltaTime();
            boolean haveNonFinalRequests = false;
            boolean haveFailedRequests = false;
            boolean haveDoneRequests = false;
            for (R fr : fileRequests) {
                switch (fr.getState()) {
                case DONE:
                    haveDoneRequests = true;
                    break;
                case FAILED:
                case CANCELED:
                    haveFailedRequests = true;
                    break;
                default:
                    haveNonFinalRequests = true;
                    break;
                }
            }

            if (!getState().isFinal() && !haveNonFinalRequests) {
                stopUpdating();

                if (haveFailedRequests) {
                    // no running, no ready and  no pending  requests
                    // there are only failed requests
                    // we can fail this request

                    // REVISIT: the above logic seems flawed: this branch is reached
                    //          if there are multiple file requests, some in state
                    //          Done and others in state Failed.  The behaviour may
                    //          be correct, but should be checked.

                    setState(State.FAILED, "File requests have failed.");
                } else if (haveDoneRequests) {
                    // all requests are done
                    setState(State.DONE, "All file requests succeeded.");
                } else {
                    // we should never be here, but we have this block
                    // in case request is restored with no files in it
                    LOGGER.error("request state is unknown or no files in request!!!");
                    setState(State.FAILED, "Request state is unknown or no files in request!!!");
                }
            }
        } catch (IllegalStateTransition e) {
            LOGGER.error("Illegal State Transition : {}", e.getMessage());
        } finally {
            wunlock();
        }
    }

    protected TReturnStatus getTReturnStatus()  {
        updateStatus();

        String description;

        rlock();
        try {
            description = latestHistoryEvent();
            TStatusCode statusCode = getStatusCode();
            if (statusCode != null) {
                return new TReturnStatus(statusCode, description);
           }
        } finally {
            runlock();
        }

        // we used to synchronize on this container request here, but
        // it does not make sence as the file requests are being processed
        // by multiple threads,  and synchronizing here can cause a deadlock
        // once all field access is made sycnhronized and file request need to
        // access container request fields
        // we can rely on the fact that
        // once file request reach their final state, this state does not change
        // so the combined logic

        int failed_req           = 0;
        int failed_space_expired = 0;
        int failed_no_free_space = 0;
        int canceled_req         = 0;
        int pending_req          = 0;
        int running_req          = 0;
        int ready_req            = 0;
        int done_req             = 0;
        boolean failure = false;
        for (R fr : fileRequests) {
            TReturnStatus fileReqRS = fr.getReturnStatus();
            TStatusCode fileReqSC   = fileReqRS.getStatusCode();
            if( fileReqSC == TStatusCode.SRM_REQUEST_QUEUED) {
                pending_req++;
            }
            else if(fileReqSC == TStatusCode.SRM_REQUEST_INPROGRESS) {
                running_req++;
            }
            else if(fileReqSC == TStatusCode.SRM_FILE_PINNED ||
                    fileReqSC == TStatusCode.SRM_SPACE_AVAILABLE) {
                ready_req++;
            }
            else if(fileReqSC == TStatusCode.SRM_SUCCESS ||
                    fileReqSC == TStatusCode.SRM_RELEASED) {
                done_req++;
            }
            else if(fileReqSC == TStatusCode.SRM_ABORTED) {
                canceled_req++;
                failure=true;
            }
            else if(fileReqSC == TStatusCode.SRM_NO_FREE_SPACE) {
                failed_no_free_space++;
                failure=true;
            }
            else if(fileReqSC == TStatusCode.SRM_SPACE_LIFETIME_EXPIRED) {
                failed_space_expired++;
                failure=true;
            }
            else if(RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                failed_req++;
                failure=true;
            }
            else {
                LOGGER.error("Unknown request status code {} for request {}", fr.getState(), fr.getId());
            }
        }

        int len = getNumOfFileRequest();
        if (canceled_req == len ) {
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        }

        if (failed_req==len) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        }
        if (ready_req==len || done_req==len || ready_req+done_req==len ) {
            if (failure) {
                return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, null);
            }
            else {
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            }
        }
        if (pending_req==len) {
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        }
        // SRM space is not enough to hold all requested SURLs for free. (so me thinks one fails - all fail)
        if (failed_no_free_space>0) {
            return new TReturnStatus(TStatusCode.SRM_NO_FREE_SPACE, description);
        }
        // space associated with the targetSpaceToken is expired. (so me thinks one fails - all fail)
        if (failed_space_expired>0) {
            return new TReturnStatus(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED, description);
        }
        // we still have work to do:
        if (running_req > 0 || pending_req > 0) {
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        }
        else {
            // all are done here
            if (failure) {
                if (ready_req > 0 || done_req > 0 ) {
                    //some succeeded some not
                    return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS, null);
                }
                else {
                    //none succeeded
                    return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
                }
            }
            else {
                //no single failure - we should not get to this piece if code
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            }
        }
    }


    public TRequestSummary getRequestSummary() {
        TRequestSummary summary = new TRequestSummary();
        summary.setStatus(getTReturnStatus());
        summary.setRequestType(getRequestType());
        summary.setRequestToken(String.valueOf(getId()));
        int total_num = getNumOfFileRequest();
        summary.setTotalNumFilesInRequest(total_num);
        int num_of_failed=0;
        int num_of_completed = 0;
        int num_of_waiting = 0;
        for(int i = 0; i< total_num; ++i) {
            R fr;
            rlock();
            try {
                fr = fileRequests.get(i);
            } finally {
                runlock();
            }
            TReturnStatus fileReqRS = fr.getReturnStatus();
            TStatusCode fileReqSC = fileReqRS.getStatusCode();
            if( fileReqSC == TStatusCode.SRM_REQUEST_QUEUED) {
                num_of_waiting++;
            } else if(fileReqSC == TStatusCode.SRM_SUCCESS ||
                    fileReqSC == TStatusCode.SRM_RELEASED) {
                num_of_completed ++;
            } else if (RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                num_of_failed ++;
            }
        }
        summary.setNumOfFailedFiles(num_of_failed);
        summary.setNumOfCompletedFiles(num_of_completed);
        summary.setNumOfWaitingFiles(num_of_waiting);

        return summary;

    }

    protected abstract TRequestType getRequestType();


    /**
     * check the object for the equality with this request
     * <p>
     * we return true only if object is this request
     * @param o
     * object to check for equality with
     * @return
     * result of the check
     */
    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return (int) (getId() ^ getId() >> 32);
    }


    /**
     * Provide a simple human-friendly name for requests of this type.
     */
    protected abstract String getNameForRequestType();

    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(getNameForRequestType()).append(" id:").append(getClientRequestId());
        sb.append(" files:").append(fileRequests.size());
        sb.append(" state:").append(getState());
        TStatusCode code = getStatusCode();
        if (code != null) {
            sb.append(" status:").append(code);
        }
        sb.append(" by:").append(getUser().getDisplayName());
        if (longformat) {
            sb.append('\n');
            long now = System.currentTimeMillis();
            sb.append("   Submitted: ").append(relativeTimestamp(getCreationTime(), now)).append('\n');
            sb.append("   Expires: ").append(relativeTimestamp(getCreationTime() + getLifetime(), now)).append('\n');
            if (this instanceof DelegatedCredentialAware) {
                Long id = ((DelegatedCredentialAware)this).getCredentialId();
                RequestCredential credential = RequestCredential.getRequestCredential(id);
                if (credential != null) {
                    sb.append("   Credential: ").append(credential.getCredentialName()).append('\n');
                }
            }
            sb.append("   History:\n");
            sb.append(getHistory("   "));
            for (R fr:fileRequests) {
                sb.append("\n");
                fr.toString(sb, "   ", longformat);
            }
        }
    }

    protected void fileRequestStateChanged(R request)
    {
        switch (request.getState()) {
        case RQUEUED:
        case READY:
        case DONE:
        case CANCELED:
        case FAILED:
            _stateChangeCounter.increment();
        }
    }

    @Nonnull
    public abstract R getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException;

    protected List<R> getFileRequests()  {
        return fileRequests;
    }

    @Override
    public void tryToReady() {
        fileRequests.forEach(FileRequest::tryToReady);
    }

    /**
     * Constructs a Date object using the given milliseconds time
     * value relative to the current point in time.
     *
     * Equivalent to 'new Date(System.currentTimeMillis() + delta)'
     * except that underflows and overflows are taken into account.
     *
     * Used by subclasses.
     *
     * @param delta milliseconds relative to the current point in time
     */
    protected Date getDateRelativeToNow(long delta)
    {
        long now = System.currentTimeMillis();
        if (delta >= 0 && now >= Long.MAX_VALUE - delta) {
            return new Date(Long.MAX_VALUE);
        } else {
            return new Date(now + delta);
        }
    }
}
