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

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Objects;

import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileBusyException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public final class PutFileRequest extends FileRequest<PutRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutFileRequest.class);

    private final URI surl;
    private final Long size;
    private URI turl;
    private String fileId;
    @Nullable
    private final String spaceReservationId;
    @Nullable
    private final TAccessLatency accessLatency;
    @Nullable
    private final TRetentionPolicy retentionPolicy;

    public PutFileRequest(long requestId,
            URI url,
            @Nullable Long size,
            long lifetime,
            @Nullable String spaceReservationId,
            @Nullable TRetentionPolicy retentionPolicy,
            @Nullable TAccessLatency accessLatency)
    {
        super(requestId, lifetime);
        this.surl = url;
        this.size = size;
        this.spaceReservationId = spaceReservationId;
        this.accessLatency = accessLatency;
        this.retentionPolicy = retentionPolicy;
    }

    public PutFileRequest(
            long id,
            Long nextJobId,
            long creationTime,
            long lifetime,
            int stateId,
            String scheduelerId,
            long schedulerTimeStamp,
            int numberOfRetries,
            long lastStateTransitionTime,
            JobHistory[] jobHistoryArray,
            long requestId,
            String statusCodeString,
            String SURL,
            @Nullable String TURL,
            @Nullable String fileId,
            @Nullable String spaceReservationId,
            @Nullable Long size,
            @Nullable TRetentionPolicy retentionPolicy,
            @Nullable TAccessLatency accessLatency)
    {
        super(id,
              nextJobId,
              creationTime,
              lifetime,
              stateId,
              scheduelerId,
              schedulerTimeStamp,
              numberOfRetries,
              lastStateTransitionTime,
              jobHistoryArray,
              requestId,
              statusCodeString);

        this.surl = URI.create(SURL);
        if (TURL != null && !TURL.equalsIgnoreCase("null")) {
            this.turl = URI.create(TURL);
        }

        if(fileId != null && (!fileId.equalsIgnoreCase("null"))) {
            this.fileId = fileId;
        }
        this.spaceReservationId = spaceReservationId;
        this.size = size;
        this.accessLatency = accessLatency;
        this.retentionPolicy = retentionPolicy;
    }

    @Nullable
    public final String getFileId() {
        rlock();
        try {
            return fileId;
        } finally {
            runlock();
        }
    }

    protected final void setFileId(String fileId) {
        wlock();
        try {
            this.fileId = fileId;
        } finally {
            wunlock();
        }
    }

    @Nullable
    public final Long getSize() {
        return size;
    }

    public final URI getSurl() {
        return surl;
    }

    public final String getSurlString() {
        return getSurl().toASCIIString();
    }

    public String getTurlString()
    {
        rlock();
        try {
            if (turl != null) {
                return turl.toASCIIString();
            }
        } finally {
            runlock();
        }
        return null;
    }

    protected TPutRequestFileStatus getTPutRequestFileStatus()
            throws SRMInvalidRequestException {
        TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
        fileStatus.setFileSize(((getSize() == null) ? null : new UnsignedLong(getSize())));

        org.apache.axis.types.URI anSurl;
        try {
            anSurl= new org.apache.axis.types.URI(getSurlString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOGGER.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
        fileStatus.setSURL(anSurl);
        //fileStatus.set

        TReturnStatus returnStatus = getReturnStatus();

        String turlstring = getTurlString();
        if(turlstring != null && TStatusCode.SRM_SPACE_AVAILABLE.equals(returnStatus.getStatusCode())) {
            org.apache.axis.types.URI transferURL;
            try {
                transferURL = new org.apache.axis.types.URI(turlstring);
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                LOGGER.error("Generated broken TURL \"{}\": {}", turlstring, e);
                throw new SRMInvalidRequestException("wrong turl format");
            }
            fileStatus.setTransferURL(transferURL);

        }
        fileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        fileStatus.setRemainingPinLifetime((int)getRemainingLifetime()/1000);
        if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            //SRM_SPACE_LIFETIME_EXPIRED is illeal on the file level,
            // but we use it to correctly calculate the request level status
            // so we do the translation here
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, null);
        }

        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat) {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Put ");
        }
        sb.append("file id:").append(getId());
        State state = getState();
        sb.append(" state:").append(state);
        if(longformat) {
            sb.append('\n');
            sb.append(padding).append("   SURL: ").append(getSurlString()).append('\n');
            sb.append(padding).append("   TURL: ").append(getTurlString()).append('\n');
            if (getSize() != null) {
                sb.append(padding).append("   Size: ").append(getSize()).append('\n');
            }
            TAccessLatency al = getAccessLatency();
            if (al != null) {
                sb.append(padding).append("   Access latency: ").append(al).append('\n');
            }
            TRetentionPolicy rp = getRetentionPolicy();
            if (rp != null) {
                sb.append(padding).append("   Retention policy: ").append(rp).append('\n');
            }
            String space = getSpaceReservationId();
            if (space != null) {
                sb.append(padding).append("   Space reservation: ").append(space).append('\n');
            }
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    @Override
    public void run() throws IllegalStateTransition, SRMException
    {
        LOGGER.trace("run");
        if (!getState().isFinal()) {
            if (getFileId() == null) {
                // [SRM 2.2, 5.5.2, t)] Upon srmPrepareToPut, SURL entry is inserted to the name space, and any
                // methods that access the SURL such as srmLs, srmBringOnline and srmPrepareToGet must return
                // SRM_FILE_BUSY at the file level. If another srmPrepareToPut or srmCopy is requested on
                // the same SURL, SRM_FILE_BUSY must be returned if the SURL can be overwritten, otherwise
                // SRM_DUPLICATION_ERROR must be returned at the file level.
                if (SRM.getSRM().hasMultipleUploads(getSurl())) {
                    if (!getContainerRequest().isOverwrite()) {
                        throw new SRMDuplicationException("The requested SURL is locked by another upload.");
                    } else {
                        throw new SRMFileBusyException("The requested SURL is locked by another upload.");
                    }
                }

                addHistoryEvent("Doing name space lookup.");
                SRMUser user = getUser();
                CheckedFuture<String, ? extends SRMException> future =
                        getStorage().prepareToPut(
                                user,
                                getSurl(),
                                getSize(),
                                Objects.toString(getAccessLatency(), null),
                                Objects.toString(getRetentionPolicy(), null),
                                getSpaceReservationId(),
                                getContainerRequest().isOverwrite());
                future.addListener(new PutCallbacks(user, getId(), surl, future), MoreExecutors.directExecutor());
                return;
            }

            computeTurl();

            wlock();
            try {
                if (getState() == State.INPROGRESS) {
                    setState(State.RQUEUED, "Putting on a \"Ready\" Queue.");
                }
            } finally {
                wunlock();
            }
        }
    }


    @Override
    protected void processStateChange(State newState, String description)
    {
        State oldState = getState();
        LOGGER.debug("State changed from {} to {}", oldState, newState);
        if (newState == State.READY) {
            try {
                getContainerRequest().resetRetryDeltaTime();
            } catch (SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
            }
        }
        try {
            if (newState == State.FAILED && getFileId() != null) {
                getStorage().abortPut(getUser(), getFileId(), getSurl(), description);
            }
        } catch (SRMException e) {
            LOGGER.error("Failed to abort put after failure: {}", e.getMessage());
        }

        super.processStateChange(newState, description);
    }

    private void computeTurl() throws SRMException
    {
        PutRequest request = getContainerRequest();
        // do not synchronize on request, since it might cause deadlock
        URI turl = getStorage().getPutTurl(request.getUser(), getFileId(), request.getProtocols(), request.getPreviousTurl());
        request.setPreviousTurl(turl);
        setTurl(turl);
    }

    @Override
    public void abort(String reason) throws IllegalStateTransition, SRMException
    {
        wlock();
        try {
            /* [ SRM 2.2, 5.12.2 ]
             *
             * a) srmAbortFiles aborts all files in the request regardless of the state.
             * d) When aborting srmPrepareToPut request before srmPutDone and before the
             *    file transfer, the SURL must not exist as the result of the successful
             *    abort on the SURL. Any srmRm request on the SURL must fail.
             * e) When aborting srmPrepareToPut request before srmPutDone and after the
             *    file transfer, the SURL may exist, and a srmRm request on the SURL may
             *    remove the requested SURL.
             * f) When aborting srmPrepareToPut request after srmPutDone, it must be failed
             *    for those files. An explicit srmRm is required to remove those successfully
             *    completed files for srmPrepareToPut.
             * g) srmAbortFiles must not change the request level status of the completed
             *    requests. Once a request is completed, the status of the request remains
             *    the same.
             *
             * We interpret this to mean that a put request in a non-final state is
             * cancelled, while the abort request itself fails if the put request has
             * finished successfully. Otherwise nothing happens.
             */
            State state = getState();
            if (!state.isFinal()) {
                if (getFileId() != null) {
                    getStorage().abortPut(getUser(), getFileId(), getSurl(), reason);
                }
                setState(State.CANCELED, "Request aborted.");
            } else if (state == State.DONE) {
                throw new IllegalStateTransition("Put request completed successfully and cannot be aborted",
                        State.DONE, State.CANCELED);
            }
        } finally {
            wunlock();
        }
    }

    public TReturnStatus done(SRMUser user)
    {
        wlock();
        try {
            switch (getState()) {
            case READY:
            case TRANSFERRING:
                try {
                    getStorage().putDone(user, getFileId(), getSurl(), getContainerRequest().isOverwrite());
                    setState(State.DONE, "SrmPutDone called.");
                    return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                } catch (SRMAuthorizationException e) {
                    setStateAndStatusCode(State.FAILED, e.getMessage(), TStatusCode.SRM_AUTHORIZATION_FAILURE);
                    return new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE, e.getMessage());
                } catch (SRMDuplicationException e) {
                    setStateAndStatusCode(State.FAILED, e.getMessage(), TStatusCode.SRM_DUPLICATION_ERROR);
                    return new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR, e.getMessage());
                } catch (SRMInvalidPathException e) {
                    setStateAndStatusCode(State.FAILED, e.getMessage(), TStatusCode.SRM_INVALID_PATH);
                    return new TReturnStatus(TStatusCode.SRM_INVALID_PATH, e.getMessage());
                } catch (SRMException e) {
                    setStateAndStatusCode(State.FAILED, e.getMessage(), TStatusCode.SRM_INTERNAL_ERROR);
                    return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR, e.getMessage());
                }
            case DONE:
                return new TReturnStatus(TStatusCode.SRM_DUPLICATION_ERROR, "File exists already.");
            case CANCELED:
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "The SURL has been aborted.");
            case FAILED:
                TStatusCode statusCode = getStatusCode();
                String description = latestHistoryEvent();
                if (statusCode != null) {
                    return new TReturnStatus(statusCode, description);
                }
                return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
            default:
                setStateAndStatusCode(State.FAILED, "SrmPutDone called before TURL was made available.", TStatusCode.SRM_INVALID_PATH);
                return new TReturnStatus(TStatusCode.SRM_INVALID_PATH, "File does not exist.");
            }
        } catch (IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "Scheduling failure.");
        } finally {
            wunlock();
        }
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    /**
     * Getter for property spaceReservationId.
     * @return Value of property spaceReservationId.
     */
    @Nullable
    public final String getSpaceReservationId() {
        return spaceReservationId;
    }

    @Override
    protected TReturnStatus getReturnStatus()
    {
        String description = latestHistoryEvent();
        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            if (statusCode == TStatusCode.SRM_SUCCESS || statusCode == TStatusCode.SRM_SPACE_AVAILABLE) {
                description = null;
            }
            return new TReturnStatus(statusCode, description);
        }

        switch (getState()) {
        case UNSCHEDULED:
        case QUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        case READY:
        case TRANSFERRING:
            return new TReturnStatus(TStatusCode.SRM_SPACE_AVAILABLE, null);
        case DONE:
            // REVISIT: Spec doesn't allow this for statusOfPut
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        case CANCELED:
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        case FAILED:
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        }
    }

    /**
     * @param turl the aTurl to set
     */
    public final void setTurl(URI turl) {
        wlock();
        try {
            this.turl = turl;
        } finally {
            wunlock();
        }
    }

    private static class PutCallbacks implements Runnable
    {
        private final CheckedFuture<String, ? extends SRMException> future;
        private final long fileRequestJobId;
        private final SRMUser user;
        private final URI surl;

        public PutCallbacks(SRMUser user, long fileRequestJobId, URI surl,
                            CheckedFuture<String, ? extends SRMException> future)
        {
            this.user = user;
            this.fileRequestJobId = fileRequestJobId;
            this.surl = surl;
            this.future = future;
        }

        @Override
        public void run()
        {
            try {
                PutFileRequest fr = Job.getJob(fileRequestJobId, PutFileRequest.class);

                try {
                    String fileId = future.checkedGet();

                    State state = fr.getState();
                    switch (state) {
                    case INPROGRESS:
                        LOGGER.trace("Storage info arrived for file {}.", fr.getSurlString());
                        fr.setFileId(fileId);
                        fr.saveJob(true);
                        Scheduler.getScheduler(fr.getSchedulerId()).execute(fr);
                        break;
                    case CANCELED:
                    case FAILED:
                        fr.getStorage().abortPut(fr.getUser(), fileId, fr.getSurl(), fr.latestHistoryEvent());
                        break;
                    default:
                        LOGGER.error("Put request is in an unexpected state in callback: {}", state);
                        fr.getStorage().abortPut(fr.getUser(), fileId, fr.getSurl(), fr.latestHistoryEvent());
                        break;
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                }
            } catch (IllegalStateTransition ist) {
                if (!ist.getFromState().isFinal()) {
                    LOGGER.error(ist.getMessage());
                }
            } catch (SRMInvalidRequestException e) {
                try {
                    String fileId = future.checkedGet();
                    SRM.getSRM().getStorage().abortPut(user, fileId, surl, "Request was aborted while being prepared.");
                } catch (SRMException ignored) {
                }
            }
        }
    }

    @Nullable
    public final TAccessLatency getAccessLatency() {
        return accessLatency;
    }

    @Nullable
    public final TRetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    /**
     *
     *
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *    -1 stands for infinite lifetime
     */
    @Override
    public long extendLifetime(long newLifetime) throws SRMException {
        long remainingLifetime = getRemainingLifetime();
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        long requestLifetime = getContainerRequest().extendLifetimeMillis(newLifetime);
        if(requestLifetime <newLifetime) {
            newLifetime = requestLifetime;
        }
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        return extendLifetimeMillis(newLifetime);
    }
}
