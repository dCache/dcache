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
import org.springframework.dao.DataAccessException;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Objects;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMDuplicationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidPathException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public final class PutFileRequest extends FileRequest<PutRequest> {
    private static final Logger logger = LoggerFactory.getLogger(PutFileRequest.class);
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
            Long requestCredentalId,
            URI url,
            @Nullable Long size,
            long lifetime,
            int maxNumberOfRetires,
            @Nullable String spaceReservationId,
            @Nullable TRetentionPolicy retentionPolicy,
            @Nullable TAccessLatency accessLatency)
    {
        super(requestId,
                requestCredentalId,
                lifetime,
                maxNumberOfRetires);
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
            String errorMessage,
            String scheduelerId,
            long schedulerTimeStamp,
            int numberOfRetries,
            int maxNumberOfRetries,
            long lastStateTransitionTime,
            JobHistory[] jobHistoryArray,
            long requestId,
            Long requestCredentalId,
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
              errorMessage,
              scheduelerId,
              schedulerTimeStamp,
              numberOfRetries,
              maxNumberOfRetries,
              lastStateTransitionTime,
              jobHistoryArray,
              requestId,
              requestCredentalId,
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

    public final void setFileId(String fileId) {
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

    @Override
    public RequestFileStatus getRequestFileStatus() {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = (int) getId();

        rfs.SURL = getSurlString();
        rfs.size = (getSize() == null) ? 0 : getSize();
        State state = getState();
        rfs.TURL = getTurlString();
        if(state == State.DONE) {
            rfs.state = "Done";
        } else if(state == State.READY) {
            rfs.state = "Ready";
        } else if(state == State.TRANSFERRING) {
            rfs.state = "Running";
        } else if(state == State.FAILED
                || state == State.CANCELED ) {
            rfs.state = "Failed";
        } else {
            rfs.state = "Pending";
        }

        logger.debug(" returning requestFileStatus for "+this.toString());
        return rfs;
    }

    public TPutRequestFileStatus getTPutRequestFileStatus()
            throws SRMInvalidRequestException {
        TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
        fileStatus.setFileSize(((getSize() == null) ? null : new UnsignedLong(getSize())));

        org.apache.axis.types.URI anSurl;
        try {
            anSurl= new org.apache.axis.types.URI(getSurlString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
        fileStatus.setSURL(anSurl);
        //fileStatus.set

        String turlstring = getTurlString();
        if(turlstring != null) {
            org.apache.axis.types.URI transferURL;
            try {
                transferURL = new org.apache.axis.types.URI(turlstring);
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                logger.error(e.toString());
                throw new SRMInvalidRequestException("wrong turl format");
            }
            fileStatus.setTransferURL(transferURL);

        }
        fileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        fileStatus.setRemainingPinLifetime((int)getRemainingLifetime()/1000);
        TReturnStatus returnStatus = getReturnStatus();
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
        if (getPriority() != 0) {
            sb.append(" priority:").append(getPriority());
        }
        State state = getState();
        sb.append(" state:").append(state);
        if(longformat) {
            sb.append('\n');
            sb.append(padding).append("   SURL: ").append(getSurlString()).append('\n');
            sb.append(padding).append("   TURL: ").append(getTurlString()).append('\n');
            if (getSize() != null) {
                sb.append(padding).append("   Size: ").append(getSize()).append('\n');
            }
            sb.append(padding).append("   AccessLatency: ").append(getAccessLatency()).append('\n');
            sb.append(padding).append("   RetentionPolicy: ").append(getRetentionPolicy()).append('\n');
            sb.append(padding).append("   spaceReservation: ").append(getSpaceReservationId()).append('\n');
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History of State Transitions:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure
    {
        addDebugHistoryEvent("run method is executed");
        try {
            if (getFileId() == null) {
                // [SRM 2.2, 5.5.2, t)] Upon srmPrepareToPut, SURL entry is inserted to the name space, and any
                // methods that access the SURL such as srmLs, srmBringOnline and srmPrepareToGet must return
                // SRM_FILE_BUSY at the file level. If another srmPrepareToPut or srmCopy is requested on
                // the same SURL, SRM_FILE_BUSY must be returned if the SURL can be overwritten, otherwise
                // SRM_DUPLICATION_ERROR must be returned at the file level.
                for (PutFileRequest request : SRM.getSRM().getActiveFileRequests(PutFileRequest.class, getSurl())) {
                    if (request != this) {
                        if (!getContainerRequest().isOverwrite()) {
                            setStateAndStatusCode(
                                    State.FAILED,
                                    "The requested SURL is locked by another upload.",
                                    TStatusCode.SRM_DUPLICATION_ERROR);
                        } else {
                            setStateAndStatusCode(
                                    State.FAILED,
                                    "The requested SURL is locked by another upload.",
                                    TStatusCode.SRM_FILE_BUSY);
                        }
                        return;
                    }
                }


                addDebugHistoryEvent("selecting transfer protocol");
                // if we can not read this path for some reason
                //(not in ftp root for example) this will throw exception
                // we do not care about the return value yet
                PutRequest request = getContainerRequest();
                String[] supportedProts = getStorage().supportedPutProtocols();
                boolean found_supp_prot=false;
                String[] requestProtocols = request.getProtocols();
                mark1:
                for(String supportedProtocol:supportedProts) {
                    for(String requestProtocol:requestProtocols) {
                        if(supportedProtocol.equals(requestProtocol)) {
                            found_supp_prot = true;
                            break mark1;
                        }
                    }
                }

                if(!found_supp_prot) {
                    throw new FatalJobFailure("transfer protocols not supported");
                }

                setState(State.ASYNCWAIT, "Doing name space lookup.");
                CheckedFuture<String, ? extends SRMException> future =
                        getStorage().prepareToPut(
                                getUser(),
                                getSurl(),
                                getSize(),
                                Objects.toString(getAccessLatency(), null),
                                Objects.toString(getRetentionPolicy(), null),
                                getSpaceReservationId(),
                                getContainerRequest().isOverwrite());
                future.addListener(new PutCallbacks(getId(), future), MoreExecutors.sameThreadExecutor());
                return;
            }

            try {
                computeTurl();
            } catch (SRMAuthorizationException e) {
                String error = e.getMessage();
                logger.error(error);
                try {
                    setStateAndStatusCode(State.FAILED,
                            error,
                            TStatusCode.SRM_AUTHORIZATION_FAILURE);
                } catch (IllegalStateTransition ist) {
                    logger.error(ist.getMessage());
                }
                return;
            } catch (SRMException e) {
                String error = "cannot obtain turl for file:" + e.getMessage();
                logger.error(error);
                try {
                    setState(State.FAILED, error);
                } catch (IllegalStateTransition ist) {
                    logger.error(ist.getMessage());
                }
                return;
            }

            logger.debug("run() returns, scheduler should bring file request into the ready state eventually");
        }
        catch(SRMException | DataAccessException | IllegalStateTransition e) {
            throw new FatalJobFailure("cannot prepare to put: " + e.getMessage());
        }
    }


    @Override
    protected void stateChanged(State oldState) {
        State state = getState();
        logger.debug("State changed from "+oldState+" to "+getState());
        if(state == State.READY) {
            try {
                getContainerRequest().resetRetryDeltaTime();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
        try {
            if (state == State.FAILED && getFileId() != null) {
                String reason = getLastJobChange().getDescription();
                getStorage().abortPut(getUser(), getFileId(), getSurl(), reason);
            }
        } catch (SRMException e) {
            logger.error("Failed to abort put after failure: {}", e.getMessage());
        }
        super.stateChanged(oldState);
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
                if (statusCode != null) {
                    return new TReturnStatus(statusCode, "Upload failed.");
                }
                return new TReturnStatus(TStatusCode.SRM_FAILURE, "Upload failed.");
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
    public void setStatus(SRMUser user, String status) throws SRMException
    {
        if (status.equalsIgnoreCase("Done")) {
            done(user);
        } else {
            super.setStatus(user, status);
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
    public TReturnStatus getReturnStatus()
    {
        String description = getLastJobChange().getDescription();
        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            if (statusCode == TStatusCode.SRM_SUCCESS || statusCode == TStatusCode.SRM_SPACE_AVAILABLE) {
                description = null;
            }
            return new TReturnStatus(statusCode, description);
        }

        switch (getState()) {
        case PENDING:
        case TQUEUED:
        case RETRYWAIT:
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
     * @return the aTurl
     */
    public final URI getTurl() {
        rlock();
        try {
            return turl;
        } finally {
            runlock();
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

        public PutCallbacks(long fileRequestJobId, CheckedFuture<String, ? extends SRMException> future)
        {
            this.fileRequestJobId = fileRequestJobId;
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
                    if(state == State.ASYNCWAIT) {
                        logger.trace("Storage info arrived for file {}.", fr.getSurlString());
                        fr.setFileId(fileId);
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        } catch(Exception ie) {
                            logger.error(ie.toString());
                        }
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                }
            } catch (IllegalStateTransition ist) {
                if (!ist.getFromState().isFinal()) {
                    logger.error(ist.getMessage());
                }
            } catch (SRMInvalidRequestException e) {
                logger.warn(e.getMessage());
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
