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
 * FileRequest.java
 *
 * Created on July 5, 2002, 12:04 PM
 */

package org.dcache.srm.request;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.CheckedFuture;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Objects;

import org.dcache.srm.CopyCallbacks;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TCopyRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public final class CopyFileRequest extends FileRequest<CopyRequest> implements DelegatedCredentialAware
{
    private static final Logger LOGGER = LoggerFactory.getLogger(CopyFileRequest.class);

    private final URI sourceSurl;
    private final URI destinationSurl;
    private URI sourceTurl;
    private URI destinationTurl;
    private String localSourcePath;
    private String localDestinationPath;
    private long size;
    private String destinationFileId;
    private String remoteRequestId;
    private String remoteFileId;
    private String transferId;
    private SRMException transferError;
    //these are used if the transfer is performed in the pull mode for
    // storage of the space reservation related info
    private final String spaceReservationId;
    private final ImmutableMap<String,String> extraInfo;
    private final Long credentialId;

    public CopyFileRequest(long requestId,
                           Long requestCredentalId,
                           URI sourceSurl,
                           URI destinationSurl,
                           String spaceToken,
                           long lifetime,
                           ImmutableMap<String,String> extraInfo)
    {
        super(requestId, lifetime);
        LOGGER.debug("CopyFileRequest");
        this.sourceSurl = sourceSurl;
        this.destinationSurl = destinationSurl;
        this.spaceReservationId = spaceToken;
        this.extraInfo = extraInfo;
        this.credentialId = requestCredentalId;
        LOGGER.debug("constructor from={} to={}", sourceSurl, destinationSurl);
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */
    public CopyFileRequest(long id,
            Long nextJobId,
            JobStorage<CopyFileRequest> jobStorage,
            long creationTime,
            long lifetime,
            int stateId,
            String scheduelerId,
            long schedulerTimeStamp,
            int numberOfRetries,
            long lastStateTransitionTime,
            JobHistory[] jobHistoryArray,
            long requestId,
            Long requestCredentalId,
            String statusCodeString,
            String sourceSurl,
            String destinationSurl,
            String sourceTurl,
            String destinationTurl,
            String localSourcePath,
            String localDestinationPath,
            long size,
            String fromFileId,
            String toFileId,
            String remoteRequestId,
            String remoteFileId,
            String spaceReservationId,
            String transferId,
            ImmutableMap<String,String> extraInfo)
    {
        super(id, nextJobId, creationTime, lifetime, stateId,
              scheduelerId, schedulerTimeStamp, numberOfRetries,
              lastStateTransitionTime, jobHistoryArray,
              requestId, statusCodeString);
        this.sourceSurl = URI.create(sourceSurl);
        this.destinationSurl = URI.create(destinationSurl);
        if (sourceTurl != null && !sourceTurl.equalsIgnoreCase("null")) {
            this.sourceTurl = URI.create(sourceTurl);
        }
        if (destinationTurl != null && !destinationTurl.equalsIgnoreCase("null")) {
            this.destinationTurl = URI.create(destinationTurl);
        }
        this.localSourcePath = localSourcePath;
        this.localDestinationPath = localDestinationPath;
        this.size = size;
        this.destinationFileId = toFileId;
        if (remoteRequestId != null && (!remoteRequestId.equalsIgnoreCase("null"))) {
            this.remoteRequestId = remoteRequestId;
        }
        if (remoteFileId != null && (!remoteFileId.equalsIgnoreCase("null"))) {
            this.remoteFileId = remoteFileId;
        }
        this.spaceReservationId = spaceReservationId;
        this.transferId = transferId;
        this.extraInfo = extraInfo;
        this.credentialId = requestCredentalId;
    }

    @Override
    public Long getCredentialId()
    {
        return credentialId;
    }

    private void done()
    {
        LOGGER.debug("done");
    }

    public ImmutableMap<String,String> getExtraInfo()
    {
        return extraInfo;
    }

    /**
     * The source location if remote, null otherwise.
     */
    public URI getSourceTurl()
    {
        rlock();
        try {
            return sourceTurl;
        } finally {
            runlock();
        }
    }

    /**
     * Set the source location; implies the source is remote.
     */
    protected void setSourceTurl(URI location)
    {
        wlock();
        try {
            this.sourceTurl = location;
        } finally {
            wunlock();
        }
    }

    /**
     * The destination location if remote, null otherwise.
     */
    public URI getDestinationTurl()
    {
        rlock();
        try {
            return destinationTurl;
        } finally {
            runlock();
        }
    }

    /**
     * Set the destination location; implies the source is remote.
     */
    protected void setDestinationTurl(URI location)
    {
        wlock();
        try {
            this.destinationTurl = location;
        } finally {
            wunlock();
        }
    }

    /** Getter for property size.
     * @return Value of property size.
     */
    public long getSize()
    {
        rlock();
        try {
            return size;
        } finally {
            runlock();
        }
    }

    /** Setter for property size.
     * @param size New value of property size.
     */
    protected void setSize(long size)
    {
        rlock();
        try {
            this.size = size;
        } finally {
            runlock();
        }

        reassessLifetime(size);
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat)
    {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Copy ");
        }
        sb.append("file id:").append(getId());
        sb.append(" state:").append(getState());
        if (longformat) {
            sb.append(" source=");
            appendPathSurlAndTurl(sb, getLocalSourcePath(), getSourceSurl(),
                    getSourceTurl());
            sb.append(" destination=");
            appendPathSurlAndTurl(sb, getLocalDestinationPath(),
                    getDestinationSurl(), getDestinationTurl());
            sb.append('\n');
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    private static void appendPathSurlAndTurl(StringBuilder sb,
            String path, URI surl, URI turl)
    {
        if (path != null) {
            sb.append(path);
        } else {
            if (surl.getScheme().equalsIgnoreCase("srm")) {
                sb.append(surl);
                if (turl != null) {
                    sb.append(" --> ").append(turl);
                }
            } else {
                sb.append(turl);
            }
        }
    }

    /**
     * The absolute path of the source if local, or null if remote.
     */
    public String getLocalSourcePath()
    {
        rlock();
        try {
            return localSourcePath;
        } finally {
            runlock();
        }
    }

    /**
     * Set the absolute path of source; implies the source is local.
     */
    protected void setLocalSourcePath(String path)
    {
        wlock();
        try {
            this.localSourcePath = path;
        } finally {
            wunlock();
        }
    }

    /**
     * The absolute path of the destination if local, or null.
     */
    public String getLocalDestinationPath()
    {
        rlock();
        try {
            return localDestinationPath;
        } finally {
            runlock();
        }
    }

    /**
     * Set the absolute path of destination; implies the destination is local.
     */
    protected void setLocalDestinationPath(String path)
    {
        wlock();
        try {
            this.localDestinationPath = path;
        } finally {
            wunlock();
        }
    }

    /** Getter for property toFileId.
     * @return Value of property toFileId.
     *
     */
    private String getDestinationFileId()
    {
        rlock();
        try {
            return destinationFileId;
        } finally {
            runlock();
        }
    }

    /** Setter for property toFileId.
     * @param id New value of property toFileId.
     *
     */
    private void setDestinationFileId(String id)
    {
        wlock();
        try {
            destinationFileId = id;
        } finally {
            wunlock();
        }
    }

    private void runLocalToLocalCopy() throws IllegalStateTransition, SRMException
    {
        LOGGER.debug("copying from local to local");
        FileMetaData fmd ;
        try {
            fmd = getStorage().getFileMetaData(getUser(), getSourceSurl(), true);
        } catch (SRMException srme) {
            try {
                setStateAndStatusCode(State.FAILED,
                                      srme.getMessage(),
                                      TStatusCode.SRM_INVALID_PATH);
            } catch (IllegalStateTransition ist) {
                LOGGER.error("Illegal State Transition : {}", ist.getMessage());
            }
            return;
        }
        setSize(fmd.size);

        if (getDestinationFileId() == null) {
            addHistoryEvent("Doing name space lookup.");
            LOGGER.debug("calling storage.prepareToPut({})", getLocalDestinationPath());
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(),
                            getDestinationSurl(),
                            size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), directExecutor());
            LOGGER.debug("callbacks.waitResult()");
            return;
        }

        LOGGER.debug("known source size is {}", size);

        try {
            getStorage().localCopy(getUser(), getSourceSurl(), getDestinationFileId());
            getStorage().putDone(getUser(), getDestinationFileId(), getDestinationSurl(), getContainerRequest().isOverwrite());
            setStateToDone();
        } catch (SRMException e) {
            getStorage().abortPut(null, getDestinationFileId(), getDestinationSurl(), e.getMessage());
            throw e;
        }
    }

    private void runRemoteToLocalCopy() throws IllegalStateTransition,
            SRMException
    {
        LOGGER.debug("copying from remote to local");
        RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
        if (getDestinationFileId() == null) {
            addHistoryEvent("Doing name space lookup.");
            LOGGER.debug("calling storage.prepareToPut({})", getLocalDestinationPath());
            CheckedFuture<String,? extends SRMException> future =
                    getStorage().prepareToPut(
                            getUser(), getDestinationSurl(), size,
                            Objects.toString(getContainerRequest().getTargetAccessLatency(), null),
                            Objects.toString(getContainerRequest().getTargetRetentionPolicy(), null),
                            getSpaceReservationId(),
                            getContainerRequest().isOverwrite());
            future.addListener(new PutCallbacks(getId(), future), directExecutor());
            LOGGER.debug("callbacks.waitResult");
            return;
        }
        LOGGER.debug("known source size is {}", size);

        if (getTransferId() == null) {
            addHistoryEvent("started remote transfer, waiting completion");
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId()) {
                @Override
                public void copyComplete()
                {
                    try {
                        getStorage().putDone(
                                getUser(),
                                getDestinationFileId(),
                                getDestinationSurl(),
                                getContainerRequest().isOverwrite());
                        super.copyComplete();
                    } catch (SRMException e) {
                        copyFailed(e);
                    }
                }
            };
            setTransferId(getStorage().getFromRemoteTURL(getUser(), getSourceTurl(), getDestinationFileId(), getUser(), credential.getId(), extraInfo, copycallbacks));
            saveJob(true);
        } else {
            // transfer id is not null and we are scheduled
            // there was some kind of error during the transfer

            getStorage().killRemoteTransfer(getTransferId());
            SRMException transferError = getTransferError();
            getStorage().abortPut(null, getDestinationFileId(), getDestinationSurl(), transferError.getMessage());
            setDestinationFileId(null);
            setTransferId(null);
            throw transferError;
        }
    }

    private void setStateToDone()
    {
        if (!getState().isFinal()) {
            try {
                setState(State.DONE, "completed");

                try {
                    getContainerRequest().fileRequestCompleted();
                } catch (SRMInvalidRequestException ire) {
                    LOGGER.error("Failed to find container request: {}", ire.toString());
                }
            } catch (IllegalStateTransition ist) {
                LOGGER.error("Failed to set copy file request state to DONE: {}",
                        ist.getMessage());
            }
        }
    }

    private void setStateToFailed(String error)
    {
        if (!getState().isFinal()) {
            try {
                setState(State.FAILED, error);

                try {
                    getContainerRequest().fileRequestCompleted();
                } catch (SRMInvalidRequestException e) {
                    LOGGER.error("Failed to find container request: {}", e.toString());
                }
            } catch (IllegalStateTransition ist) {
                LOGGER.error("Failed to set copy file request state to FAILED: {}",
                        ist.getMessage());
            }
        }
    }

    private void runLocalToRemoteCopy() throws SRMException, IllegalStateTransition
    {
        if (getTransferId() == null) {
            LOGGER.debug("copying using storage.putToRemoteTURL");
            RequestCredential credential = RequestCredential.getRequestCredential(credentialId);
            TheCopyCallbacks copycallbacks = new TheCopyCallbacks(getId());
            setTransferId(getStorage().putToRemoteTURL(getUser(), getSourceSurl(), getDestinationTurl(), getUser(), credential.getId(), extraInfo, copycallbacks));
            addHistoryEvent("Transferring file.");
            saveJob(true);
        } else {
            // transfer id is not null and we are scheduled
            // there was some kind of error durign the transfer
            getStorage().killRemoteTransfer(getTransferId());
            setTransferId(null);
            throw getTransferError();
        }
    }

    @Override
    public void run() throws SRMException, IllegalStateTransition
    {
        LOGGER.trace("run");
        if (!getState().isFinal()) {
            if (getLocalDestinationPath() != null && getLocalSourcePath() != null) {
                runLocalToLocalCopy();
            } else if (getLocalDestinationPath() != null && getSourceTurl() != null) {
                runRemoteToLocalCopy();
            } else if (getDestinationTurl() != null && getLocalSourcePath() != null) {
                runLocalToRemoteCopy();
            } else {
                LOGGER.error("Unknown combination of to/from ursl");
                setStateToFailed("Unknown combination of to/from ursl");
            }
        }
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        if (newState.isFinal()) {
            if (getTransferId() != null && newState != State.DONE) {
                getStorage().killRemoteTransfer(getTransferId());
                String toFileId = getDestinationFileId();
                if (toFileId != null) {
                    try {
                        Exception transferError = getTransferError();
                        getStorage().abortPut(null, toFileId, getDestinationSurl(),
                                              (transferError == null) ? null : transferError.getMessage());
                    } catch (SRMException e) {
                        LOGGER.error("Failed to abort copy: {}", e.getMessage());
                    }
                }
            }
            String remoteRequestId = getRemoteRequestId();
            if (remoteRequestId != null) {
                if (getLocalSourcePath() != null) {
                    remoteFileRequestDone(getDestinationSurl(), remoteRequestId, getRemoteFileId());
                } else {
                    remoteFileRequestDone(getSourceSurl(), remoteRequestId, getRemoteFileId());
                }
            }
        }

        super.processStateChange(newState, description);
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSourceSurl()) || surl.equals(getDestinationSurl());
    }

    private void remoteFileRequestDone(URI SURL,String remoteRequestId,String remoteFileId)
    {
        try {
            LOGGER.debug("setting remote file status to Done, SURL={} " +
                    "remoteRequestId={} remoteFileId={}", SURL,
                    remoteRequestId, remoteFileId);
            getContainerRequest().remoteFileRequestDone(SURL.toString(),
                    remoteRequestId, remoteFileId);
        } catch (Exception e) {
            LOGGER.error("set remote file status to done failed, surl={}, " +
                    "requestId={}, fileId={}", SURL, remoteRequestId, remoteFileId);
        }
    }

    /** Getter for property remoteFileId.
     * @return Value of property remoteFileId.
     *
     */
    public String getRemoteFileId()
    {
        rlock();
        try {
            return remoteFileId;
        } finally {
            runlock();
        }
    }

    /** Getter for property remoteRequestId.
     * @return Value of property remoteRequestId.
     *
     */
    public String getRemoteRequestId()
    {
        rlock();
        try {
            return remoteRequestId;
        } finally {
            runlock();
        }
    }
    /**
     * Getter for property from_surl.
     * @return Value of property from_surl.
     */
    public URI getSourceSurl()
    {
        return sourceSurl;
    }
    /**
     * Getter for property to_surl.
     * @return Value of property to_surl.
     */
    public URI getDestinationSurl()
    {
        return destinationSurl;
    }
    /**
     * Setter for property remoteRequestId.
     * @param remoteRequestId New value of property remoteRequestId.
     */
    protected void setRemoteRequestId(String remoteRequestId)
    {
        wlock();
        try {
            this.remoteRequestId = remoteRequestId;
        } finally {
            wunlock();
        }
    }
    /**
     * Setter for property remoteFileId.
     * @param remoteFileId New value of property remoteFileId.
     */
    protected void setRemoteFileId(String remoteFileId)
    {
        wlock();
        try {
            this.remoteFileId = remoteFileId;
        } finally {
            wunlock();
        }
    }
    /**
     * Getter for property spaceReservationId.
     * @return Value of property spaceReservationId.
     */
    public String getSpaceReservationId()
    {
        return spaceReservationId;
    }

    /**
     * @param transferId the transferId to set
     */
    private void setTransferId(String transferId)
    {
        wlock();
        try {
            this.transferId = transferId;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the transferError
     */
    private SRMException getTransferError()
    {
        rlock();
        try {
            return transferError;
        } finally {
            runlock();
        }
    }

    /**
     * @param transferError the transferError to set
     */
    private void setTransferError(SRMException transferError)
    {
        wlock();
        try {
            this.transferError = transferError;
        } finally {
            wunlock();
        }
    }

    private static class PutCallbacks implements Runnable
    {
        private final long fileRequestJobId;
        private final CheckedFuture<String, ? extends SRMException> future;

        public PutCallbacks(long fileRequestJobId, CheckedFuture<String, ? extends SRMException> future)
        {
            this.fileRequestJobId = fileRequestJobId;
            this.future = future;
        }

        @Override
        public void run()
        {
            try {
                CopyFileRequest fr = Job.getJob(fileRequestJobId, CopyFileRequest.class);
                try {
                    String fileId = future.checkedGet();
                    State state = fr.getState();
                    if (state == State.INPROGRESS) {
                        LOGGER.debug("PutCallbacks success for file {}", fr.getDestinationSurl());
                        fr.setDestinationFileId(fileId);
                        fr.saveJob(true);
                        Scheduler.getScheduler(fr.getSchedulerId()).execute(fr);
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            e.getMessage(),
                            e.getStatusCode());
                }
            } catch (IllegalStateTransition e) {
                LOGGER.error("Illegal State Transition: {}", e.getMessage());
            } catch (SRMInvalidRequestException e) {
                LOGGER.error(e.getMessage());
            }
        }
    }

    private static class TheCopyCallbacks implements CopyCallbacks
    {
        private final long fileRequestJobId;

        public TheCopyCallbacks(long fileRequestJobId)
        {
            this.fileRequestJobId = fileRequestJobId;
        }

        private CopyFileRequest getCopyFileRequest()
                throws SRMInvalidRequestException
        {
            return Job.getJob(fileRequestJobId, CopyFileRequest.class);
        }

        @Override
        public void copyComplete()
        {
            try {
                CopyFileRequest copyFileRequest = getCopyFileRequest();
                LOGGER.debug("copy succeeded");
                copyFileRequest.setStateToDone();
            } catch (SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
            }
        }

        @Override
        public void copyFailed(SRMException e)
        {
            CopyFileRequest copyFileRequest;
            try {
                copyFileRequest = getCopyFileRequest();
            } catch (SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
                return;
            }
            copyFileRequest.setTransferError(e);
            LOGGER.error("copy failed: {}", e.getMessage());
            State state = copyFileRequest.getState();
            Scheduler scheduler = Scheduler.getScheduler(copyFileRequest.getSchedulerId());
            if (!state.isFinal() && scheduler != null) {
                scheduler.execute(copyFileRequest);
            }
        }
    }

    public TCopyRequestFileStatus getTCopyRequestFileStatus() throws SRMInvalidRequestException
    {
        TCopyRequestFileStatus copyRequestFileStatus = new TCopyRequestFileStatus();
        copyRequestFileStatus.setFileSize(new UnsignedLong(size));
        copyRequestFileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        copyRequestFileStatus.setRemainingFileLifetime((int) (getRemainingLifetime() / 1000));
        org.apache.axis.types.URI sourceSurl;
        org.apache.axis.types.URI destinationSurl;
        try {
            sourceSurl = new org.apache.axis.types.URI(getDestinationSurl().toASCIIString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOGGER.error(e.toString());
            throw new SRMInvalidRequestException("wrong SURL format: " + getDestinationSurl());
        }
        try {
            destinationSurl = new org.apache.axis.types.URI(getSourceSurl().toASCIIString());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOGGER.error(e.toString());
            throw new SRMInvalidRequestException("wrong SURL format: " + getSourceSurl());
        }
        copyRequestFileStatus.setSourceSURL(destinationSurl);
        copyRequestFileStatus.setTargetSURL(sourceSurl);
        TReturnStatus returnStatus = getReturnStatus();
        if (TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            returnStatus = new TReturnStatus(TStatusCode.SRM_FAILURE, null);
        }
        copyRequestFileStatus.setStatus(returnStatus);
        return copyRequestFileStatus;
    }

    @Override
    protected TReturnStatus getReturnStatus()
    {
        String description = latestHistoryEvent();
        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            return new TReturnStatus(statusCode, description);
        }
        switch (getState()) {
        case DONE:
            return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
        case READY:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        case TRANSFERRING:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        case FAILED:
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        case CANCELED:
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        case QUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        case INPROGRESS:
        case RQUEUED:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, description);
        }
    }

    /**
     * Getter for property transferId.
     * @return Value of property transferId.
     */
    public String getTransferId()
    {
        return transferId;
    }

    /**
     *
     *
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *  -1 stands for infinite lifetime
     */
    @Override
    public long extendLifetime(long newLifetime) throws SRMException
    {
        long remainingLifetime = getRemainingLifetime();
        if (remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        long requestLifetime = getContainerRequest().extendLifetimeMillis(newLifetime);
        if (requestLifetime <newLifetime) {
            newLifetime = requestLifetime;
        }
        if (remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        return extendLifetimeMillis(newLifetime);
    }
}
