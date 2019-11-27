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

import com.google.common.util.concurrent.CheckedFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileBusyException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 * @version
 */
public final class GetFileRequest extends FileRequest<GetRequest> {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetFileRequest.class);

    private final URI surl;
    private URI turl;
    private String pinId;
    private String fileId;

    /**
     * Transient, since we do not need to replicate the field,
     * the field is needed only in jvm where the request is originally
     * scheduled
     */
    private transient FileMetaData fileMetaData;

    /** Creates new FileRequest */
    public GetFileRequest(long requestId,
                          URI surl,
                          long lifetime)
    {
        super(requestId, lifetime);
        LOGGER.debug("GetFileRequest, requestId={} fileRequestId = {}", requestId, getId());
        this.surl = surl;
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */

    public GetFileRequest(
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
    String TURL,
    String fileId,
    String pinId
    ) {
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
        if(TURL != null && !TURL.equalsIgnoreCase("null")) {
            this.turl = URI.create(TURL);
        }

        if(fileId != null && (!fileId.equalsIgnoreCase("null"))) {
            this.fileId = fileId;
        }

        if(pinId != null && (!pinId.equalsIgnoreCase("null"))) {
            this.pinId = pinId;
        }
    }

    private void setPinId(String pinId) {
        wlock();
        try {
            this.pinId = pinId;
        } finally {
            wunlock();
        }
    }

    public String getPinId() {
        rlock();
        try {
            return pinId;
        } finally {
            runlock();
        }
    }

    private boolean isPinned() {
        return getPinId() != null;
    }

    public URI getSurl() {
        return surl;
    }

    public final String getSurlString() {
        return getSurl().toASCIIString();
    }

    public String getTurlString()  {
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


    public String getFileId() {
        rlock();
        try {
           return fileId;
        } finally {
            runlock();
        }
    }

    protected TGetRequestFileStatus getTGetRequestFileStatus()
            throws SRMInvalidRequestException {
        TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
        if(getFileMetaData() != null) {
            fileStatus.setFileSize(new UnsignedLong(getFileMetaData().size));
        }

        try {
             fileStatus.setSourceSURL(new org.apache.axis.types.URI(getSurlString()));
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            LOGGER.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }

        TReturnStatus returnStatus = getReturnStatus();
        String turlstring = getTurlString();
        if(turlstring != null && TStatusCode.SRM_FILE_PINNED.equals(returnStatus.getStatusCode())) {
            try {
            fileStatus.setTransferURL(new org.apache.axis.types.URI(turlstring));
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                LOGGER.error("Generated broken TURL \"{}\": {}", turlstring, e);
                throw new SRMInvalidRequestException("wrong turl format");
            }

        }
        if(this.isPinned()) {

            fileStatus.setRemainingPinTime((int)(getRemainingLifetime()/1000));
        }
        fileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat) {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Get ");
        }
        sb.append("file id:").append(getId());
        State state = getState();
        sb.append(" state:").append(state);
        if(longformat) {
            sb.append('\n');
            sb.append(padding).append("   SURL: ").append(getSurlString()).append('\n');
            sb.append(padding).append("   Pinned: ").append(isPinned()).append('\n');
            String thePinId = getPinId();
            if(thePinId != null) {
                sb.append(padding).append("   Pin id: ").append(thePinId).append('\n');
            }
            sb.append(padding).append("   TURL: ").append(getTurlString()).append('\n');
            TStatusCode status = getStatusCode();
            if (status != null) {
                sb.append(padding).append("   Status:").append(status).append('\n');
            }
            sb.append(padding).append("   History:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    @Override
    public synchronized void run()
            throws SRMException, IllegalStateTransition
    {
        LOGGER.trace("run");
        if (!getState().isFinal()) {
            if (getPinId() == null) {
                // [ SRM 2.2, 5.2.2, g)] The file request must fail with an error SRM_FILE_BUSY
                // if srmPrepareToGet requests for files which there is an active srmPrepareToPut
                // (no srmPutDone is yet called) request for.
                //
                // [ SRM 2.2, 5.1.3] SRM_FILE_BUSY: client requests for a file which there is an
                // active srmPrepareToPut (no srmPutDone is yet called) request for.
                if (SRM.getSRM().isFileBusy(surl)) {
                    throw new SRMFileBusyException("The requested SURL is locked by an upload.");
                }

                addHistoryEvent("Pinning file.");
                pinFile(getContainerRequest());
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

    protected void pinFile(GetRequest request)
    {
        URI surl = getSurl();
        LOGGER.info("Pinning {}", surl);
        CheckedFuture<AbstractStorageElement.Pin,? extends SRMException> future =
                getStorage().pinFile(
                        request.getUser(),
                        surl,
                        request.getClient_host(),
                        getLifetime(),
                        String.valueOf(getRequestId()),
                        request.isStagingAllowed());
        LOGGER.trace("GetFileRequest: waiting async notification about pinId...");
        future.addListener(new ThePinCallbacks(getId(), future), MoreExecutors.directExecutor());
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        State oldState = getState();
        LOGGER.debug("State changed from {} to {}", oldState, newState);
        switch (newState) {
        case READY:
            try {
                getContainerRequest().resetRetryDeltaTime();
            } catch (SRMInvalidRequestException ire) {
                LOGGER.error(ire.toString());
            }
            break;

        case DONE:
        case FAILED:
        case CANCELED:
            AbstractStorageElement storage = getStorage();
            try {
                SRMUser user = getUser();
                String fileId = getFileId();
                String pinId = getPinId();
                if (fileId != null && pinId != null) {
                    LOGGER.info("State changed to final state, unpinning fileId = {} pinId = {}.", fileId, pinId);
                    CheckedFuture<String, ? extends SRMException> future = storage.unPinFile(null, fileId, pinId);
                    future.addListener(() -> {
                        try {
                            LOGGER.debug("Unpinned (pinId={}).", future.checkedGet());
                        } catch (SRMException e) {
                            LOGGER.error("Unpinning failed: {}", e.getMessage());
                        }
                    }, MoreExecutors.directExecutor());
                } else {
                    BringOnlineFileRequest.unpinBySURLandRequestToken(storage, user, String.valueOf(getRequestId()), getSurl());
                }
            } catch (SRMInternalErrorException | SRMInvalidRequestException e) {
                LOGGER.error(e.toString()) ;
            }
        }

        super.processStateChange(newState, description);
    }

    private void computeTurl() throws SRMException
    {
        GetRequest request = getContainerRequest();
        URI turl = getStorage().getGetTurl(request.getUser(), getSurl(), request.getProtocols(), request.getPreviousTurl());
        request.setPreviousTurl(turl);
        setTurl(turl);
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    @Override
    protected TReturnStatus getReturnStatus()
    {
        String description = latestHistoryEvent();

        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            if (statusCode == TStatusCode.SRM_DONE || statusCode == TStatusCode.SRM_FILE_PINNED) {
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
            return new TReturnStatus(TStatusCode.SRM_FILE_PINNED, null);
        case DONE:
            return new TReturnStatus(TStatusCode.SRM_RELEASED, null);
        case CANCELED:
            return new TReturnStatus(TStatusCode.SRM_ABORTED, description);
        case FAILED:
            return new TReturnStatus(TStatusCode.SRM_FAILURE, description);
        default:
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, description);
        }
    }


    /**
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

        newLifetime = extendLifetimeMillis(newLifetime);
        if(remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        if(getPinId() == null) {
            return newLifetime;
        }
        SRMUser user = getUser();
        return getStorage().extendPinLifetime(user,getFileId(), getPinId(),newLifetime);
    }

    /**
     * @param turl the turl to set
     */
    private void setTurl(URI turl) {
        wlock();
        try {
            this.turl = turl;
        } finally {
            wunlock();
        }
    }

    /**
     * @param fileId the fileId to set
     */
    private void setFileId(String fileId) {
        wlock();
        try {
            this.fileId = fileId;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the fileMetaData
     */
    private FileMetaData getFileMetaData() {
        rlock();
        try {
             return fileMetaData;
        } finally {
            runlock();
        }
    }

    /**
     * @param fileMetaData the fileMetaData to set
     */
    private void setFileMetaData(FileMetaData fileMetaData) {
        wlock();
        try {
            this.fileMetaData = fileMetaData;
        } finally {
            wunlock();
        }

        reassessLifetime(fileMetaData.size);
    }

    public TReturnStatus release()
    {
        wlock();
        try {
            State state = getState();
            switch (state) {
            case READY:
                setState(State.DONE, "TURL released.");
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            case DONE:
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            case CANCELED:
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL has been aborted and cannot be released");
            case FAILED:
                return new TReturnStatus(TStatusCode.SRM_FAILURE, "Pinning failed");
            default:
                setState(State.CANCELED, "Aborted by srmReleaseFile request.");
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL is not yet pinned, pinning aborted");
            }
        } catch (IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        } finally {
            wunlock();
        }
    }

    private  static class ThePinCallbacks implements Runnable
    {
        private final long fileRequestJobId;
        private final CheckedFuture<AbstractStorageElement.Pin, ? extends SRMException> future;

        public ThePinCallbacks(long fileRequestJobId,
                               CheckedFuture<AbstractStorageElement.Pin, ? extends SRMException> future)
        {
            this.fileRequestJobId = fileRequestJobId;
            this.future = future;
        }

        public GetFileRequest getGetFileRequest()
            throws SRMInvalidRequestException
        {
            return Job.getJob(fileRequestJobId, GetFileRequest.class);
        }

        @Override
        public void run()
        {
            try {
                GetFileRequest fr = getGetFileRequest();
                fr.wlock();
                try {
                    AbstractStorageElement.Pin pin = future.checkedGet();
                    LOGGER.debug("File pinned (pinId={}).", pin.pinId);
                    State state = fr.getState();
                    if (state == State.INPROGRESS) {
                        fr.setFileId(pin.fileMetaData.fileId);
                        fr.setFileMetaData(pin.fileMetaData);
                        fr.setPinId(pin.pinId);
                        Scheduler.getScheduler(fr.getSchedulerId()).execute(fr);
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(State.FAILED,
                                             e.getMessage(),
                                             e.getStatusCode());
                } finally {
                    fr.wunlock();
                }
            } catch (SRMInvalidRequestException e) {
                LOGGER.warn(e.getMessage());
            } catch (IllegalStateTransition e) {
                if (!e.getFromState().isFinal()) {
                    LOGGER.error(e.getMessage());
                }
            } catch (RuntimeException e) {
                LOGGER.error("Criticial failure in pinning (please report to support@dcache.org).", e);
            }
        }
    }

}
