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
import org.springframework.dao.DataAccessException;

import java.net.URI;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
/**
 *
 * @author  timur
 * @version
 */
public final class GetFileRequest extends FileRequest<GetRequest> {
    private final static Logger logger = LoggerFactory.getLogger(GetFileRequest.class);

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
                          Long  requestCredentalId,
                          URI surl,
                          long lifetime,
                          int maxNumberOfRetries)
    {
        super(requestId,
            requestCredentalId,
            lifetime,
            maxNumberOfRetries);
        logger.debug("GetFileRequest, requestId="+requestId+" fileRequestId = "+getId());
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
    String errorMessage,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    long requestId,
    Long  requestCredentalId,
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

    public void setPinId(String pinId) {
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

    public boolean isPinned() {
        return getPinId() != null;
    }

    public URI getSurl() {
        return surl;
    }

    public URI getTurl() {
        rlock();
        try {
            return turl;
        } finally {
            runlock();
        }
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


    @Override
    public RequestFileStatus getRequestFileStatus(){
        RequestFileStatus rfs;
        if(getFileMetaData() != null) {
            rfs = new RequestFileStatus(getFileMetaData());
        }
        else {
            rfs = new RequestFileStatus();
        }

        rfs.fileId = (int) getId();
        rfs.SURL = getSurlString();



        if(this.isPinned()) {
            rfs.isPinned = true;
            rfs.isCached = true;
        }

        State state = getState();
        rfs.TURL = getTurlString();
        if(state == State.DONE) {
            rfs.state = "Done";
        }
        else if(state == State.READY) {
            rfs.state = "Ready";
        }
        else if(state == State.TRANSFERRING) {
            rfs.state = "Running";
        }
        else if(state == State.FAILED
        || state == State.CANCELED ) {
            rfs.state = "Failed";
        }
        else {
            rfs.state = "Pending";
        }

        //logger.debug(" returning requestFileStatus for "+rfs.toString());
        return rfs;
    }

    public TGetRequestFileStatus getTGetRequestFileStatus()
            throws SRMInvalidRequestException {
        TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
        if(getFileMetaData() != null) {
            fileStatus.setFileSize(new UnsignedLong(getFileMetaData().size));
        }

        try {
             fileStatus.setSourceSURL(new org.apache.axis.types.URI(getSurlString()));
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }

        String turlstring = getTurlString();
        if(turlstring != null) {
            try {
            fileStatus.setTransferURL(new org.apache.axis.types.URI(turlstring));
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                logger.error(e.toString());
                throw new SRMInvalidRequestException("wrong turl format");
            }

        }
        if(this.isPinned()) {

            fileStatus.setRemainingPinTime((int)(getRemainingLifetime()/1000));
        }
        fileStatus.setEstimatedWaitTime(getContainerRequest().getRetryDeltaTime());
        fileStatus.setStatus(getReturnStatus());

        return fileStatus;
    }

    public TSURLReturnStatus  getTSURLReturnStatus() throws SRMInvalidRequestException
    {
        try {
            return new TSURLReturnStatus(new org.apache.axis.types.URI(getSurlString()), getReturnStatus());
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
    }

    @Override
    public void toString(StringBuilder sb, String padding, boolean longformat) {
        sb.append(padding);
        if (padding.isEmpty()) {
            sb.append("Get ");
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
            sb.append(padding).append("   History of State Transitions:\n");
            sb.append(getHistory(padding + "   "));
        }
    }

    @Override
    public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
        logger.trace("run()");
        try {
            if(getPinId() == null) {
                // [ SRM 2.2, 5.2.2, g)] The file request must fail with an error SRM_FILE_BUSY
                // if srmPrepareToGet requests for files which there is an active srmPrepareToPut
                // (no srmPutDone is yet called) request for.
                //
                // [ SRM 2.2, 5.1.3] SRM_FILE_BUSY: client requests for a file which there is an
                // active srmPrepareToPut (no srmPutDone is yet called) request for.
                if (SRM.getSRM().isFileBusy(surl)) {
                    setStateAndStatusCode(State.FAILED, "The requested SURL is locked by an upload.",
                            TStatusCode.SRM_FILE_BUSY);
                    return;
                }

                pinFile();
                if(getPinId() == null) {
                    setState(State.ASYNCWAIT, "Pinning file.");
                    logger.trace("GetFileRequest: waiting async notification about pinId...");
                    return;
                }
            }

            try {
                computeTurl();
            } catch (SRMAuthorizationException e) {
                String error = e.getMessage();
                logger.error(error);
                try {
                    setStateAndStatusCode(
                            State.FAILED,
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
                    setState(State.FAILED,error);
                } catch (IllegalStateTransition ist) {
                    logger.error(ist.getMessage());
                }
                return;
            }
        } catch (IllegalStateTransition | DataAccessException | SRMException e) {
            // FIXME some SRMException failures are temporary and others are
            // permanent.  Code currently doesn't distinguish between them and
            // always retries, even if problem isn't transitory.
            throw new NonFatalJobFailure(e.toString());
        }
        logger.info("PinId is "+getPinId()+" returning, scheduler should change state to \"Ready\"");

    }

    public void pinFile()
        throws NonFatalJobFailure, FatalJobFailure, SRMException
    {
        GetRequest request = getContainerRequest();
        if (!isProtocolSupported(request.protocols)) {
            throw new FatalJobFailure("Transfer protocols not supported");
        }

        URI surl = getSurl();
        logger.info("Pinning {}", surl);
        CheckedFuture<AbstractStorageElement.Pin,? extends SRMException> future =
                getStorage().pinFile(
                        getUser(),
                        surl,
                        getContainerRequest().getClient_host(),
                        lifetime,
                        String.valueOf(getRequestId()));
        future.addListener(new ThePinCallbacks(getId(), future), MoreExecutors.sameThreadExecutor());
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

        if(state.isFinal()) {
            if(getFileId() != null && getPinId() != null) {
                logger.info("state changed to final state, unpinning fileId= "+ getFileId()+" pinId = "+getPinId());
                SRMUser user;
                try {
                    user = getUser();
                } catch (SRMInvalidRequestException ire) {
                    logger.error(ire.toString()) ;
                    return;
                }
                final CheckedFuture<String, ? extends SRMException> future =
                        getStorage().unPinFile(user, getFileId(), getPinId());
                future.addListener(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        try {
                            String pinId = future.checkedGet();
                            logger.debug("Unpinned (pinId={}).", pinId);
                        } catch (SRMException e) {
                            logger.error("Unpinning failed: {}", e.getMessage());
                        }
                    }
                }, MoreExecutors.sameThreadExecutor());
            }
        }

        super.stateChanged(oldState);
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
    public TReturnStatus getReturnStatus()
    {
        String description = getLastJobChange().getDescription();

        TStatusCode statusCode = getStatusCode();
        if (statusCode != null) {
            if (statusCode == TStatusCode.SRM_DONE || statusCode == TStatusCode.SRM_FILE_PINNED) {
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
                    logger.debug("File pinned (pinId={}).", pin.pinId);
                    State state = fr.getState();
                    if (state == State.ASYNCWAIT || state == State.RUNNING) {
                        fr.setFileId(pin.fileMetaData.fileId);
                        fr.setFileMetaData(pin.fileMetaData);
                        fr.setPinId(pin.pinId);
                        if (state == State.ASYNCWAIT) {
                            Scheduler scheduler =
                                    Scheduler.getScheduler(fr.getSchedulerId());
                            scheduler.schedule(fr);
                        }
                    }
                } catch (SRMException e) {
                    fr.setStateAndStatusCode(State.FAILED,
                                             e.getMessage(),
                                             e.getStatusCode());
                } finally {
                    fr.wunlock();
                }
            } catch (SRMInvalidRequestException e) {
                logger.warn(e.getMessage());
            } catch (IllegalStateTransition e) {
                if (!e.getFromState().isFinal()) {
                    logger.error(e.getMessage());
                }
            }
        }
    }

}
