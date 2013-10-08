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

import org.apache.axis.types.UnsignedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;

import java.net.URI;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.UnpinCallbacks;
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

    private URI surl;
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
    String url,
    long lifetime,
    int maxNumberOfRetries

    ) throws Exception {
        super(requestId,
            requestCredentalId,
            lifetime,
            maxNumberOfRetries);
        logger.debug("GetFileRequest, requestId="+requestId+" fileRequestId = "+getId());
        surl = URI.create(url);
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
        rlock();
        try {
            return surl;
        } finally {
            runlock();
        }
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
        wlock();
        try {
            State state = getState();
            if(getTurl() == null && (state == State.READY ||
            state == State.TRANSFERRING)) {
                try {
                    setTurl(getTURL());
                }
                catch(SRMAuthorizationException srmae) {
                    String error =srmae.getMessage();
                    logger.error(error);
                    try {
                        setStateAndStatusCode(
                                State.FAILED,
                                error,
                                TStatusCode.SRM_AUTHORIZATION_FAILURE);
                    }
                    catch(IllegalStateTransition ist) {
                        logger.warn("Illegal State Transition : " +ist.getMessage());
                    }

                }
                catch(Exception srme) {
                    String error =
                    "can not obtain turl for file:"+srme;
                    logger.error(error);
                    try {
                        setState(State.FAILED,error);
                    }
                    catch(IllegalStateTransition ist) {
                        logger.warn("Illegal State Transition : " +ist.getMessage());
                    }
                }
            }

            if(getTurl()!= null) {
                return getTurl().toASCIIString();
            }
        } finally {
            wunlock();
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
        TReturnStatus returnStatus = getReturnStatus();
        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    public TSURLReturnStatus  getTSURLReturnStatus() throws SRMInvalidRequestException {
        TReturnStatus returnStatus = getReturnStatus();
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        try {
            surlReturnStatus.setSurl(new org.apache.axis.types.URI(getSurlString()));
        } catch (org.apache.axis.types.URI.MalformedURIException e) {
            logger.error(e.toString());
            throw new SRMInvalidRequestException("wrong surl format");
        }
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }

    private URI getTURL() throws SRMException {
        String firstDcapTurl = null;
        GetRequest request = getContainerRequest();
        if (request != null) {
            firstDcapTurl = request.getFirstDcapTurl();
            if (firstDcapTurl == null) {
                URI turl =
                    getStorage().getGetTurl(getUser(),
                                            getSurl(),
                                            request.protocols);
                if (turl == null) {
                    throw new SRMException("turl is null");
                }
                if (turl.getScheme().equals("dcap")) {
                    request.setFirstDcapTurl(turl.toString());
                }
                return turl;
            }
        }

        return getStorage().getGetTurl(getUser(),
                                       getSurl(),
                                       URI.create(firstDcapTurl));
    }

    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(" GetFileRequest ");
        sb.append(" id:").append(getId());
        sb.append(" priority:").append(getPriority());
        sb.append(" creator priority:");
        try {
            sb.append(getUser().getPriority());
        } catch (SRMInvalidRequestException ire) {
            sb.append("Unknown");
        }
        State state = getState();
        sb.append(" state:").append(state);
        if(longformat) {
            sb.append('\n').append("   SURL: ").append(getSurlString());
            sb.append('\n').append("   pinned: ").append(isPinned());
            String thePinId = getPinId();
            if(thePinId != null) {
                sb.append('\n').append("   pinid: ").append(thePinId);
            }
            sb.append('\n').append("   TURL: ").append(getTurlString());
            sb.append('\n').append("   status code:").append(getStatusCode());
            sb.append('\n').append("   error message:").append(getErrorMessage());
            sb.append('\n').append("   History of State Transitions: \n");
            sb.append(getHistory());
        }
    }

    @Override
    public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
        logger.debug("run()");
        try {
            if(getPinId() == null) {
                // [ SRM 2.2, 5.2.2, g)] The file request must fail with an error SRM_FILE_BUSY
                // if srmPrepareToGet requests for files which there is an active srmPrepareToPut
                // (no srmPutDone is yet called) request for.
                //
                // [ SRM 2.2, 5.1.3] SRM_FILE_BUSY: client requests for a file which there is an
                // active srmPrepareToPut (no srmPutDone is yet called) request for.
                if (SRM.getSRM().isFileBusy(surl)) {
                    setStateAndStatusCode(State.FAILED, "The requested SURL is being used by another client.",
                            TStatusCode.SRM_FILE_BUSY);
                }

                pinFile();
                if(getPinId() == null) {
                    setState(State.ASYNCWAIT, "pinning file");
                    logger.debug("GetFileRequest: waiting async notification about pinId...");
                    return;
                }
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
        getStorage().pinFile(getUser(),
                             surl,
                             getContainerRequest().getClient_host(),
                             lifetime,
                             String.valueOf(getRequestId()),
                             new ThePinCallbacks(getId()));
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

        if(State.isFinalState(state)) {
            if(getFileId() != null && getPinId() != null) {
                UnpinCallbacks callbacks = new TheUnpinCallbacks(this.getId());
                logger.info("state changed to final state, unpinning fileId= "+ getFileId()+" pinId = "+getPinId());
                SRMUser user;
                try {
                    user = getUser();
                } catch (SRMInvalidRequestException ire) {
                    logger.error(ire.toString()) ;
                    return;
                }
                getStorage().unPinFile(user,getFileId(),callbacks, getPinId());
            }
        }

        super.stateChanged(oldState);
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    @Override
    public TReturnStatus getReturnStatus() {
        State state = getState();
        if (getStatusCode() != null) {
            return new TReturnStatus(getStatusCode(), state.toString());
        } else if(state == State.DONE) {
            return new TReturnStatus(TStatusCode.SRM_RELEASED, state.toString());
        }
        else if(state == State.READY) {
            return new TReturnStatus(TStatusCode.SRM_FILE_PINNED, state.toString());
        }
        else if(state == State.TRANSFERRING) {
            return new TReturnStatus(TStatusCode.SRM_FILE_PINNED, state.toString());
        }
        else if(state == State.FAILED) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, "FAILED: " + getErrorMessage());
        }
        else if(state == State.CANCELED ) {
            return new TReturnStatus(TStatusCode.SRM_ABORTED, state.toString());
        }
        else if(state == State.TQUEUED ) {
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, state.toString());
        }
        else if(state == State.RUNNING ||
                state == State.RQUEUED ||
                state == State.ASYNCWAIT ) {
            return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS, state.toString());
        }
        else {
            return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, state.toString());
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
                setState(State.DONE, "TURL released");
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            case DONE:
                return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
            case CANCELED:
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL has been aborted and cannot be released");
            case FAILED:
                return new TReturnStatus(TStatusCode.SRM_FAILURE, "Pinning failed");
            default:
                setState(State.CANCELED, "Aborted by srmReleaseFile request");
                return new TReturnStatus(TStatusCode.SRM_ABORTED, "SURL is not yet pinned, pinning aborted");
            }
        } catch (IllegalStateTransition e) {
            return new TReturnStatus(TStatusCode.SRM_FAILURE, e.getMessage());
        } finally {
            wunlock();
        }
    }

    private  static class ThePinCallbacks implements PinCallbacks {

        private final long fileRequestJobId;

        public ThePinCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public GetFileRequest getGetFileRequest()
            throws SRMInvalidRequestException
        {
            return Job.getJob(fileRequestJobId, GetFileRequest.class);
        }

        @Override
        public void FileNotFound(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setStateAndStatusCode(State.FAILED,
                        reason,
                        TStatusCode.SRM_INVALID_PATH);
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.warn("GetCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Unavailable(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setStateAndStatusCode(State.FAILED,
                                             reason,
                                             TStatusCode.SRM_FILE_UNAVAILABLE);
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("GetCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Error( String error) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setState(State.FAILED,error);
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("ThePinCallbacks error: "+ error);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Exception( Exception e) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setState(State.FAILED,e.toString());
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("ThePinCallbacks exception",e);
            }
            catch(Exception e1) {
                logger.error(e1.toString());
            }
        }




        @Override
        public void Timeout() {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setState(State.FAILED,"ThePinCallbacks Timeout");
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("GetCallbacks Timeout");
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Pinned(FileMetaData fileMetaData, String pinId) {
            try {
                logger.debug("File pinned (pinId={})", pinId);

                GetFileRequest fr = getGetFileRequest();
                fr.wlock();
                try {
                    State state = fr.getState();
                    if (state == State.ASYNCWAIT || state == State.RUNNING) {
                        fr.setFileId(fileMetaData.fileId);
                        fr.setFileMetaData(fileMetaData);
                        fr.setPinId(pinId);
                        if (state == State.ASYNCWAIT) {
                            Scheduler scheduler =
                                Scheduler.getScheduler(fr.getSchedulerId());
                            scheduler.schedule(fr);
                        }
                    }
                } finally {
                    fr.wunlock();
                }
            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                Thread.currentThread().interrupt();
            } catch (SRMInvalidRequestException e) {
                logger.error("BringOnlineFileRequest failed: {}", e.getMessage());
            } catch (IllegalStateTransition e) {
                logger.warn("Illegal State Transition: {}", e.getMessage());
            }
        }

        @Override
        public void PinningFailed(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("ThePinCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

    }

    private  static class TheUnpinCallbacks implements UnpinCallbacks {

        private final long fileRequestJobId;

        public TheUnpinCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public GetFileRequest getGetFileRequest()
                throws SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, GetFileRequest.class);
        }

        @Override
        public void Error( String error) {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.error("TheUnpinCallbacks error: "+ error);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Exception( Exception e) {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.error("TheUnpinCallbacks exception",e);
            }
            catch(Exception e1) {
                logger.error(e1.toString());
            }
        }




        @Override
        public void Timeout() {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.error("TheUnpinCallbacks Timeout");
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Unpinned(String pinId) {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.debug("TheUnpinCallbacks: Unpinned() pinId:"+pinId);
                State state = fr.getState();
               if(state == State.ASYNCWAIT) {
                    fr.setPinId(pinId);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    }
                    catch(Exception ie) {
                        logger.error(ie.toString());
                    }
                }
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void UnpinningFailed(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.error("TheUnpinCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }
}
