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

import java.net.URISyntaxException;
import java.net.URI;

import diskCacheV111.srm.RequestFileStatus;
import java.sql.SQLException;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.util.Tools;
import org.dcache.srm.GetFileInfoCallbacks;
import org.dcache.srm.PinCallbacks;
import org.dcache.srm.UnpinCallbacks;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.FatalJobFailure;

import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.TReturnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.SRMInvalidRequestException;
/**
 *
 * @author  timur
 * @version
 */
public final class BringOnlineFileRequest extends FileRequest {
    private final static Logger logger =
            LoggerFactory.getLogger(BringOnlineFileRequest.class);

    // the globus url class created from surl_string
    private URI surl;
    private String pinId;
    private String fileId;
    private transient FileMetaData fileMetaData;

    private static final long serialVersionUID = -9155373723705753177L;

    /** Creates new FileRequest */
    public BringOnlineFileRequest(Long requestId,
                                  Long requestCredentalId,
                                  URI surl,
                                  long lifetime,
                                  int maxNumberOfRetries)
    {
        super(requestId,
                requestCredentalId,
                lifetime,
                maxNumberOfRetries);
        logger.debug("BringOnlineFileRequest, requestId="+requestId+" fileRequestId = "+getId());
        this.surl = surl;
        updateMemoryCache();
    }

    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */

    public BringOnlineFileRequest(
    Long id,
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
    Long requestId,
    Long  requestCredentalId,
    String statusCodeString,
    String SURL,
    String fileId,
    String pinId
    ) throws java.sql.SQLException {
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
        rlock();
        try {
            return getPinId() != null;
        } finally {
            runlock();
        }

    }

    public URI getSurl() {
        rlock();
        try {
            return surl;
        } finally {
            runlock();
        }
    }


    public String getSurlString() {
        return getSurl().toString();
    }

    public String getFileId() {
        rlock();
        try {
            return fileId;
        } finally {
            runlock();
        }
    }


    public RequestFileStatus getRequestFileStatus(){
        RequestFileStatus rfs;
        if(fileMetaData != null) {
            rfs = new RequestFileStatus(fileMetaData);
        }
        else {
            rfs = new RequestFileStatus();
        }

        rfs.fileId = getId().intValue();
        rfs.SURL = getSurlString();


        if(this.isPinned()) {
            rfs.isPinned = true;
            rfs.isCached = true;
        }

        State state = getState();
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

    public TBringOnlineRequestFileStatus  getTGetRequestFileStatus()
    throws java.sql.SQLException, SRMInvalidRequestException{
        TBringOnlineRequestFileStatus fileStatus = new TBringOnlineRequestFileStatus();
        if(fileMetaData != null) {
            fileStatus.setFileSize(new org.apache.axis.types.UnsignedLong(fileMetaData.size));
        }

        try {
             fileStatus.setSourceSURL(new org.apache.axis.types.URI(getSurlString()));
        } catch (Exception e) {
            logger.error(e.toString());
            throw new java.sql.SQLException("wrong surl format");
        }

        if(this.isPinned()) {

            fileStatus.setRemainingPinTime((int)(getRemainingLifetime()/1000));
        }
        fileStatus.setEstimatedWaitTime((int)(getRequest().getRetryDeltaTime()));
        TReturnStatus returnStatus = getReturnStatus();
        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    public TSURLReturnStatus  getTSURLReturnStatus() throws java.sql.SQLException{
        TReturnStatus returnStatus = getReturnStatus();
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        try {
            surlReturnStatus.setSurl(new org.apache.axis.types.URI(getSurlString()));
        } catch (Exception e) {
            logger.error(e.toString());
            throw new java.sql.SQLException("wrong surl format");
        }
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }

    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(" BringOnlineFileRequest ");
        sb.append(" id:").append(getId());
        sb.append(" priority:").append(getPriority());
        sb.append(" creator priority:");
        try {
            sb.append(getUser().getPriority());
        } catch (SRMInvalidRequestException ire) {
            sb.append("Unknown");
        }
        sb.append(" state:").append(getState());
        if(longformat) {
            sb.append('\n').append("   SURL: ").append(getSurl());
            sb.append('\n').append("   pinned: ").append(isPinned());
            String thePinId = getPinId();
            if(thePinId != null) {
                sb.append('\n').append("   pinid: ").append(thePinId);
            }
            sb.append('\n').append("   status code:").append(getStatusCode());
            sb.append('\n').append("   error message:").append(getErrorMessage());
            sb.append('\n').append("   History of State Transitions: \n");
            sb.append(getHistory());
        }
    }

    public final void run() throws NonFatalJobFailure, FatalJobFailure {
        logger.debug("run()");
        try {
            if(getFileId() == null) {
                logger.debug("fileId is null, asking to get a fileId");
                askFileId();
                if(getFileId() == null) {
                    setState(State.ASYNCWAIT, "getting file Id");
                    logger.debug("BringOnlineFileRequest: waiting async notification about fileId...");
                    return;
                }
            }
            logger.debug("fileId = "+getFileId());

            if(getPinId() == null) {

                // do not check explicitely if we can read the file
                // this is done by pnfs manager when we call askFileId()

                logger.debug("pinId is null, asking to pin ");
                pinFile();
                if(getPinId() == null) {
                    setState(State.ASYNCWAIT,"pinning file");
                    logger.debug("BringOnlineFileRequest: waiting async notification about pinId...");
                    return;
                }
            }
        } catch(IllegalStateTransition ist) {
            throw new NonFatalJobFailure("Illegal State Transition : " +ist.getMessage());
        }
        logger.info("PinId is "+getPinId()+" returning, scheduler should change" +
            " state to \"Ready\"");

    }

    public void askFileId() throws NonFatalJobFailure, FatalJobFailure {
        try {

            logger.debug(" proccessing the file request id "+getId());
            URI surl = getSurl();
            logger.debug(" path is "+surl);
            // if we can not read this path for some reason
            //(not in ftp root for example) this will throw exception
            // we do not care about the return value yet
            logger.debug("calling Job.getJob("+requestId+")");
            BringOnlineRequest request = (BringOnlineRequest)
                Job.getJob(requestId);
            logger.debug("this file request's request is  "+request);
            //this will fail if the protocols are not supported
            String[] protocols = request.getProtocols();
            if(protocols != null && protocols.length > 0) {
                String[] supported_prots = getStorage().supportedGetProtocols();
                boolean found_supp_prot=false;
                mark1:
                for(String supported_protocol: supported_prots) {
                    for(String request_protocol: protocols) {
                        if(supported_protocol.equals(request_protocol)) {
                            found_supp_prot = true;
                            break mark1;
                        }
                    }
                }
                if(!found_supp_prot) {
                    StringBuilder request_protocols = new StringBuilder("transfer protocols not supported: [");
                    for(String request_protocol: protocols ) {
                        request_protocols.append(request_protocol);
                        request_protocols.append(',');
                    }
                    int len = request_protocols.length();
                    request_protocols.replace(len-1, len,"]");
                    throw new FatalJobFailure(request_protocols.toString());
                }
            }
            //storage.getGetTurl(getUser(),path,request.protocols);
            logger.debug("storage.prepareToGet("+surl+",...)");
            GetFileInfoCallbacks callbacks = new GetCallbacks(getId());
            getStorage().getFileInfo(getUser(),
                                     surl,
                                     true,
                                     callbacks);
        }
        catch(Exception e) {
            logger.error(e.toString());
            throw new NonFatalJobFailure(e.toString());
        }
    }

    public void pinFile() throws NonFatalJobFailure, FatalJobFailure {
        try {

            PinCallbacks callbacks = new ThePinCallbacks(getId());
            logger.info("storage.pinFile("+getFileId()+",...)");
            long desiredPinLifetime =
                ((BringOnlineRequest)getRequest()).getDesiredOnlineLifetimeInSeconds();
            if(desiredPinLifetime != -1) {
                //convert to millis
                desiredPinLifetime *= 1000;
            }

            getStorage().pinFile(getUser(),
                getFileId(),getRequest().getClient_host(),
                fileMetaData,
                desiredPinLifetime,
                getRequestId().longValue() ,
                callbacks);
        }
        catch(Exception e) {
            logger.error(e.toString());
            throw new NonFatalJobFailure(e.toString());
        }
    }

    protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
        State state = getState();
        logger.debug("State changed from "+oldState+" to "+getState());
        if(state == State.READY) {
            try {
                getRequest().resetRetryDeltaTime();
            }
            catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
        if(state == State.CANCELED || state == State.FAILED ) {
            if(getFileId() != null && getPinId() != null) {
                UnpinCallbacks callbacks = new TheUnpinCallbacks(this.getId());
                logger.info("state changed to final state, unpinning fileId= "+ getFileId()+" pinId = "+getPinId());
                try {
                    getStorage().unPinFile(getUser(),getFileId(),callbacks, getPinId());
                }
                catch (SRMInvalidRequestException ire) {
                    logger.error(ire.toString());
                    return;
                }
            }
        }
        super.stateChanged(oldState);
    }

    public TSURLReturnStatus releaseFile() throws SRMInvalidRequestException {
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        TReturnStatus returnStatus = new TReturnStatus();
        try {
            surlReturnStatus.setSurl(new org.apache.axis.types.URI(getSurlString()));
        } catch (Exception e) {
            logger.error(e.toString());
           returnStatus.setExplanation("wrong surl format");
           returnStatus.setStatusCode(TStatusCode.SRM_INVALID_REQUEST);
           surlReturnStatus.setStatus(returnStatus);
           return surlReturnStatus;
        }
        State state = getState();
        if(!State.isFinalState(state)) {
            logger.error("Canceled by the srmReleaseFile");
            try {
                this.setState(State.CANCELED, "Canceled by the srmReleaseFile");
            } catch (IllegalStateTransition ist) {
                logger.warn("Illegal State Transition : " +ist.getMessage());
            }
           returnStatus.setExplanation("srmBringOnline for this file has not completed yet,"+
                    " pending srmBringOnline canceled");
           returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
           surlReturnStatus.setStatus(returnStatus);
           return surlReturnStatus;

        }

        if(getFileId() != null && getPinId() != null) {
            TheUnpinCallbacks callbacks = new TheUnpinCallbacks(this.getId());
            logger.debug("srmReleaseFile, unpinning fileId= "+
                    getFileId()+" pinId = "+getPinId());
            getStorage().unPinFile(getUser(),getFileId(),callbacks, getPinId());
            try {
                callbacks.waitCompleteion(60000); //one minute
                if(callbacks.success) {
                    setPinId(null);
                    this.saveJob();
                    returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
                    surlReturnStatus.setStatus(returnStatus);
                    return surlReturnStatus;
                }
            } catch( InterruptedException ie) {
                logger.error(ie.toString());
            }


           returnStatus.setExplanation(" srmReleaseFile failed: "+callbacks.getError());
           returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
           surlReturnStatus.setStatus(returnStatus);
           return surlReturnStatus;
        } else {
           returnStatus.setExplanation(" srmReleaseFile failed: file is not pinned");
           returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
           surlReturnStatus.setStatus(returnStatus);
           return surlReturnStatus;

        }


    }

    public TReturnStatus getReturnStatus() {
        TReturnStatus returnStatus = new TReturnStatus();

        State state = getState();
 	returnStatus.setExplanation(state.toString());

        if(getStatusCode() != null) {
            returnStatus.setStatusCode(getStatusCode());
        } else if(state == State.DONE) {
            if(getPinId() != null) {
                returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
            }  else {
                returnStatus.setStatusCode(TStatusCode.SRM_RELEASED);
            }
        }
        else if(state == State.READY) {
            returnStatus.setStatusCode(TStatusCode.SRM_FILE_PINNED);
        }
        else if(state == State.TRANSFERRING) {
            returnStatus.setStatusCode(TStatusCode.SRM_FILE_PINNED);
        }
        else if(state == State.FAILED) {
	    returnStatus.setExplanation("FAILED: "+getErrorMessage());
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        else if(state == State.CANCELED ) {
            returnStatus.setStatusCode(TStatusCode.SRM_ABORTED);
        }
        else if(state == State.TQUEUED ) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
        }
        else if(state == State.RUNNING ||
                state == State.RQUEUED ||
                state == State.ASYNCWAIT ) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        }
        else {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
        }
        return returnStatus;
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
        long requestLifetime = getRequest().extendLifetimeMillis(newLifetime);
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
        SRMUser user =(SRMUser) getUser();
        getStorage().extendPinLifetime(user,getFileId(), getPinId(),newLifetime);
        return newLifetime;
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



    private  static class GetCallbacks implements GetFileInfoCallbacks

    {

        Long fileRequestJobId;

        public GetCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        private BringOnlineFileRequest getBringOnlineFileRequest()
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (BringOnlineFileRequest) job;
            }
            return null;
        }

        public void FileNotFound(String reason) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_INVALID_PATH);
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

        public void Error( String error) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                try {
                    fr.setState(State.FAILED,error);
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("GetCallbacks error: "+ error);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        public void Exception( Exception e) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                try {
                    fr.setState(State.FAILED,e.toString());
                }
                catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("GetCallbacks exception",e);
            }
            catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        public void GetStorageInfoFailed(String reason) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
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



        public void StorageInfoArrived(String fileId,FileMetaData fileMetaData) {
            try {
                if (fileMetaData.isDirectory) {
                    FileNotFound("Path is a directory");
                    return;
                }
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                logger.debug("StorageInfoArrived: FileId:"+fileId);
                State state ;
                synchronized(fr) {
                    state = fr.getState();
                }

                if(state == State.ASYNCWAIT || state == State.RUNNING) {
                    fr.setFileId(fileId);
                    fr.fileMetaData = fileMetaData;
                    if(state == State.ASYNCWAIT) {
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        }
                        catch(Exception ie) {
                            logger.error(ie.toString());
                        }
                    }
                }

            }
            catch(Exception e) {
                logger.error(e.toString());
            }

        }


        public void Timeout() {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                try {
                   fr.setState(State.FAILED,"GetCallbacks Timeout");
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

    }


    private  static class ThePinCallbacks implements PinCallbacks {

        Long fileRequestJobId;

        public ThePinCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public BringOnlineFileRequest getBringOnlineFileRequest()
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (BringOnlineFileRequest) job;
            }
            return null;
        }

        public void Error( String error) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
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

        public void Exception( Exception e) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
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




        public void Timeout() {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                logger.info("Pin request timed out");
                if (!fr.getState().isFinalState()) {
                    fr.pinFile();
                }
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        public void Pinned(String pinId) {
            try {
               BringOnlineFileRequest fr = getBringOnlineFileRequest();
               logger.debug("ThePinCallbacks: Pinned() pinId:"+pinId);
                synchronized(fr ) {
                    fr.setPinId(pinId);
                    fr.setState(State.DONE," file is pinned, pinId="+pinId);
                }
            }
            catch (SRMInvalidRequestException ire) {
                logger.error("BringOnlineFileRequest failed: " + ire.getMessage());
            }
            catch(SQLException e) {
                logger.error("BringOnlineFileRequest failed: " + e.getMessage());
            }
            catch(IllegalStateTransition ist) {
                logger.warn("Illegal State Transition : " +ist.getMessage());
            }
        }

        public void PinningFailed(String reason) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
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

    private static class TheUnpinCallbacks implements UnpinCallbacks {

        Long fileRequestJobId;

        public TheUnpinCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public BringOnlineFileRequest getBringOnlineFileRequest()
                throws java.sql.SQLException, SRMInvalidRequestException {
            if(fileRequestJobId != null) {
                Job job = Job.getJob(fileRequestJobId);
                if(job != null) {
                    return (BringOnlineFileRequest) job;
                }
            }
            return null;
        }

        public void Error( String error) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                /*
                 * unpin is called when the file request  is already
                 * in a final state
                 */
                /*
                 try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //logger.error("can not fail state:"+ist);
                }
                 */
                this.error = "TheUnpinCallbacks error: "+ error;
                if(fr != null) {
                    logger.error(this.error);
                }
                success = false;
                done();
            }
            catch(Exception e) {
                logger.error(e.toString());
                done();
            }
        }

        public void Exception( Exception e) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                /*
                 * unpin is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //logger.error("can not fail state:"+ist);
                }
                 */
                if(fr != null) {
                    logger.error("TheUnpinCallbacks exception",e);
                }
                this.error = "TheUninCallbacks exception: "+e.toString();
                success = false;
                done();
            }
            catch(Exception e1) {
                logger.error(e1.toString());
                done();
            }
        }




        public void Timeout() {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                /*
                 * unpin is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //logger.error("can not fail state:"+ist);
                }
                 */

                this.error = "TheUninCallbacks Timeout";
                if(fr  != null) {
                    logger.error(this.error);
               }
                success = false;
                done();

            }
            catch(Exception e) {
                logger.error(e.toString());
                done();
            }
        }

        public void Unpinned(String pinId) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                if(fr != null) {
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
                success = true;
                done();
            }
            catch(Exception e) {
                logger.error(e.toString());
                done();
            }
        }

        public void UnpinningFailed(String reason) {
            try {
                BringOnlineFileRequest fr = getBringOnlineFileRequest();
                /*
                 * unpin is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //logger.error("can not fail state:"+ist);
                }
                 */

                this.error = "TheUnpinCallbacks error: "+ reason;
                if(fr  != null) {
                    logger.error(this.error);
                }
                success = false;
                done();

            }
            catch(Exception e) {
                logger.error(e.toString());
                done();
            }
        }

        private boolean done = false;
        private boolean success  = true;
        private String error;

        public synchronized boolean  isSuccess() {
            return done && success;
        }
        public  boolean waitCompleteion(long timeout) throws InterruptedException {
           long starttime = System.currentTimeMillis();
            while(true) {
                synchronized(this) {
                    wait(1000);
                    if(done) {
                        return success;
                    }
                    else
                    {
                        if((System.currentTimeMillis() - starttime)>timeout) {
                            error = " TheUnpinCallbacks Timeout";
                            return false;
                        }
                    }
                }
            }
        }

        public  synchronized void done() {
            done = true;
            notifyAll();
        }

        public java.lang.String getError() {
            return error;
        }

        public synchronized  boolean  isDone() {
            return done;
        }


    }
    public static void unpinBySURLandRequestId(
        AbstractStorageElement storage,
        final SRMUser user,
        final long id,
        final URI surl) throws SRMException {

        FileMetaData fmd =
            storage.getFileMetaData(user,surl,true);
        String fileId = fmd.fileId;
        if(fileId != null) {
            BringOnlineFileRequest.TheUnpinCallbacks unpinCallbacks =
                new BringOnlineFileRequest.TheUnpinCallbacks(null);
            storage.unPinFileBySrmRequestId(user,
                fileId,unpinCallbacks,id);
          try {
                unpinCallbacks.waitCompleteion(60000); //one minute
                if(unpinCallbacks.isDone()) {

                    if(unpinCallbacks.isSuccess()) {
                        return;
                    } else
                    throw new SRMException("unpinning of "+surl+" by SrmRequestId "+id+
                        " failed :"+unpinCallbacks.getError());
                } else {
                    throw new SRMException("unpinning of "+surl+" by SrmRequestId "+id+
                        " took too long");

                }
            } catch( InterruptedException ie) {
                logger.error(ie.toString());
                throw new SRMException("unpinning of "+surl+" by SrmRequestId "+id+
                        " got interrupted");
            }
         }
    }

    public static void unpinBySURL(
        AbstractStorageElement storage,
        final SRMUser user,
        final URI surl)
        throws SRMException {
        FileMetaData fmd =
            storage.getFileMetaData(user,surl,true);
        String fileId = fmd.fileId;
        if(fileId != null) {
            BringOnlineFileRequest.TheUnpinCallbacks unpinCallbacks =
                new BringOnlineFileRequest.TheUnpinCallbacks(null);
            storage.unPinFile(user,
                fileId,unpinCallbacks);
          try {
                unpinCallbacks.waitCompleteion(60000); //one minute
                if(unpinCallbacks.isDone()) {
                    if(unpinCallbacks.isSuccess()) {
                        return;
                    } else {
                        throw new SRMException("unpinning of "+surl+
                            " failed :"+unpinCallbacks.getError());
                    }
                } else {
                        throw new SRMException("unpinning of "+surl+
                            " took too long");
                }
            } catch( InterruptedException ie) {
                logger.error(ie.toString());
                throw new SRMException("unpinning of "+surl+
                        " got interrupted");
            }
         }
    }
}
