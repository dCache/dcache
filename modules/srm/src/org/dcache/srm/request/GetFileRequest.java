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

import java.net.MalformedURLException;

import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.FileMetaData;
import org.globus.util.GlobusURL;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMAuthorizationException;
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
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.apache.axis.types.URI;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 *
 * @author  timur
 * @version
 */
public final class GetFileRequest extends FileRequest {
    private final static Logger logger = LoggerFactory.getLogger(GetFileRequest.class);

    // the globus url class created from surl_string
    private GlobusURL surl;
    private GlobusURL turl;
    private String pinId;
    private String fileId;

    /**
     * Transient, since we do not need to replicate the field,
     * the field is needed only in jvm where the request is originally
     * scheduled
     */
    private transient FileMetaData fileMetaData;

    private static final long serialVersionUID = -9155373723705753177L;

    /** Creates new FileRequest */
    public GetFileRequest(Long requestId,
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
        try {
            surl = new GlobusURL(url);
            logger.debug("    surl = "+surl.getURL());
        }
        catch(MalformedURLException murle) {
            throw new IllegalArgumentException(murle.toString());
        }

    }
    /**
     * restore constructore, used for restoring the existing
     * file request from the database
     */

    public GetFileRequest(
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
    String TURL,
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

        try {
            this.surl = new GlobusURL(SURL);
            if(TURL != null && (!TURL.equalsIgnoreCase("null"))) {
                this.turl = new GlobusURL(TURL);
            }
        }
        catch(MalformedURLException murle) {
            throw new IllegalArgumentException(murle.toString());
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
        rlock();
        try {
            return getPinId() != null;
        } finally {
            runlock();
        }
    }



    private final String getPath() {
        String path;

        rlock();
        try {
            path = getSurl().getPath();
        } finally {
            runlock();
        }

        int indx=path.indexOf(SFN_STRING);
        if( indx != -1) {

            path=path.substring(indx+SFN_STRING.length());
        }

        if(!path.startsWith("/")) {
            path = "/"+path;
        }
        return path;
    }


    private final GlobusURL getSurl() {
        rlock();
        try {
            return surl;
        } finally {
            runlock();
        }
    }

    private final GlobusURL getTurl() {
        rlock();
        try {
            return turl;
        } finally {
            runlock();
        }
    }

    public final String getSurlString() {
        rlock();
        try {
            return getSurl().getURL();
        } finally {
            runlock();
        }
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
                    logger.error(error.toString());
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
                    logger.error(error.toString());
                    try {
                        setState(State.FAILED,error);
                    }
                    catch(IllegalStateTransition ist) {
                        logger.warn("Illegal State Transition : " +ist.getMessage());
                    }
                }
            }

            if(getTurl()!= null) {
                return getTurl().getURL();
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


    public RequestFileStatus getRequestFileStatus(){
        RequestFileStatus rfs;
        if(getFileMetaData() != null) {
            rfs = new RequestFileStatus(getFileMetaData());
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
            throws java.sql.SQLException, SRMInvalidRequestException{
        TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
        if(getFileMetaData() != null) {
            fileStatus.setFileSize(new org.apache.axis.types.UnsignedLong(getFileMetaData().size));
        }

        try {
             fileStatus.setSourceSURL(new URI(getSurlString()));
        } catch (Exception e) {
            logger.error(e.toString());
            throw new java.sql.SQLException("wrong surl format");
        }

        String turlstring = getTurlString();
        if(turlstring != null) {
            try {
            fileStatus.setTransferURL(new URI(turlstring));
            } catch (Exception e) {
                logger.error(e.toString());
                throw new java.sql.SQLException("wrong turl format");
            }

        }
        if(this.isPinned()) {

            fileStatus.setRemainingPinTime(new Integer((int)(getRemainingLifetime()/1000)));
        }
        fileStatus.setEstimatedWaitTime(new Integer((int)(getRequest().getRetryDeltaTime())));
        TReturnStatus returnStatus = getReturnStatus();
        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    public TSURLReturnStatus  getTSURLReturnStatus() throws java.sql.SQLException{
        TReturnStatus returnStatus = getReturnStatus();
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        try {
            surlReturnStatus.setSurl(new URI(getSurlString()));
        } catch (Exception e) {
            logger.error(e.toString());
            throw new java.sql.SQLException("wrong surl format");
        }
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }

    private GlobusURL getTURL() throws SRMException, java.sql.SQLException{
        String firstDcapTurl = null;
        GetRequest request = (GetRequest) Job.getJob(requestId);
        if(request != null) {
            firstDcapTurl = request.getFirstDcapTurl();
            if(firstDcapTurl == null) {
                try {
                    String theTurl = getStorage().getGetTurl(getUser(),
                        getPath(),
                        request.protocols);
                    if(theTurl == null) {
                        throw new SRMException("turl is null");
                    }
                    GlobusURL g_turl = new GlobusURL(theTurl);
                    if(g_turl.getProtocol().equals("dcap")) {

                        request.setFirstDcapTurl(theTurl);
                    }
                    return g_turl;
                }
                catch(MalformedURLException murle) {
                    logger.error(murle.toString());
                    throw new SRMException(murle.toString());
                }
            }
        }

        try {

            String theTurl =getStorage().getGetTurl(getUser(),getPath(), firstDcapTurl);
            if(theTurl == null) {
                return null;
            }
            return new GlobusURL(theTurl);
        }
        catch(MalformedURLException murle) {
            return null;
        }
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
            sb.append('\n').append("   SURL: ").append(getSurl().getURL());
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

    public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
        logger.debug("run()");
        try {
            if(getFileId() == null) {
                try {
                    if(!Tools.sameHost(getConfiguration().getSrmHosts(),
                    getSurl().getHost())) {
                        String error ="surl is not local : "+getSurl().getURL();
                        logger.error(error.toString());
                        throw new FatalJobFailure(error);
                    }
                }
                catch(java.net.UnknownHostException uhe) {
                    logger.error(uhe.toString());
                    throw new FatalJobFailure(uhe.toString());
                }

                logger.debug("fileId is null, asking to get a fileId");
                askFileId();
                if(getFileId() == null) {
                    setState(State.ASYNCWAIT, "getting file Id");
                    logger.debug("GetFileRequest: waiting async notification about fileId...");
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
                    logger.debug("GetFileRequest: waiting async notification about pinId...");
                    return;
                }
            }
        } catch(IllegalStateTransition ist) {
            throw new NonFatalJobFailure("Illegal State Transition : " +ist.getMessage());
        }
        logger.info("PinId is "+getPinId()+" returning, scheduler should change state to \"Ready\"");

    }

    public void askFileId() throws NonFatalJobFailure, FatalJobFailure {
        try {

            logger.debug(" proccessing the file request id "+getId());
            String  path =   getPath();
            logger.debug(" path is "+path);
            // if we can not read this path for some reason
            //(not in ftp root for example) this will throw exception
            // we do not care about the return value yet
            logger.debug("calling Job.getJob("+requestId+")");
            GetRequest request = (GetRequest) Job.getJob(requestId);
            logger.debug("this file request's request is  "+request);
            //this will fail if the protocols are not supported
            String[] supported_prots = getStorage().supportedGetProtocols();
            boolean found_supp_prot=false;
            mark1:
            for(int i=0; i< supported_prots.length;++i) {
                for(int j=0; j<request.protocols.length; ++j) {
                    if(supported_prots[i].equals(request.protocols[j])) {
                        found_supp_prot = true;
                        break mark1;
                    }
                }
            }
            if(!found_supp_prot) {
                throw new FatalJobFailure("transfer protocols not supported");
            }
            //storage.getGetTurl(getUser(),path,request.protocols);
            logger.debug("storage.prepareToGet("+path+",...)");
            GetFileInfoCallbacks callbacks = new GetCallbacks(getId());
            getStorage().getFileInfo(getUser(),path,true,callbacks);
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
            getStorage().pinFile(getUser(),
                getFileId(),getRequest().getClient_host(), getFileMetaData(),
                lifetime,
                    getRequestId().longValue() ,callbacks);
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
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire.toString());
            }
        }
        if(State.isFinalState(state)) {
            if(getFileId() != null && getPinId() != null) {
                UnpinCallbacks callbacks = new TheUninCallbacks(this.getId());
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
    }

    public TReturnStatus getReturnStatus() {
        TReturnStatus returnStatus = new TReturnStatus();

        State state = getState();

 	returnStatus.setExplanation(state.toString());

        if(getStatusCode() != null) {
            returnStatus.setStatusCode(getStatusCode());
        } else if(state == State.DONE) {
            returnStatus.setStatusCode(TStatusCode.SRM_RELEASED);
        }
        else if(state == State.READY) {
            returnStatus.setStatusCode(TStatusCode.SRM_FILE_PINNED);
        }
        else if(state == State.TRANSFERRING) {
            returnStatus.setStatusCode(TStatusCode.SRM_FILE_PINNED);
        }
        else if(state == State.FAILED) {
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
	    returnStatus.setExplanation("FAILED: "+getErrorMessage());
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
        return getStorage().extendPinLifetime(user,getFileId(), getPinId(),newLifetime);
    }

    /**
     * @param turl the turl to set
     */
    private void setTurl(GlobusURL turl) {
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
    private final void setFileId(String fileId) {
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
    private final FileMetaData getFileMetaData() {
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



    private  static class GetCallbacks implements GetFileInfoCallbacks

    {

        Long fileRequestJobId;

        public GetCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        private GetFileRequest getGetFileRequest()
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (GetFileRequest) job;
            }
            return null;
        }

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
                logger.error("GetCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        public void Error( String error) {
            try {
                GetFileRequest fr = getGetFileRequest();
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
                GetFileRequest fr = getGetFileRequest();
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
                GetFileRequest fr = getGetFileRequest();
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

                GetFileRequest fr = getGetFileRequest();
                logger.debug("StorageInfoArrived: FileId:"+fileId);
                State state ;
                synchronized(fr) {
                    state = fr.getState();
                }

                if(state == State.ASYNCWAIT || state == State.RUNNING) {
                    fr.setFileId(fileId);
                    fr.setFileMetaData(fileMetaData);

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
                GetFileRequest fr = getGetFileRequest();
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

        public GetFileRequest getGetFileRequest()
                throws java.sql.SQLException,SRMInvalidRequestException  {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (GetFileRequest) job;
            }
            return null;
        }

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

        public void Pinned(String pinId) {
            try {
                GetFileRequest fr = getGetFileRequest();
                State state;
                synchronized(fr ) {
                    state = fr.getState();
                }
                logger.debug("ThePinCallbacks: Pinned() pinId:"+pinId);
                if(state == State.ASYNCWAIT || state == State.RUNNING) {
                    fr.setPinId(pinId);
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

    private  static class TheUninCallbacks implements UnpinCallbacks {

        Long fileRequestJobId;

        public TheUninCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public GetFileRequest getGetFileRequest()
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (GetFileRequest) job;
            }
            return null;
        }

        public void Error( String error) {
            try {
                GetFileRequest fr = getGetFileRequest();
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
                logger.error("TheUninCallbacks error: "+ error);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        public void Exception( Exception e) {
            try {
                GetFileRequest fr = getGetFileRequest();
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
                logger.error("TheUninCallbacks exception",e);
            }
            catch(Exception e1) {
                logger.error(e1.toString());
            }
        }




        public void Timeout() {
            try {
                GetFileRequest fr = getGetFileRequest();
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

                logger.error("TheUninCallbacks Timeout");
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

        public void Unpinned(String pinId) {
            try {
                GetFileRequest fr = getGetFileRequest();
                logger.debug("TheUninCallbacks: Unpinned() pinId:"+pinId);
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

        public void UnpinningFailed(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
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

                logger.error("TheUninCallbacks error: "+ reason);
            }
            catch(Exception e) {
                logger.error(e.toString());
            }
        }

    }
}
