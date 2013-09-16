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

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;

import diskCacheV111.srm.RequestFileStatus;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.ReleaseSpaceCallbacks;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 * @version
 */
public final class PutFileRequest extends FileRequest<PutRequest> {
    private final static Logger logger = LoggerFactory.getLogger(PutFileRequest.class);
    // this is anSurl path
    private URI surl;
    private URI turl;
    private long size;
    // parent directory info
    private String fileId;
    private String parentFileId;
    private transient FileMetaData fmd;
    private transient FileMetaData parentFmd;
    private String spaceReservationId;
    private boolean weReservedSpace;
    private TAccessLatency accessLatency ;//null by default
    private TRetentionPolicy retentionPolicy;//null default value
    private boolean spaceMarkedAsBeingUsed;

    /** Creates new FileRequest */
    public PutFileRequest(long requestId,
            Long requestCredentalId,
            String url,
            long size,
            long lifetime,
            int maxNumberOfRetires,
            String spaceReservationId,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency
            ) throws Exception {
        super(requestId,
                requestCredentalId,
                lifetime,
                maxNumberOfRetires);
        this.surl = URI.create(url);
        this.size = size;
        this.spaceReservationId = spaceReservationId;
        if(accessLatency != null) {
            this.accessLatency = accessLatency;
        }
        if(retentionPolicy != null ) {
            this.retentionPolicy = retentionPolicy;
        }
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
            String TURL,
            String fileId,
            String parentFileId,
            String spaceReservationId,
            long size,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency
            ) throws SQLException {
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

        if(parentFileId != null && (!parentFileId.equalsIgnoreCase("null"))) {
            this.parentFileId = parentFileId;
        }
        this.spaceReservationId = spaceReservationId;
        this.size = size;
        this.accessLatency = accessLatency;
        this.retentionPolicy = retentionPolicy;
    }

    public final String getFileId() {
        rlock();
        try {
            return fileId;
        } finally {
            runlock();
        }
    }

    public final void setSize(long size) {
        wlock();
        try {
            this.size = size;
        } finally {
            wunlock();
        }
    }

    public final long getSize() {
        rlock();
        try {
            return size;
        } finally {
            runlock();
        }
    }

    private void setTurl(String turl_string) throws URISyntaxException {
        setTurl(new URI(turl_string));
    }

    public final URI getSurl() {
        rlock();
        try {
            return surl;
       } finally {
           runlock();
       }
    }

    public final String getSurlString() {
        return getSurl().toASCIIString();
    }

    public String getTurlString() {
        wlock();
        try {
            State state = getState();
            if(getTurl() == null && (state == State.READY ||
                    state == State.TRANSFERRING)) {
                try {
                    setTurl(getTURL());

                }
            catch(SRMAuthorizationException srme) {
                String error =srme.getMessage();
                logger.error(error);
                try {
                    setStateAndStatusCode(State.FAILED,
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
                    catch(IllegalStateTransition ist)
                    {
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

    @Override
    public RequestFileStatus getRequestFileStatus() {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = (int) getId();

        rfs.SURL = getSurlString();
        rfs.size = getSize();
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
            throws SQLException, SRMInvalidRequestException{
        TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
        fileStatus.setFileSize(new UnsignedLong(getSize()));


        org.apache.axis.types.URI anSurl;
        try {
            anSurl= new org.apache.axis.types.URI(getSurlString());
        } catch (Exception e) {
            logger.error(e.toString());
            throw new SQLException("wrong surl format");
        }
        fileStatus.setSURL(anSurl);
        //fileStatus.set

        String turlstring = getTurlString();
        if(turlstring != null) {
            org.apache.axis.types.URI transferURL;
            try {
                transferURL = new org.apache.axis.types.URI(turlstring);
            } catch (Exception e) {
                logger.error(e.toString());
                throw new SQLException("wrong turl format");
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
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
        }

        returnStatus.setExplanation(getErrorMessage());

        fileStatus.setStatus(returnStatus);

        return fileStatus;
    }

    public TSURLReturnStatus  getTSURLReturnStatus() throws SQLException{
        org.apache.axis.types.URI anSurl;
        try {
            anSurl = new org.apache.axis.types.URI(getSurlString());
        } catch (Exception e) {
            logger.error(e.toString());
            throw new SQLException("wrong surl format");
        }
        TReturnStatus returnStatus = getReturnStatus();
        if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            //SRM_SPACE_LIFETIME_EXPIRED is illeal on the file level,
            // but we use it to correctly calculate the request level status
            // so we do the translation here
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        surlReturnStatus.setSurl(anSurl);
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }

    private URI getTURL() throws SRMException, SQLException {
        PutRequest request = getContainerRequest();
        // do not synchronize on request, since it might cause deadlock
        String firstDcapTurl = request.getFirstDcapTurl();
        if(firstDcapTurl == null) {
            URI turl =
                getStorage().getPutTurl(getUser(), getSurl(),
                                        request.getProtocols());
            if(turl.getScheme().equals("dcap")) {
                request.setFirstDcapTurl(turl.toString());
            }
            return turl;
        }

        return getStorage().getPutTurl(request.getUser(),
                                       getSurl(),
                                       URI.create(firstDcapTurl));
    }

    @Override
    public void toString(StringBuilder sb, boolean longformat) {
        sb.append(" PutFileRequest ");
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
            sb.append('\n').append("   TURL: ").append(getTurlString());
            sb.append('\n').append("   size: ").append(getSize());
            sb.append('\n').append("   AccessLatency: ").append(getAccessLatency());
            sb.append('\n').append("   RetentionPolicy: ").append(getRetentionPolicy());
            sb.append('\n').append("   spaceReservation: ").append(getSpaceReservationId());
            sb.append('\n').append("   isReservedByUs: ").append(isWeReservedSpace());
            sb.append('\n').append("   isSpaceMarkedAsBeingUsed: ").
                    append(isSpaceMarkedAsBeingUsed());
            sb.append('\n').append("   status code:").append(getStatusCode());
            sb.append('\n').append("   error message:").append(getErrorMessage());
            sb.append('\n').append("   History of State Transitions: \n");
            sb.append(getHistory());
        }
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure
    {
        addDebugHistoryEvent("run method is executed");
        try {
            if (getFileId() == null && getParentFileId() == null) {
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
                                    "SURL exists already",
                                    TStatusCode.SRM_DUPLICATION_ERROR);
                        } else {
                            setStateAndStatusCode(
                                    State.FAILED,
                                    "The requested file is being used by another client.",
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

                //storage.getPutTurl(getUser(),path,request.protocols);
                PutCallbacks callbacks = new PutCallbacks(this.getId());
                setState(State.ASYNCWAIT, "calling Storage.prepareToPut()");
                getStorage().prepareToPut(getUser(),getSurl(),callbacks,
                        getContainerRequest().isOverwrite());
                return;
            }
            long defaultSpaceReservationId=0;
            if (getParentFmd().spaceTokens!=null) {
                if (getParentFmd().spaceTokens.length>0) {
                    defaultSpaceReservationId=getParentFmd().spaceTokens[0];
                }
            }
            if (getSpaceReservationId()==null) {
                if (defaultSpaceReservationId!=0) {
                    if( getRetentionPolicy()==null&&getAccessLatency()==null) {
                        StringBuilder sb = new StringBuilder();
                        sb.append(defaultSpaceReservationId);
                        spaceReservationId=sb.toString();
                    }
                }
            }

            if (getConfiguration().isReserve_space_implicitely()&&getSpaceReservationId() == null) {
                long remaining_lifetime;
                setState(State.ASYNCWAIT,"reserving space");
                remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
                SrmReserveSpaceCallbacks callbacks = new PutReserveSpaceCallbacks(getId());
                //
                //the following code allows the inheritance of the
                // retention policy from the directory metatada
                //
                if( getRetentionPolicy() == null && getParentFmd() != null && getParentFmd().retentionPolicyInfo != null ) {
                    setRetentionPolicy(getParentFmd().retentionPolicyInfo.getRetentionPolicy());
                }
                //
                //the following code allows the inheritance of the
                // access latency from the directory metatada
                //
                if( getAccessLatency() == null && getParentFmd() != null && getParentFmd().retentionPolicyInfo != null ) {
                    setAccessLatency(getParentFmd().retentionPolicyInfo.getAccessLatency());
                }
                logger.debug("reserving space, size="+(getSize()==0?1L:getSize()));
                    getStorage().srmReserveSpace(
                    getUser(),
                    getSize()==0?1L:getSize(),
                    remaining_lifetime,
                    getRetentionPolicy() ==null ? null: getRetentionPolicy().getValue(),
                    getAccessLatency() == null? null:getAccessLatency().getValue(),
                        null,
                    callbacks);
                return;
            }
            if( getSpaceReservationId() != null &&
            !   isSpaceMarkedAsBeingUsed()) {
                setState(State.ASYNCWAIT,"marking space as being used");
                long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
                SrmUseSpaceCallbacks  callbacks = new PutUseSpaceCallbacks(getId());
                    getStorage().srmMarkSpaceAsBeingUsed(getUser(),
                                getSpaceReservationId(),getSurl(),
                                getSize()==0?1:getSize(),
                                remaining_lifetime,
                                getContainerRequest().isOverwrite(),
                                    callbacks );
                return;
            }
            logger.debug("run() returns, scheduler should bring file request into the ready state eventually");
        }
        catch(SRMException | SQLException | IllegalStateTransition e) {
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
        SRMUser user;
         try {
             user = getUser();
         }catch(SRMInvalidRequestException ire) {
             logger.error(ire.toString());
             return;
         }
        if(State.isFinalState(state)) {
            logger.debug("space reservation is "+getSpaceReservationId());

            if ( getSpaceReservationId() != null &&
                 isSpaceMarkedAsBeingUsed() ) {
                SrmCancelUseOfSpaceCallbacks callbacks =
                        new PutCancelUseOfSpaceCallbacks(getId());
                getStorage().srmUnmarkSpaceAsBeingUsed(user,getSpaceReservationId(),getSurl(),
                        callbacks);

            }
            if(getSpaceReservationId() != null && isWeReservedSpace()) {
                logger.debug("storage.releaseSpace("+getSpaceReservationId()+"\"");
                SrmReleaseSpaceCallbacks callbacks =
                        new PutReleaseSpaceCallbacks(this.getId());
                getStorage().srmReleaseSpace(  user,getSpaceReservationId(),
                        null, //release all of space we reserved
                        callbacks);

            }
        }

        super.stateChanged(oldState);
    }

    @Override
    public boolean isTouchingSurl(URI surl)
    {
        return surl.equals(getSurl());
    }

    /**
     * Getter for property parentFileId.
     * @return Value of property parentFileId.
     */
    public final String getParentFileId() {
        rlock();
        try {
            return parentFileId;
        } finally {
            runlock();
        }
    }

    /**
     * Getter for property spaceReservationId.
     * @return Value of property spaceReservationId.
     */
    public final String getSpaceReservationId() {
        rlock();
        try {
            return spaceReservationId;
        } finally {
            runlock();
        }
    }

    /**
     * Setter for property spaceReservationId.
     * @param spaceReservationId New value of property spaceReservationId.
     */
    public final void setSpaceReservationId(String spaceReservationId) {
        wlock();
        try {
            this.spaceReservationId = spaceReservationId;
        } finally {
            wunlock();
        }
    }

    @Override
    public TReturnStatus getReturnStatus() {
        TReturnStatus returnStatus = new TReturnStatus();

        State state = getState();

     	returnStatus.setExplanation(state.toString());

        if(getStatusCode() != null) {
            returnStatus.setStatusCode(getStatusCode());
        } else if(state == State.DONE) {
            returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        } else if(state == State.READY) {
            returnStatus.setStatusCode(TStatusCode.SRM_SPACE_AVAILABLE);
        } else if(state == State.TRANSFERRING) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        } else if(state == State.FAILED) {
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation("FAILED: "+getErrorMessage());
        } else if(state == State.CANCELED ) {
            returnStatus.setStatusCode(TStatusCode.SRM_ABORTED);
        } else if(state == State.TQUEUED ) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
        } else if(state == State.RUNNING ||
                state == State.RQUEUED ||
                state == State.ASYNCWAIT ) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        } else {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
        }
        return returnStatus;
    }

    /**
     * @param fileId the fileId to set
     */
    public final void setFileId(String fileId) {
        wlock();
        try {
            this.fileId = fileId;
        } finally {
            wunlock();
        }
    }

    /**
     * @param parentFileId the parentFileId to set
     */
    public final void setParentFileId(String parentFileId) {
        wlock();
        try {
            this.parentFileId = parentFileId;
        } finally {
            wunlock();
        }
    }

    /**
     * @param anSurl the anSurl to set
     */
    public final void setSurl(URI surl) {
        wlock();
        try {
            this.surl = surl;
        } finally {
            wunlock();
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
     * @param aTurl the aTurl to set
     */
    public final void setTurl(URI turl) {
        wlock();
        try {
            this.turl = turl;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the fmd
     */
    private FileMetaData getFmd() {
        rlock();
        try {
            return fmd;
        } finally {
            runlock();
        }
    }

    /**
     * @param fmd the fmd to set
     */
    private void setFmd(FileMetaData fmd) {
        wlock();
        try {
            this.fmd = fmd;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the parentFmd
     */
    private FileMetaData getParentFmd() {
        rlock();
        try {
            return parentFmd;
        } finally {
            runlock();
        }
    }

    /**
     * @param parentFmd the parentFmd to set
     */
    private void setParentFmd(FileMetaData parentFmd) {
        wlock();
        try {
            this.parentFmd = parentFmd;
        } finally {
            wunlock();
        }
    }

    private static class PutCallbacks implements PrepareToPutCallbacks {
        private final long fileRequestJobId;

        public PutCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public PutFileRequest getPutFileRequest() throws SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }

        @Override
        public void DuplicationError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_DUPLICATION_ERROR);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Error( String error) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setState(State.FAILED,error);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks error: "+ error);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Exception( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                    fr.setState(State.FAILED,error);
                } catch(IllegalStateTransition ist) {
                    logger.error("can not fail state:"+ist);
                }
                logger.error("PutCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        @Override
        public void GetStorageInfoFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }


        @Override
        public void StorageInfoArrived(String fileId,FileMetaData fmd,String parentFileId, FileMetaData parentFmd) {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("StorageInfoArrived: FileId:"+fileId);
                State state = fr.getState();
                if(state == State.ASYNCWAIT) {
                    logger.debug("PutCallbacks StorageInfoArrived for file "+fr.getSurlString());
                    fr.setFileId(fileId);
                    fr.setFmd(fmd);
                    fr.setParentFileId(parentFileId);
                    fr.setParentFmd(parentFmd);

                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        logger.error(ie.toString());
                    }
                }
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Timeout() {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setState(State.FAILED,"PutCallbacks Timeout");
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks Timeout");
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void InvalidPathError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_INVALID_PATH);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void AuthorizationError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_AUTHORIZATION_FAILURE);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public static class PutReserveSpaceCallbacks implements SrmReserveSpaceCallbacks {
        private final long fileRequestJobId;

        public PutFileRequest getPutFileRequest()
                throws SQLException,
                SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }


        public PutReserveSpaceCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        @Override
        public void ReserveSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                    fr.setState(State.FAILED,error);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("PutReserveSpaceCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        @Override
        public void ReserveSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutReserveSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void NoFreeSpace(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutReserveSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("Space Reserved: spaceReservationToken:"+spaceReservationToken);
                State state;
                synchronized(fr) {
                    state = fr.getState();
                }
                if(state == State.ASYNCWAIT) {
                    logger.debug("PutReserveSpaceCallbacks Space Reserved for file "+fr.getSurlString());
                    fr.setWeReservedSpace(true);
                    fr.setSpaceReservationId(spaceReservationToken);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        logger.error(ie.toString());
                    }
                }
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public static class PutReleaseSpaceCallbacks implements SrmReleaseSpaceCallbacks {
        private final long fileRequestJobId;

        public PutFileRequest getPutFileRequest()
                throws SQLException,
                  SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }


        public PutReleaseSpaceCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        @Override
        public void ReleaseSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                logger.error("PutReleaseSpaceCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        @Override
        public void ReleaseSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();

                logger.error("PutReleaseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void SpaceReleased(String spaceReservationToken, long reservedSpaceSize) {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("Space Released: spaceReservationToken:"+spaceReservationToken+" remaining space="+reservedSpaceSize);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public static class PutUseSpaceCallbacks implements SrmUseSpaceCallbacks {
        private final long fileRequestJobId;

        public PutFileRequest getPutFileRequest()
                throws SQLException,
                SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }


        public PutUseSpaceCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        @Override
        public void SrmUseSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                            fr.setState(State.FAILED,error);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }
                logger.error("PutUseSpaceCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        @Override
        public void SrmUseSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
        /**
         * call this if space reservation exists, but has no free space
         */
        @Override
        public void SrmNoFreeSpace(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }

        }

        /**
         * call this if space reservation exists, but has been released
         */
        @Override
        public void SrmReleased(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_NO_FREE_SPACE);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }

        }

        /**
         * call this if space reservation exists, but has been released
         */
        @Override
        public void SrmExpired(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_SPACE_LIFETIME_EXPIRED);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }

        }

        /**
         * call this if space reservation exists, but not authorized
         */
        @Override
        public void SrmNotAuthorized(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    fr.setStateAndStatusCode(
                            State.FAILED,
                            reason,
                            TStatusCode.SRM_AUTHORIZATION_FAILURE);
                } catch(IllegalStateTransition ist) {
                    logger.warn("Illegal State Transition : " +ist.getMessage());
                }

                logger.error("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }

        }

        @Override
        public void SpaceUsed() {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("Space Marked as Being Used");
                State state = fr.getState();
                if(state == State.ASYNCWAIT) {
                    logger.debug("PutUseSpaceCallbacks Space Marked as Being Used for file "+fr.getSurlString());
                    fr.setSpaceMarkedAsBeingUsed(true);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        logger.error(ie.toString());
                    }
                }
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }

    public static class PutCancelUseOfSpaceCallbacks implements SrmCancelUseOfSpaceCallbacks {
        private final long fileRequestJobId;

        public PutFileRequest getPutFileRequest()
                throws SQLException, SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }


        public PutCancelUseOfSpaceCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        @Override
        public void CancelUseOfSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                logger.error("PutCancelUseOfSpaceCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }

        @Override
        public void CancelUseOfSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();

                logger.error("PutCancelUseOfSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void UseOfSpaceSpaceCanceled() {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("Umarked Space as Being Used");
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }
    }

    private  static class TheReleaseSpaceCallbacks implements ReleaseSpaceCallbacks {

        private final long fileRequestJobId;

        public TheReleaseSpaceCallbacks(long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public PutFileRequest getPutFileRequest()
                throws SQLException, SRMInvalidRequestException {
            return Job.getJob(fileRequestJobId, PutFileRequest.class);
        }

        @Override
        public void Error( String error) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                logger.error("TheReleaseSpaceCallbacks error: "+ error);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void Exception( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                logger.error("TheReleaseSpaceCallbacks exception",e);
            } catch(Exception e1) {
                logger.error(e1.toString());
            }
        }




        @Override
        public void Timeout() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                logger.error("TheReleaseSpaceCallbacks Timeout");
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void SpaceReleased() {
            try {
                PutFileRequest fr = getPutFileRequest();
                logger.debug("TheReleaseSpaceCallbacks: SpaceReleased");
                fr.setSpaceReservationId(null);
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

        @Override
        public void ReleaseSpaceFailed(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                logger.error("TheReleaseSpaceCallbacks error: "+ reason+" ignoring, could have been all used up");
            } catch(Exception e) {
                logger.error(e.toString());
            }
        }

    }

    public final boolean isWeReservedSpace() {
        rlock();
        try {
            return weReservedSpace;
        } finally {
            runlock();
        }
    }

    public final void setWeReservedSpace(boolean weReservedSpace) {
        wlock();
        try {
            this.weReservedSpace = weReservedSpace;
        } finally {
            wunlock();
        }
    }

    public final boolean isSpaceMarkedAsBeingUsed() {
        rlock();
        try {
            return spaceMarkedAsBeingUsed;
        } finally {
            runlock();
        }
    }

    public final void setSpaceMarkedAsBeingUsed(boolean spaceMarkedAsBeingUsed) {
        wlock();
        try {
            this.spaceMarkedAsBeingUsed = spaceMarkedAsBeingUsed;
        } finally {
            wunlock();
        }
    }

    public final TAccessLatency getAccessLatency() {
        rlock();
        try {
            return accessLatency;
        } finally {
            runlock();
        }
    }

    public final void setAccessLatency(TAccessLatency accessLatency) {
        wlock();
        try {
            this.accessLatency = accessLatency;
        } finally {
            wunlock();
        }
    }

    public final TRetentionPolicy getRetentionPolicy() {
        rlock();
        try {
            return retentionPolicy;
        } finally {
            runlock();
        }
    }

    public final void setRetentionPolicy(TRetentionPolicy retentionPolicy) {
        wlock();
        try {
            this.retentionPolicy = retentionPolicy;
        } finally {
            wunlock();
        }
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
        String spaceToken =getSpaceReservationId();

        if(!getConfiguration().isReserve_space_implicitely() ||
           spaceToken == null ||
           !isWeReservedSpace()) {
            return extendLifetimeMillis(newLifetime);
        }
        newLifetime = extendLifetimeMillis(newLifetime);

        if( remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        SRMUser user = getUser();
        return getStorage().srmExtendReservationLifetime(user,spaceToken,newLifetime);


    }

}
