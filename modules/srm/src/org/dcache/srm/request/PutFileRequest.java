// $Id$

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
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.AbstractStorageElement;
import org.globus.util.GlobusURL;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.SrmReserveSpaceCallbacks;
import org.dcache.srm.SrmReleaseSpaceCallbacks;
import org.dcache.srm.SrmUseSpaceCallbacks;
import org.dcache.srm.SrmCancelUseOfSpaceCallbacks;
import org.dcache.srm.SRMInvalidRequestException;

import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.apache.axis.types.URI;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;


/**
 *
 * @author  timur
 * @version
 */
public class PutFileRequest extends FileRequest {

    // this is surl path
    private GlobusURL surl;
    private GlobusURL turl;
    private long size;
    // parent directory info
    private String fileId;
    private String parentFileId;
    private FileMetaData fmd;
    private FileMetaData parentFmd;
    private String spaceReservationId;
    private boolean weReservedSpace;
    private TAccessLatency accessLatency ;//null by default
    private TRetentionPolicy retentionPolicy;//null default value
    private boolean spaceMarkedAsBeingUsed=false;
    private static final long serialVersionUID = 542933938646172116L;

    /** Creates new FileRequest */
    public PutFileRequest(Long requestId,
            Long requestCredentalId,
            Configuration configuration,
            String url,
            long size,
            long lifetime,
            JobStorage jobStorage,
            AbstractStorageElement storage,
            int maxNumberOfRetires,
            String spaceReservationId,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency
            ) throws Exception {
        super(requestId,
                requestCredentalId,
                configuration,
                lifetime,
                jobStorage,
                maxNumberOfRetires);
        say("constructor url = "+url+")");
        try {
            surl = new GlobusURL(url);
            say("    surl = "+surl);

        } catch(MalformedURLException murle) {
            throw new IllegalArgumentException(murle.toString());
        }
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
            Long id,
            Long nextJobId,
            JobStorage jobStorage,
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
            Long requestCredentalId,
            String statusCodeString,
            Configuration configuration,
            String SURL,
            String TURL,
            String fileId,
            String parentFileId,
            String spaceReservationId,
            long size,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency
            ) throws java.sql.SQLException {
        super(id,
                nextJobId,
                jobStorage,
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
                statusCodeString,
                configuration
                );

        try {
            this.surl = new GlobusURL(SURL);
            if(TURL != null && (!TURL.equalsIgnoreCase("null"))) {
                this.turl = new GlobusURL(TURL);
            }
        } catch(MalformedURLException murle) {
            throw new IllegalArgumentException(murle.toString());
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

    public void say(String s) {
        if(storage != null) {
            storage.log("PutFileRequest #"+/*getFileId()+*/": "+s);
        }

    }

    public void esay(String s) {
        if(storage != null) {
            storage.elog("PutFileRequest #"+/*getFileId()+*/": "+s);
        }
    }

    public void esay(Throwable t) {
        if(storage != null) {
            storage.elog(t);
        }
    }


    public String getFileId() {
        return fileId;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public long getSize() {
        return size;
    }


    private void setTurl(String turl_string) throws java.net.MalformedURLException {
        this.turl = new GlobusURL(turl_string);
    }



    public GlobusURL getSurl() {
        return surl;
    }

    public String getPath() {
        String path = surl.getPath();
        int indx=path.indexOf(SFN_STRING);
        if( indx != -1) {

            path=path.substring(indx+SFN_STRING.length());
        }

        if(!path.startsWith("/")) {
            path = "/"+path;
        }
        return path;
    }


    public String getSurlString() {
        return surl.getURL().toString();
    }



    public String getTurlString() {
        State state = getState();
        if(turl == null && (state == State.READY ||
			    state == State.TRANSFERRING)) {
            try {
                turl = getTURL();

            }
	    catch(SRMAuthorizationException srme) {
                String error =srme.getMessage();
                esay(error);
                synchronized(this) {
                    State state1 = getState();
                    if(state1 != State.DONE && state1 != State.CANCELED && state1 != State.FAILED) {
                        try {
                            setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                            setState(State.FAILED,error);
                        }
			catch(Exception e) {
                            esay("can not fail state:"+e);
                        }
                    }
                }
            }
	    catch(Exception srme) {
                String error =
		    "can not obtain turl for file:"+srme;
                esay(error);
                synchronized(this) {
                    State state1 = getState();
                    if(state1 != State.DONE && state1 != State.CANCELED && state1 != State.FAILED) {
                        try {
                            setState(State.FAILED,error);
                        } catch(Exception e) {
                            esay("can not fail state:"+e);
                        }
                    }
                }

            }
        }

        if(turl!= null) {
            return turl.getURL().toString();
        }
        return null;
    }


    public boolean canWrite() throws SRMInvalidRequestException {
        if(fileId == null && parentFileId == null) {
            return false;
        }
        SRMUser user = (SRMUser) getUser();
        boolean canwrite =storage.canWrite(user,fileId,fmd,parentFileId,parentFmd,
                ((PutRequest)getRequest()).isOverwrite());
        say("PutFileRequest  storage.canWrite() returned "+canwrite);
        return  canwrite;
    }


    public RequestFileStatus getRequestFileStatus() {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = getId().intValue();

        rfs.SURL = getSurlString();
        rfs.size = getSize();
        State state = getState();
        if(state == State.RQUEUED) {
            tryToReady();
            state = getState();
        }
        // call getTurlString only after we called
        // tryToReady, otherwise we might get a ready 
        // request without TURL!!!
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

        say(" returning requestFileStatus for "+this.toString());
        return rfs;
    }

    public TPutRequestFileStatus getTPutRequestFileStatus()
            throws java.sql.SQLException, SRMInvalidRequestException{
        TPutRequestFileStatus fileStatus = new TPutRequestFileStatus();
        fileStatus.setFileSize(new org.apache.axis.types.UnsignedLong(getSize()));


        URI surl;
        try {
            surl= new URI(getSurlString());
        } catch (Exception e) {
            esay(e);
            throw new java.sql.SQLException("wrong surl format");
        }
        fileStatus.setSURL(surl);
        //fileStatus.set

        String turlstring = getTurlString();
        if(turlstring != null) {
            URI transferURL;
            try {
                transferURL = new URI(turlstring);
            } catch (Exception e) {
                esay(e);
                throw new java.sql.SQLException("wrong turl format");
            }
            fileStatus.setTransferURL(transferURL);

        }
        fileStatus.setEstimatedWaitTime(new Integer(getRequest().getRetryDeltaTime()));
        fileStatus.setRemainingPinLifetime(new Integer((int)getRemainingLifetime()/1000));
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

    public TSURLReturnStatus  getTSURLReturnStatus() throws java.sql.SQLException{
        URI surl;
        try {
            surl = new URI(getSurlString());
        } catch (Exception e) {
            esay(e);
            throw new java.sql.SQLException("wrong surl format");
        }
        TReturnStatus returnStatus = getReturnStatus();
        if(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED.equals(returnStatus.getStatusCode())) {
            //SRM_SPACE_LIFETIME_EXPIRED is illeal on the file level,
            // but we use it to correctly calculate the request level status
            // so we do the translation here
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        surlReturnStatus.setSurl(surl);
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }

    private GlobusURL getTURL() throws SRMException, java.sql.SQLException {
        String firstDcapTurl = null;
        PutRequest request = (PutRequest) Job.getJob(requestId);
        // do not synchronize on request, since it might cause deadlock
        firstDcapTurl = request.getFirstDcapTurl();
        if(firstDcapTurl == null) {
            try {
                String turl = storage.getPutTurl((SRMUser)request.getUser(),getPath(),
                        request.protocols);
                GlobusURL g_turl = new GlobusURL(turl);
                if(g_turl.getProtocol().equals("dcap")) {
                    request.setFirstDcapTurl(turl);
                }
                return g_turl;
            } catch(MalformedURLException murle) {
                esay(murle);
                throw new SRMException(murle.toString());
            }
        }

        try {

            String turl =storage.getPutTurl(request.getUser(),getPath(), firstDcapTurl);
            return new GlobusURL(turl);
        } catch(MalformedURLException murle) {
            esay(murle);
            throw new SRMException(murle.toString());
        }
    }


    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(" PutFileRequest # ").append(getId());
        sb.append("\n          State =").append(getState());
        sb.append("\n           SURL   =").append(getSurlString());
        sb.append("\n           size   =").append(getSize());
        try{
            sb.append("\n           TURL   =").append(getTurlString());
        } catch(Exception e) {
            sb.append("\n           TURL   =<error ").append(e).append('>');
        }

        return sb.toString();
    }

    public void done() {
    }

    public void error() {
    }

    public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure,
            org.dcache.srm.scheduler.FatalJobFailure {
        addDebugHistoryEvent("run method is executed");
        String  path = getPath();

        try {
            if(fileId == null &&
                    parentFileId == null) {

                addDebugHistoryEvent("selecting transfer protocol");
                if(!Tools.sameHost(configuration.getSrmhost(),
                        getSurl().getHost())) {
                    String error ="surl is not local : "+getSurl().getURL();
                    synchronized(this) {

                        State state = getState();
                        if(!State.isFinalState(state)) {
                            setState(State.FAILED,error);
                        }
                    }
                    esay("can not prepare to put file request fr :"+this);
                    esay(error);
                    return;
                }
                // if we can not read this path for some reason
                //(not in ftp root for example) this will throw exception
                // we do not care about the return value yet
                PutRequest request = (PutRequest) getJob(requestId);
                String[] supported_prots = storage.supportedPutProtocols();
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
                    throw new org.dcache.srm.scheduler.FatalJobFailure("transfer protocols not supported");
                }

                //storage.getPutTurl(getUser(),path,request.protocols);
                PutCallbacks callbacks = new PutCallbacks(this.getId());
              /*  synchronized(callbacks_set)
                {
                    callbacks_set.add(callbacks);
                }
               **/
                synchronized(this) {

                    State state = getState();
                    if(!State.isFinalState(state)) {
                        setState(State.ASYNCWAIT, "calling Storage.prepareToPut()");
                    }
                }
                storage.prepareToPut(getUser(),path,callbacks,
                        ((PutRequest)getRequest()).isOverwrite());
                return;
            }
            addDebugHistoryEvent("checking user has permission to  write");
            say("fileId = "+fileId+" parentFileId="+parentFileId);
            if(!canWrite()) {
                synchronized(this) {
                    State state = getState();
                    if(!State.isFinalState(state)) {
                        String error = "user "+getUser()+"has not permission to write "+surl;
                        esay( error);
                        try {
                            setState(State.FAILED,error);
                        } catch(IllegalStateTransition ist) {
                            esay("can not fail state:"+ist);
                        }
                    }
                }
                return;

            }
	    long defaultSpaceReservationId=0;
	    if (parentFmd.spaceTokens!=null) { 
		    if (parentFmd.spaceTokens.length>0) { 
			    defaultSpaceReservationId=parentFmd.spaceTokens[0];
		    }
	    }
	    if (spaceReservationId==null) { 
		    if (defaultSpaceReservationId!=0) {
			    if(retentionPolicy==null&&accessLatency==null) { 
				    StringBuffer sb = new StringBuffer();
				    sb.append(defaultSpaceReservationId);
				    spaceReservationId=sb.toString();
			    }
		    }
	    }
	    
	    if (configuration.isReserve_space_implicitely()&&spaceReservationId == null) { 
		    synchronized(this) {
			    State state = getState();
			    if(!State.isFinalState(state)) {
				    setState(State.ASYNCWAIT,"reserving space");
			    }
		    }
		    long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
		    SrmReserveSpaceCallbacks callbacks = new PutReserveSpaceCallbacks(getId());
		    //
		    //the following code allows the inheritance of the
		    // retention policy from the directory metatada
		    //
		    if(retentionPolicy == null && parentFmd != null && parentFmd.retentionPolicyInfo != null ) {
			    retentionPolicy = parentFmd.retentionPolicyInfo.getRetentionPolicy();
		    }
		    //
		    //the following code allows the inheritance of the
		    // access latency from the directory metatada
		    //
		    if(accessLatency == null && parentFmd != null && parentFmd.retentionPolicyInfo != null ) {
			    accessLatency = parentFmd.retentionPolicyInfo.getAccessLatency();
		    }
		    say("reserving space, size="+(size==0?1L:size));
		    storage.srmReserveSpace(
			    getUser(),
			    size==0?1L:size,
			    remaining_lifetime,
			    retentionPolicy ==null ? null: retentionPolicy.getValue(),
			    accessLatency == null? null:accessLatency.getValue(),
				    null,
			    callbacks);
		    return;
	    }
	    if( spaceReservationId != null &&
		!spaceMarkedAsBeingUsed) {
		    synchronized(this) {
			    State state = getState();
			    if(!State.isFinalState(state)) {
				    setState(State.ASYNCWAIT,"marking space as being used");
			    }
		    }
		    long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
		    SrmUseSpaceCallbacks  callbacks = new PutUseSpaceCallbacks(getId());
		    storage.srmMarkSpaceAsBeingUsed(getUser(),
						    spaceReservationId,
						    getPath(),
						    size==0?1:size,
						    remaining_lifetime,
						    ((PutRequest)getRequest()).isOverwrite(),
							    callbacks );
		    return;
	    }
	    say("run() returns, scheduler should bring file request into the ready state eventually");
	    return;
	}
	catch(Exception e) {
		esay("can not prepare to put : ");
		esay(e);
		String error ="can not prepare to put : "+e;
		try {
			synchronized(this) {
				State state = getState();
				if(!State.isFinalState(state)) {
					setState(State.FAILED,error);
				}
			}
		} 
		catch(IllegalStateTransition ist) {
			esay("can not faile state:"+ist);
		}
	}
    }
	

    protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
        State state = getState();
        say("State changed from "+oldState+" to "+getState());
        if(state == State.READY) {
            try {
                getRequest().resetRetryDeltaTime();
            } catch (SRMInvalidRequestException ire) {
                esay(ire);
            }
        }
        SRMUser user;
         try {
             user = getUser();
         }catch(SRMInvalidRequestException ire) {
             esay(ire);
             return;
         }
        if(State.isFinalState(state)) {
            say("space reservation is "+spaceReservationId);
            if(configuration.isReserve_space_implicitely() &&
                    spaceReservationId != null &&
                    spaceMarkedAsBeingUsed ) {
                SrmCancelUseOfSpaceCallbacks callbacks =
                        new PutCancelUseOfSpaceCallbacks(getId());
                storage.srmUnmarkSpaceAsBeingUsed(user,
                        spaceReservationId,getPath(),
                        callbacks);

            }
            if(spaceReservationId != null && weReservedSpace) {
                say("storage.releaseSpace("+spaceReservationId+"\"");
                SrmReleaseSpaceCallbacks callbacks =
                        new PutReleaseSpaceCallbacks(this.getId());
                storage.srmReleaseSpace(  user,
                        spaceReservationId,
                        (Long)null, //release all of space we reserved
                        callbacks);

            }
        }
    }

    /**
     * Getter for property parentFileId.
     * @return Value of property parentFileId.
     */
    public java.lang.String getParentFileId() {
        return parentFileId;
    }

    /**
     * Getter for property spaceReservationId.
     * @return Value of property spaceReservationId.
     */
    public java.lang.String getSpaceReservationId() {
        return spaceReservationId;
    }

    /**
     * Setter for property spaceReservationId.
     * @param spaceReservationId New value of property spaceReservationId.
     */
    public void setSpaceReservationId(java.lang.String spaceReservationId) {
        this.spaceReservationId = spaceReservationId;
    }

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

    private static class PutCallbacks implements PrepareToPutCallbacks {
        Long fileRequestJobId;

        public PutCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public PutFileRequest getPutFileRequest() throws SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }

        public void DuplicationError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setStatusCode(TStatusCode.SRM_DUPLICATION_ERROR);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void Error( String error) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setState(State.FAILED,error);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks error: "+ error);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void Exception( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,error);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("PutCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }

        public void GetStorageInfoFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }


        public void StorageInfoArrived(String fileId,FileMetaData fmd,String parentFileId, FileMetaData parentFmd) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("StorageInfoArrived: FileId:"+fileId);
                State state;
                synchronized(fr) {
                    state = fr.getState();
                }
                if(state == State.ASYNCWAIT) {
                    fr.say("PutCallbacks StorageInfoArrived for file "+fr.getSurlString());
                    fr.fileId = fileId;
                    fr.fmd = fmd;
                    fr.parentFileId = parentFileId;
                    fr.parentFmd = parentFmd;

                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        fr.esay(ie);
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void Timeout() {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setState(State.FAILED,"PutCallbacks Timeout");
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks Timeout");
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void InvalidPathError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void AuthorizationError(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PutReserveSpaceCallbacks implements SrmReserveSpaceCallbacks {
        Long fileRequestJobId;

        public PutFileRequest getPutFileRequest() 
                throws java.sql.SQLException,
                SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }


        public PutReserveSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public void ReserveSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,error);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("PutReserveSpaceCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }

        public void ReserveSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutReserveSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void NoFreeSpace(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutReserveSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("Space Reserved: spaceReservationToken:"+spaceReservationToken);
                State state;
                synchronized(fr) {
                    state = fr.getState();
                }
                if(state == State.ASYNCWAIT) {
                    fr.say("PutReserveSpaceCallbacks Space Reserved for file "+fr.getSurlString());
                    fr.setWeReservedSpace(true);
                    fr.setSpaceReservationId(spaceReservationToken);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        fr.esay(ie);
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PutReleaseSpaceCallbacks implements SrmReleaseSpaceCallbacks {
        Long fileRequestJobId;

        public PutFileRequest getPutFileRequest()
                throws java.sql.SQLException,
                  SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }


        public PutReleaseSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public void ReleaseSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                fr.esay("PutReleaseSpaceCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }

        public void ReleaseSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();

                fr.esay("PutReleaseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void SpaceReleased(String spaceReservationToken, long reservedSpaceSize) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("Space Released: spaceReservationToken:"+spaceReservationToken+" remaining space="+reservedSpaceSize);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PutUseSpaceCallbacks implements SrmUseSpaceCallbacks {
        Long fileRequestJobId;

        public PutFileRequest getPutFileRequest() 
                throws java.sql.SQLException,
                SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }


        public PutUseSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public void SrmUseSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,error);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("PutUseSpaceCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }

        public void SrmUseSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
        /**
         * call this if space reservation exists, but has no free space
         */
        public void SrmNoFreeSpace(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }

        }

        /**
         * call this if space reservation exists, but has been released
         */
        public void SrmReleased(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }

        }

        /**
         * call this if space reservation exists, but has been released
         */
        public void SrmExpired(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setStatusCode(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }

        }

        /**
         * call this if space reservation exists, but not authorized
         */
        public void SrmNotAuthorized(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                    synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                            fr.setState(State.FAILED,reason);
                        }
                    }
                } catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }

                fr.esay("PutUseSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }

        }

        public void SpaceUsed() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("Space Marked as Being Used");
                State state;
                synchronized(fr) {
                    state = fr.getState();
                }
                if(state == State.ASYNCWAIT) {
                    fr.say("PutUseSpaceCallbacks Space Marked as Being Used for file "+fr.getSurlString());
                    fr.setSpaceMarkedAsBeingUsed(true);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    } catch(Exception ie) {
                        fr.esay(ie);
                    }
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class PutCancelUseOfSpaceCallbacks implements SrmCancelUseOfSpaceCallbacks {
        Long fileRequestJobId;

        public PutFileRequest getPutFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }


        public PutCancelUseOfSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public void CancelUseOfSpaceFailed( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                String error = e.toString();
                fr.esay("PutCancelUseOfSpaceCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }

        public void CancelUseOfSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();

                fr.esay("PutCancelUseOfSpaceCallbacks error: "+ reason);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void UseOfSpaceSpaceCanceled() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("Umarked Space as Being Used");
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private  static class TheReleaseSpaceCallbacks implements org.dcache.srm.ReleaseSpaceCallbacks {

        Long fileRequestJobId;

        public TheReleaseSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }

        public PutFileRequest getPutFileRequest() 
                throws java.sql.SQLException, SRMInvalidRequestException {
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }

        public void Error( String error) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                /*
                 * releaseSpace is called when the file request  is already
                 * in a final state
                 */
                /*
                 try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                fr.esay("TheReleaseSpaceCallbacks error: "+ error);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void Exception( Exception e) {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                /*
                 * releaseSpace is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                fr.esay("TheReleaseSpaceCallbacks exception");
                fr.esay(e);
            } catch(Exception e1) {
                e1.printStackTrace();
            }
        }




        public void Timeout() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                /*
                 * releaseSpace is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //fr.esay("can not fail state:"+ist);
                }
                 */

                fr.esay("TheReleaseSpaceCallbacks Timeout");
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void SpaceReleased() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("TheReleaseSpaceCallbacks: SpaceReleased");
                fr.setSpaceReservationId(null);
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

        public void ReleaseSpaceFailed(String reason){
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.setSpaceReservationId(null);
                /*
                 * releaseSpace is called when the file request  is already
                 * in a final state
                 */
                /*
                try {
                    //fr.setState(State.FAILED);
                }
                catch(IllegalStateTransition ist) {
                    //fr.esay("can not fail state:"+ist);
                }
                 */

                fr.esay("TheReleaseSpaceCallbacks error: "+ reason+" ignoring, could have been all used up");
            } catch(Exception e) {
                e.printStackTrace();
            }
        }

    }

    public boolean isWeReservedSpace() {
        return weReservedSpace;
    }

    public void setWeReservedSpace(boolean weReservedSpace) {
        this.weReservedSpace = weReservedSpace;
    }

    public boolean isSpaceMarkedAsBeingUsed() {
        return spaceMarkedAsBeingUsed;
    }

    public void setSpaceMarkedAsBeingUsed(boolean spaceMarkedAsBeingUsed) {
        this.spaceMarkedAsBeingUsed = spaceMarkedAsBeingUsed;
    }

    public TAccessLatency getAccessLatency() {
        return accessLatency;
    }

    public void setAccessLatency(TAccessLatency accessLatency) {
        this.accessLatency = accessLatency;
    }

    public TRetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }

    public void setRetentionPolicy(TRetentionPolicy retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }

    /**
     *
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
        String spaceToken =spaceReservationId;

        if(!configuration.isReserve_space_implicitely() ||
           spaceToken == null ||
           !weReservedSpace) {
            return extendLifetimeMillis(newLifetime);
        }
        newLifetime = extendLifetimeMillis(newLifetime);

        if( remainingLifetime >= newLifetime) {
            return remainingLifetime;
        }
        SRMUser user =(SRMUser) getUser();
        return storage.srmExtendReservationLifetime(user,spaceToken,newLifetime);


    }

}

// $Log: not supported by cvs2svn $
// Revision 1.43  2007/10/03 20:33:02  litvinse
// throw SRMAuthorisation exeption in getTurl
//
// Revision 1.42  2007/09/19 22:41:29  timur
// Default access latency and retention policy is now based on directory AccessLatency and RetentionPolicy tags if these are present
//
// Revision 1.41  2007/09/14 21:11:43  timur
// rename srmSpaceManager option into srmImplicitSpaceManagerEnabled, make its value set to yes by default is srmSpaceManagerEnabled=yes and always set to no if srmImplicitSpaceManagerEnabled=no
//
// Revision 1.40  2007/09/13 19:14:30  timur
// return SRM AUTHORIZATION or INVALID PATH errors instead of generic SRM_FAILURE in several instances
//
// Revision 1.39  2007/08/03 15:47:58  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.38  2007/06/18 21:44:58  timur
// better reporting of the expired space reservations
//
// Revision 1.37  2007/05/15 01:50:55  timur
// if no space is available, return SRM_NO_FREE_SPACE
//
// Revision 1.36  2007/04/11 23:34:42  timur
// Propagate SrmNoFreeSpace and SrmSpaceReleased errors in case of useSpace function
//
// Revision 1.35  2007/03/10 00:13:20  timur
// started work on adding support for optional overwrite
//
// Revision 1.34  2007/03/03 00:43:05  timur
// make srm reserve space and space get metadata return correct values, set status before changing request state, to make it save its value in database
//
// Revision 1.33  2007/02/20 01:37:56  timur
// more changes to report status according to the spec and make ls report lifetime as -1 (infinite)
//
// Revision 1.32  2007/02/17 05:44:25  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace, refactored database code a bit
//
// Revision 1.31  2007/02/10 04:46:15  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.29  2007/01/11 22:33:09  timur
// fixed bugs: not accounting for possible null values and making correct sql statement
//
// Revision 1.28  2007/01/10 23:00:24  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.27  2006/10/10 20:59:57  timur
// more changes for srmBringOnline
//
// Revision 1.26  2006/10/04 21:20:33  timur
// different calculation of the v2.2 status
//
// Revision 1.25  2006/08/25 00:18:15  timur
// space reservation and synchronization issue resolution
//
// Revision 1.24  2006/08/18 22:05:32  timur
// srm usage of space by srmPrepareToPut implemented
//
// Revision 1.23  2006/08/08 15:33:34  timur
// do not return SRM_REQUEST_SUSPENDED status
//
// Revision 1.22  2006/08/07 21:03:59  timur
// implemented srmStatusOfReserveSpaceRequest
//
// Revision 1.21  2006/07/10 22:03:28  timur
// updated some of the error codes
//
// Revision 1.20  2006/06/21 20:29:53  timur
// Upgraded code to the latest srmv2.2 wsdl (final)
//
// Revision 1.19  2006/06/20 15:42:17  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.18  2006/04/26 17:17:55  timur
// store the history of the state transitions in the database
//
// Revision 1.17  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.16  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.15  2006/03/31 23:26:59  timur
// better error reporting
//
// Revision 1.14  2006/03/14 17:44:19  timur
// moving toward the axis 1_3
//
// Revision 1.13  2006/02/02 01:27:16  timur
// better error propagation to the user
//
// Revision 1.12  2005/12/12 22:35:47  timur
// more work on srmPrepareToGet and related srm v2 functions
//
// Revision 1.11  2005/11/04 22:23:31  timur
// if file size is not known (0) do not reserve space for it  and do not reject it in gridftp transfer
//
// Revision 1.10  2005/10/07 22:57:16  timur
// work for srm v2
//
// Revision 1.9  2005/10/03 19:02:40  timur
// space release failure should not case transfer failures, if the transfer succeded
//
// Revision 1.8  2005/05/12 21:42:00  timur
// use AbstractStorageElement.getSupported[Get/Put]Protocols() to determine supported protocols and not getTurl
//
// Revision 1.7  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.6  2005/03/23 18:10:38  timur
// more space reservation related changes, need to support it in case of "copy"
//
// Revision 1.5  2005/03/11 21:16:26  timur
// making srm compatible with cern tools again
//
// Revision 1.4  2005/03/09 23:20:49  timur
// more database checks, more space reservation code
//
// Revision 1.3  2005/03/01 23:10:38  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.2  2005/02/02 22:19:30  timur
// make sure we call canRead/Write of the storage when performing get/put
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.6  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.5  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.4  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.3  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.11  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.10  2004/07/12 21:52:06  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.1.2.9  2004/07/02 20:10:24  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.8  2004/06/30 20:37:24  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.7  2004/06/24 23:03:07  timur
// put requests, put file requests and copy file requests are now stored in database, copy requests need more work
//
// Revision 1.1.2.6  2004/06/22 01:38:06  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.1.2.5  2004/06/16 19:44:33  timur
// added cvs logging tags and fermi copyright headers at the top, removed Copier.java and CopyJob.java
//


