// $Id: PutFileRequest.java,v 1.21.2.1 2006-08-25 00:21:29 timur Exp $
// $Log: not supported by cvs2svn $
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

import java.net.InetAddress;
import java.net.MalformedURLException;

import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.AbstractStorageElement;
import org.globus.util.GlobusURL;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.Tools;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobCreator;
import org.dcache.srm.PrepareToPutCallbacks;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.ReserveSpaceCallbacks;

import org.dcache.srm.v2_2.TGroupPermission;
import org.dcache.srm.v2_2.TUserPermission;
import org.dcache.srm.v2_2.TFileStorageType;
import org.dcache.srm.v2_2.TFileType;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TPermissionMode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmPrepareToPutRequest;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.TPutFileRequest;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.apache.axis.types.URI;
import org.dcache.srm.v2_2.TSURLReturnStatus;

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
    private String clientHost;
    
    private static final long serialVersionUID = 542933938646172116L;
    
    /** Creates new FileRequest */
    public PutFileRequest(Long requestId,
    Long requestCredentalId,
    String requestUserId,
    Configuration configuration,
    String url,
    long size,
    long lifetime,
    JobStorage jobStorage,
    AbstractStorageElement storage,
    int maxNumberOfRetires,
    String clientHost
    ) throws Exception {
        super(requestId,
        requestCredentalId,
        requestUserId,
        configuration,
        lifetime,
        jobStorage,
        maxNumberOfRetires);
        say("constructor url = "+url+")");
        try {
            surl = new GlobusURL(url);
            say("    surl = "+surl);
            
        }
        catch(MalformedURLException murle) {
            throw new IllegalArgumentException(murle.toString());
        }
        this.size = size;
        this.clientHost = clientHost;
    }
    
    
    
    public PutFileRequest(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    String creatorId,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long requestId,
    Long requestCredentalId,
    Configuration configuration,
    String SURL,
    String TURL,
    String fileId,
    String parentFileId,
    String spaceReservationId,
    String clientHost,
    long size
    ) throws java.sql.SQLException {
        super(id,
        nextJobId,
        jobStorage,
        creationTime,
        lifetime,
        stateId,
        errorMessage,
        creatorId,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,
        maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray, 
        requestId,
        requestCredentalId,
        configuration
        );
        
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
        
        if(parentFileId != null && (!parentFileId.equalsIgnoreCase("null"))) {
            this.parentFileId = parentFileId;
        }
        this.spaceReservationId = spaceReservationId;
        this.clientHost = clientHost;
        this.size = size;
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
            catch(Exception srme) {
                String error =
                "can not obtain turl for file:"+srme;
                esay(error);
               synchronized(this) {

                    State state1 = getState();
                    if(state1 != State.DONE && state1 != State.CANCELED && state1 != State.FAILED) {
                        try {
                            setState(State.FAILED,error);
                        }
                        catch(Exception e) {
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
    
    
    public boolean canWrite() {
        if(fileId == null && parentFileId == null) {
            return false;
        }
        SRMUser user = (SRMUser) getUser();
        boolean canwrite =storage.canWrite(user,fileId,fmd,parentFileId,parentFmd);
        say("PutFileRequest  storage.canWrite() returned "+canwrite);
        return  canwrite;
    }
    
    
    public RequestFileStatus getRequestFileStatus() {
        RequestFileStatus rfs = new RequestFileStatus();
        rfs.fileId = getId().intValue();
        
        rfs.SURL = getSurlString();
        rfs.size = new Long(getSize()).longValue();
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
        
        say(" returning requestFileStatus for "+this.toString());
        return rfs;
    }
    
   public TPutRequestFileStatus getTPutRequestFileStatus() throws java.sql.SQLException{
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
        TReturnStatus returnStatus = new TReturnStatus();
        State state = getState();
        returnStatus.setExplanation(state.toString());
        if(state == State.DONE) {
            returnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
        }
        else if(state == State.READY) {
            returnStatus.setStatusCode(TStatusCode.SRM_SPACE_AVAILABLE);
        }
        else if(state == State.TRANSFERRING) {
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
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
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_SUSPENDED);
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
        TReturnStatus returnStatus = new TReturnStatus();
        
        State state = getState();
        if(state == State.DONE) {
            returnStatus.setStatusCode(TStatusCode.SRM_DONE);
        }
        else if(state == State.READY) {
            returnStatus.setStatusCode(TStatusCode.SRM_DONE);
        }
        else if(state == State.TRANSFERRING) {
            returnStatus.setStatusCode(TStatusCode.SRM_DONE);
        }
        else if(state == State.FAILED) {
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
            returnStatus.setStatusCode(TStatusCode.SRM_REQUEST_SUSPENDED);
        }
        TSURLReturnStatus surlReturnStatus = new TSURLReturnStatus();
        surlReturnStatus.setSurl(surl);
        surlReturnStatus.setStatus(returnStatus);
        return surlReturnStatus;
    }
    
    private GlobusURL getTURL() throws SRMException, java.sql.SQLException {
        String firstDcapTurl = null;
        PutRequest request = (PutRequest) Job.getJob(requestId);
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
                }
                catch(MalformedURLException murle) {
                    esay(murle);
                    throw new SRMException(murle.toString());
                }
            }
        
        try {
            
            String turl =storage.getPutTurl(request.getUser(),getPath(), firstDcapTurl);
            return new GlobusURL(turl);
        }
        catch(MalformedURLException murle) {
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
        }
        catch(Exception e) {
            sb.append("\n           TURL   =<error ").append(e).append('>');
        }
        
        return sb.toString();
    }
    
    public boolean equals(Object o) {
        if(o == this) {
            return true;
        }
        
        return false;
    }
    
    public void done() {
    }
    
    public void error() {
    }
    
    public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure,
    org.dcache.srm.scheduler.FatalJobFailure {
        String  path = getPath();
        
        try {
            if(fileId == null && 
            parentFileId == null) {
                
            
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
               storage.prepareToPut(getUser(),path,callbacks);
               return;
            }
            
            say("fileId = "+fileId+" parentFileId="+parentFileId);
            if(!canWrite()) {
                     synchronized(this) {
                        State state = getState();
                        if(!State.isFinalState(state)) {
                            String error = "user "+getUser()+"has not permission to write "+surl;
                            esay( error);
                            try {
                                setState(State.FAILED,error);
                            }
                            catch(IllegalStateTransition ist) {
                                esay("can not fail state:"+ist);
                            }
                        }
                     }
                     return;
                
            }
            
            if( size>0 && configuration.isReserve_space() && 
               spaceReservationId == null) {
               synchronized(this) {

                    State state = getState();
                    if(!State.isFinalState(state)) {
                        setState(State.ASYNCWAIT,"reserve space");
                    }
               }
                
                long remaining_lifetime = lifetime - ( System.currentTimeMillis() -creationTime);
                ReserveSpaceCallbacks callbacks = new PutReserveSpaceCallbacks (getId());
                storage.reserveSpace(
                    getUser(), 
                    size, 
                    remaining_lifetime, 
                    getPath(), 
                    getClientHost(), 
                    callbacks);
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
                    if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
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
            getRequest().resetRetryDeltaTime();
        }
        if(State.isFinalState(state)) {
            say("space reservation is "+spaceReservationId);
            if(spaceReservationId != null) {
                say("storage.releaseSpace("+spaceReservationId+"\"");
                TheReleaseSpaceCallbacks callbacks = new TheReleaseSpaceCallbacks(this.getId());
                storage.releaseSpace(  getUser(),
                    spaceReservationId,
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
    
    /**
     * Getter for property clientHost.
     * @return Value of property clientHost.
     */
    public java.lang.String getClientHost() {
        return clientHost;
    }
    
    /**
     * Setter for property clientHost.
     * @param clientHost New value of property clientHost.
     */
    public void setClientHost(java.lang.String clientHost) {
        this.clientHost = clientHost;
    }
    
    private static class PutCallbacks implements PrepareToPutCallbacks {
        Long fileRequestJobId;
        
        public PutCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }
        
        public PutFileRequest getPutFileRequest() throws java.sql.SQLException{
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
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
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutCallbacks error: "+ error);
            }
            catch(Exception e) {
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
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                fr.setState(State.FAILED,error);
                            }
                       }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("PutCallbacks exception");
                fr.esay(e);
            }
            catch(Exception e1) {
                e1.printStackTrace();
            }
        }
        
        public void GetStorageInfoFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                       synchronized(fr) {

                            State state = fr.getState();
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                fr.setState(State.FAILED,reason);
                            }
                       }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutCallbacks error: "+ reason);
            }
            catch(Exception e) {
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
                    }
                    catch(Exception ie) {
                        fr.esay(ie);
                    }
                }
            }
            catch(Exception e) {
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
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutCallbacks Timeout");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
    
    public static class PutReserveSpaceCallbacks implements ReserveSpaceCallbacks {
        Long fileRequestJobId;
        
        public PutFileRequest getPutFileRequest() throws java.sql.SQLException{
            Job job = Job.getJob(fileRequestJobId);
            if(job != null) {
                return (PutFileRequest) job;
            }
            return null;
        }
        

        public PutReserveSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
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
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutReserveSpaceCallbacks error: "+ error);
            }
            catch(Exception e) {
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
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                fr.setState(State.FAILED,error);
                            }
                       }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("PutReserveSpaceCallbacks exception");
                fr.esay(e);
            }
            catch(Exception e1) {
                e1.printStackTrace();
            }
        }
        
        public void ReserveSpaceFailed(String reason) {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                       synchronized(fr) {

                            State state = fr.getState();
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                fr.setState(State.FAILED,reason);
                            }
                       }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutReserveSpaceCallbacks error: "+ reason);
            }
            catch(Exception e) {
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
                    fr.setSpaceReservationId(spaceReservationToken);
                    Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                    try {
                        scheduler.schedule(fr);
                    }
                    catch(Exception ie) {
                        fr.esay(ie);
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void Timeout() {
            try {
                PutFileRequest fr = getPutFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,"PutReserveSpaceCallbacks Timeout");
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("PutReserveSpaceCallbacks Timeout");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
    }
    
    private  static class TheReleaseSpaceCallbacks implements org.dcache.srm.ReleaseSpaceCallbacks {
        
        Long fileRequestJobId;
        
        public TheReleaseSpaceCallbacks(Long fileRequestJobId) {
            this.fileRequestJobId = fileRequestJobId;
        }
        
        public PutFileRequest getPutFileRequest() throws java.sql.SQLException {
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
            }
            catch(Exception e) {
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
            }
            catch(Exception e1) {
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
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void SpaceReleased() {
            try {
                PutFileRequest fr = getPutFileRequest();
                fr.say("TheReleaseSpaceCallbacks: SpaceReleased");
                fr.setSpaceReservationId(null);
            }
            catch(Exception e) {
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
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
    }

}



