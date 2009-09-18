// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.32  2007/08/29 10:49:29  behrmann
// Added check for whether the given path points to a directory. If it does,
// the request will fail with an INVALID_PATH error.
//
// Revision 1.31  2007/03/03 00:43:05  timur
// make srm reserve space and space get metadata return correct values, set status before changing request state, to make it save its value in database
//
// Revision 1.30  2007/02/23 17:05:25  timur
// changes to comply with the spec and appear green on various tests, mostly propogating the errors as correct SRM Status Codes, filling in correct fields in srm ls, etc
//
// Revision 1.29  2007/02/20 01:37:56  timur
// more changes to report status according to the spec and make ls report lifetime as -1 (infinite)
//
// Revision 1.28  2007/02/10 04:46:15  timur
//  first version of SrmExtendFileLifetime
//
// Revision 1.26  2006/10/10 20:59:57  timur
// more changes for srmBringOnline
//
// Revision 1.25  2006/10/04 21:20:33  timur
// different calculation of the v2.2 status
//
// Revision 1.24  2006/08/25 00:18:42  timur
// synchronization issue resolution
//
// Revision 1.23  2006/08/08 15:33:33  timur
// do not return SRM_REQUEST_SUSPENDED status
//
// Revision 1.22  2006/07/10 22:03:28  timur
// updated some of the error codes
//
// Revision 1.21  2006/06/21 20:29:52  timur
// Upgraded code to the latest srmv2.2 wsdl (final)
//
// Revision 1.20  2006/06/20 15:42:17  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.19  2006/04/26 17:17:55  timur
// store the history of the state transitions in the database
//
// Revision 1.18  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.17  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.16  2006/03/31 23:26:59  timur
// better error reporting
//
// Revision 1.15  2006/03/14 17:44:19  timur
// moving toward the axis 1_3
//
// Revision 1.14  2006/02/02 01:27:16  timur
// better error propagation to the user
//
// Revision 1.13  2005/12/09 00:26:09  timur
// srmPrepareToGet works
//
// Revision 1.12  2005/12/02 22:20:51  timur
// working on srmReleaseFiles
//
// Revision 1.11  2005/11/20 02:40:11  timur
// SRM PrepareToGet and srmStatusOfPrepareToGet functions
//
// Revision 1.10  2005/11/17 20:45:55  timur
// started work on srmPrepareToGet functions
//
// Revision 1.9  2005/10/07 22:57:16  timur
// work for srm v2
//
// Revision 1.8  2005/05/12 21:42:00  timur
// use AbstractStorageElement.getSupported[Get/Put]Protocols() to determine supported protocols and not getTurl
//
// Revision 1.7  2005/04/27 19:55:06  timur
// added gridftp list
//
// Revision 1.6  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.5  2005/03/23 18:10:38  timur
// more space reservation related changes, need to support it in case of "copy"
//
// Revision 1.4  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
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
// Revision 1.7  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.6  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.5  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.4  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.3  2004/08/17 17:17:24  timur
// increment number of retries to avoid infinite retries
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.13  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.12  2004/07/29 22:17:29  timur
// Some functionality for disk srm is working
//
// Revision 1.1.2.11  2004/07/02 20:10:24  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.10  2004/06/30 20:37:23  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.9  2004/06/24 23:03:07  timur
// put requests, put file requests and copy file requests are now stored in database, copy requests need more work
//
// Revision 1.1.2.8  2004/06/22 01:38:06  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.1.2.7  2004/06/18 22:20:52  timur
// adding sql database storage for requests
//
// Revision 1.1.2.6  2004/06/16 19:44:33  timur
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

import java.net.MalformedURLException;

import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.FileMetaData;
import org.dcache.srm.AbstractStorageElement;
import org.globus.util.GlobusURL;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMAuthorizationException;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.util.Configuration;
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
/**
 *
 * @author  timur
 * @version
 */
public class GetFileRequest extends FileRequest {
    public static final String SFN_STRING="?SFN=";
    
    // the globus url class created from surl_string
    private GlobusURL surl;
    private GlobusURL turl;
    private String pinId;
    private String fileId;
    private FileMetaData fileMetaData;
    
    private static final long serialVersionUID = -9155373723705753177L;
    
    /** Creates new FileRequest */
    public GetFileRequest(Long requestId,
    Long  requestCredentalId,
    Configuration configuration,
    String url,
    long lifetime,
    JobStorage jobStorage,
    AbstractStorageElement storage,
    int maxNumberOfRetries
    
    ) throws Exception {
        super(requestId,
            requestCredentalId, 
            configuration, 
            lifetime, 
            jobStorage,
            maxNumberOfRetries);
        say("GetFileRequest, requestId="+requestId+" fileRequestId = "+getId());
        try {
            surl = new GlobusURL(url);
            say("    surl = "+surl.getURL());
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
    Long  requestCredentalId,
    String statusCodeString,            
    Configuration configuration,
    String SURL,
    String TURL,
    String fileId,
    String pinId
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
        configuration);
        
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
    
    public void say(String s) {
        if(storage != null) {
            storage.log("GetFileRequest reqId #"+requestId+" file#"+getId()+": "+s);
        }
        
    }
    
    public void esay(String s) {
        if(storage != null) {
            storage.elog("GetFileRequest eqId #"+requestId+" file#"+getId()+": "+s);
        }
    }
    
    public void esay(Throwable t) {
        if(storage != null) {
            storage.elog(t);
        }
    }
    
    public void setPinId(String pinId) {
        this.pinId = pinId;
    }
    
    public String getPinId() {
        return pinId;
    }
    
    public boolean isPinned() {
        return pinId != null;
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
    
    
    public GlobusURL getSurl() {
        return surl;
    }
    
    public GlobusURL getTurl() {
        return turl;
    }
    
    public String getSurlString() {
        return surl.getURL();
    }
    
    
    
    public String getTurlString()  {
        State state = getState();
        if(turl == null && (state == State.READY ||
        state == State.TRANSFERRING)) {
            try {
                turl = getTURL();
            }
            catch(SRMAuthorizationException srmae) {
                String error =srmae.getMessage();
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
                        }
                        catch(Exception e) {
                            esay("can not fail state:"+e);
                        }
                    }
               }
                
            }
        }
        
        if(turl!= null) {
            return turl.getURL();
        }
        return null;
    }
    
    
    
    public boolean canRead() throws SRMInvalidRequestException{
        if(fileId == null) {
            return false;
        }
        SRMUser user =(SRMUser) getUser();
        say("GetFileRequest calling storage.canRead()");
        return storage.canRead(user,fileId,fileMetaData);
    }
    
    
    public String getFileId() {
        return fileId;
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
        
        //say(" returning requestFileStatus for "+rfs.toString());
        return rfs;
    }
    
    public TGetRequestFileStatus getTGetRequestFileStatus() 
            throws java.sql.SQLException, SRMInvalidRequestException{
        TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
        if(fileMetaData != null) {
            fileStatus.setFileSize(new org.apache.axis.types.UnsignedLong(fileMetaData.size));
        }
         
        try {
             fileStatus.setSourceSURL(new URI(getSurlString()));
        } catch (Exception e) {
            esay(e);
            throw new java.sql.SQLException("wrong surl format");
        }
        
        String turlstring = getTurlString();
        if(turlstring != null) {
            try {
            fileStatus.setTransferURL(new URI(turlstring));
            } catch (Exception e) {
                esay(e);
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
            esay(e);
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
                    String turl = storage.getGetTurl(getUser(),
                    getPath(),
                    request.protocols);
                    if(turl == null) {
                        throw new SRMException("turl is null");
                    }
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
        }
        
        try {
            
            String turl =storage.getGetTurl(getUser(),getPath(), firstDcapTurl);
            if(turl == null) {
                return null;
            }
            return new GlobusURL(turl);
        }
        catch(MalformedURLException murle) {
            return null;
        }
    }
    
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(" GetFileRequest ");
        sb.append(" id =").append(getId());
        sb.append(" SURL=").append(surl==null?
            "null":surl.getURL());
        
        return sb.toString();
    }
    
    public synchronized void run() throws NonFatalJobFailure, FatalJobFailure {
        say("run()");
        try {
            if(fileId == null) {
                try {
                    if(!Tools.sameHost(configuration.getSrmhost(),
                    getSurl().getHost())) {
                        String error ="surl is not local : "+getSurl().getURL();
                        esay(error);
                        throw new FatalJobFailure(error);
                    }
                }
                catch(java.net.UnknownHostException uhe) {
                    esay(uhe);
                    throw new FatalJobFailure(uhe.toString());
                }

                say("fileId is null, asking to get a fileId");
                askFileId();
                if(fileId == null) {
                   synchronized(this) {

                        State state = getState();
                        if(!State.isFinalState(state)) {
                            setState(State.ASYNCWAIT, "getting file Id");
                        }
                   }
                    say("GetFileRequest: waiting async notification about fileId...");
                    return;
                }
            }
            say("fileId = "+fileId);
            
            if(pinId == null) {
                if(!canRead()) {
                    
                     synchronized(this) {
                                State state = getState();
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                esay( "user "+getUser()+"has no permission to read "+fileId);
                                try {
                                    setState(State.FAILED,"user "+getUser()+"has no permission to read "+fileId);
                                }
                                catch(IllegalStateTransition ist) {
                                    esay("can not fail state:"+ist);
                                }
                            }
                       }
                     return;
                }

                say("pinId is null, asking to pin ");
                pinFile();
                if(pinId == null) {
                       synchronized(this) {

                            State state = getState();
                            if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                                setState(State.ASYNCWAIT,"pinning file");
                            }
                       }
                    say("GetFileRequest: waiting async notification about pinId...");
                    return;
                }
            }
        } catch (SRMInvalidRequestException ire) {
            esay(ire);
            throw new FatalJobFailure(ire.getMessage());
        }
        catch(IllegalStateTransition ist) {
            throw new NonFatalJobFailure(ist.toString());
        }
        say("PinId is "+pinId+" returning, scheduler should change state to \"Ready\"");
        
    }
    
    public void askFileId() throws NonFatalJobFailure, FatalJobFailure {
        try {
            
            say(" proccessing the file request id "+getId());
            String  path =   getPath();
            say(" path is "+path);
            // if we can not read this path for some reason
            //(not in ftp root for example) this will throw exception
            // we do not care about the return value yet
            say("calling Job.getJob("+requestId+")");
            GetRequest request = (GetRequest) Job.getJob(requestId);
            say("this file request's request is  "+request);
            //this will fail if the protocols are not supported
            String[] supported_prots = storage.supportedGetProtocols();
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
            say("storage.prepareToGet("+path+",...)");
            GetFileInfoCallbacks callbacks = new GetCallbacks(getId());
            storage.getFileInfo(getUser(),path,callbacks);
        }
        catch(Exception e) {
            esay(e);
            throw new NonFatalJobFailure(e.toString());
        }
    }
    
    public void pinFile() throws NonFatalJobFailure, FatalJobFailure {
        try {
            
            PinCallbacks callbacks = new ThePinCallbacks(getId());
            say("storage.pinFile("+fileId+",...)");
            storage.pinFile(getUser(),
                fileId, 
                getRequest().getClient_host(),
                fileMetaData, lifetime, 
                    getRequestId().longValue() ,callbacks);
        }
        catch(Exception e) {
            esay(e);
            throw new NonFatalJobFailure(e.toString());
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
        if(State.isFinalState(state)) {
            if(fileId != null && pinId != null) {
                UnpinCallbacks callbacks = new TheUninCallbacks(this.getId());
                say("state changed to final state, unpinning fileId= "+ fileId+" pinId = "+pinId);
                SRMUser user;
                try {
                    user = getUser();
                } catch (SRMInvalidRequestException ire) {
                    esay (ire) ;
                    return;
                }
                storage.unPinFile(user,fileId, callbacks, pinId);
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
        if(pinId == null) {
            return newLifetime;
        }
        SRMUser user =(SRMUser) getUser();
        return storage.extendPinLifetime(user,fileId,pinId,newLifetime);
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
                   synchronized(fr) {

                        State state = fr.getState();
                        fr.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,reason);
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("GetCallbacks error: "+ reason);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
  
        public void Error( String error) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,error);
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("GetCallbacks error: "+ error);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void Exception( Exception e) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(!State.isFinalState(state)) {
                            fr.setState(State.FAILED,e.toString());
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("GetCallbacks exception");
                fr.esay(e);
            }
            catch(Exception e1) {
                e1.printStackTrace();
            }
        }
        
        public void GetStorageInfoFailed(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
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
                
                fr.esay("GetCallbacks error: "+ reason);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
            
        }
        
        
        
        public void StorageInfoArrived(String fileId,FileMetaData fileMetaData) {
            try {
                if (fileMetaData.isDirectory) {
                    FileNotFound("Path is a directory");
                    return;
                }

                GetFileRequest fr = getGetFileRequest();
                fr.say("StorageInfoArrived: FileId:"+fileId);
                State state ;
                synchronized(fr) {
                    state = fr.getState();
                }
                
                if(state == State.ASYNCWAIT || state == State.RUNNING) {
                    fr.fileId = fileId;
                    fr.fileMetaData = fileMetaData;

                    if(state == State.ASYNCWAIT) {
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        }
                        catch(Exception ie) {
                            fr.esay(ie);
                        }
                    }
                }
                
            }
            catch(Exception e) {
                e.printStackTrace();
            }            
        }
        
        
        public void Timeout() {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setState(State.FAILED,"GetCallbacks Timeout");
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("GetCallbacks Timeout");
            }
            catch(Exception e) {
                e.printStackTrace();
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
                fr.esay("ThePinCallbacks error: "+ error);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void Exception( Exception e) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setState(State.FAILED,e.toString());
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                fr.esay("ThePinCallbacks exception");
                fr.esay(e);
            }
            catch(Exception e1) {
                e1.printStackTrace();
            }
        }
        
        
        
        
        public void Timeout() {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                   synchronized(fr) {

                        State state = fr.getState();
                        if(state != State.DONE && state != State.CANCELED && state != State.FAILED) {
                            fr.setState(State.FAILED,"ThePinCallbacks Timeout");
                        }
                   }
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("GetCallbacks Timeout");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void Pinned(String pinId) {
            try {
                GetFileRequest fr = getGetFileRequest();
                State state;
                synchronized(fr ) {
                    state = fr.getState();
                }
                fr.say("ThePinCallbacks: Pinned() pinId:"+pinId);
                if(state == State.ASYNCWAIT || state == State.RUNNING) {
                    fr.pinId = pinId;
                    if(state == State.ASYNCWAIT) {
                        Scheduler scheduler = Scheduler.getScheduler(fr.getSchedulerId());
                        try {
                            scheduler.schedule(fr);
                        }
                        catch(Exception ie) {
                            fr.esay(ie);
                        }
                    }
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void PinningFailed(String reason) {
            try {
                GetFileRequest fr = getGetFileRequest();
                try {
                    fr.setState(State.FAILED,reason);
                }
                catch(IllegalStateTransition ist) {
                    fr.esay("can not fail state:"+ist);
                }
                
                fr.esay("ThePinCallbacks error: "+ reason);
            }
            catch(Exception e) {
                e.printStackTrace();
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
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                fr.esay("TheUninCallbacks error: "+ error);
            }
            catch(Exception e) {
                e.printStackTrace();
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
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                fr.esay("TheUninCallbacks exception");
                fr.esay(e);
            }
            catch(Exception e1) {
                e1.printStackTrace();
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
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                
                fr.esay("TheUninCallbacks Timeout");
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
        public void Unpinned(String pinId) {
            try {
                GetFileRequest fr = getGetFileRequest();
                fr.say("TheUninCallbacks: Unpinned() pinId:"+pinId);
                State state;
                synchronized(fr ) {
                    state = fr.getState();
                }
               if(state == State.ASYNCWAIT) {
                    fr.pinId = pinId;
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
                    //fr.esay("can not fail state:"+ist);
                }
                 */
                
                fr.esay("TheUninCallbacks error: "+ reason);
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
        
    }
}
