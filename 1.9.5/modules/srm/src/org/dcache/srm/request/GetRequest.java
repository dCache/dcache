// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.21  2007/01/10 23:00:24  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.20  2006/12/20 15:38:47  timur
// implemented getRequestSummary
//
// Revision 1.19  2006/10/02 23:29:17  timur
// implemented srmPing and srmBringOnline (not tested yet), refactored Request.java
//
// Revision 1.18  2006/06/21 20:29:53  timur
// Upgraded code to the latest srmv2.2 wsdl (final)
//
// Revision 1.17  2006/06/20 15:42:17  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.16  2006/05/26 21:17:46  timur
// remove \' from the string
//
// Revision 1.15  2006/04/26 17:17:55  timur
// store the history of the state transitions in the database
//
// Revision 1.14  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.13  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.12  2006/03/28 00:20:48  timur
// added srmAbortFiles support
//
// Revision 1.11  2006/03/24 00:22:17  timur
// regenerated stubs with array wrappers for v2_1
//
// Revision 1.10  2006/03/18 00:41:03  timur
// srm v2 bug fixes
//
// Revision 1.9  2006/03/14 17:44:19  timur
// moving toward the axis 1_3
//
// Revision 1.8  2005/12/02 22:20:51  timur
// working on srmReleaseFiles
//
// Revision 1.7  2005/11/20 02:40:11  timur
// SRM PrepareToGet and srmStatusOfPrepareToGet functions
//
// Revision 1.6  2005/11/17 20:45:55  timur
// started work on srmPrepareToGet functions
//
// Revision 1.5  2005/05/04 21:54:52  timur
// new scheduling policy on restart for put and get request - do not schedule the request if the user does not update its status
//
// Revision 1.4  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.3  2005/03/11 21:16:26  timur
// making srm compatible with cern tools again
//
// Revision 1.2  2005/03/01 23:10:38  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.8  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.7  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.6  2004/10/30 04:19:07  timur
// Fixed a problem related to the restoration of the job from database
//
// Revision 1.5  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.4  2004/08/17 16:01:14  timur
// simplifying scheduler, removing some bugs, and redusing the amount of logs
//
// Revision 1.3  2004/08/10 17:03:47  timur
// more monitoring, change file request state, when request is complete
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.14  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.13  2004/07/12 21:52:06  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.1.2.12  2004/07/02 20:10:24  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.11  2004/06/30 20:37:24  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.10  2004/06/24 23:03:07  timur
// put requests, put file requests and copy file requests are now stored in database, copy requests need more work
//
// Revision 1.1.2.9  2004/06/23 21:56:01  timur
// Get Requests are now stored in database, ContainerRequest Credentials are now stored in database too
//
// Revision 1.1.2.8  2004/06/22 01:38:06  timur
// working on the database part, created persistent storage for getFileRequests, for the next requestId
//
// Revision 1.1.2.7  2004/06/16 22:14:31  timur
// copy works for mulfile request
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
 * GetRequestHandler.java
 *
 * Created on July 15, 2003, 1:59 PM
 */


package org.dcache.srm.request;

import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.SRMUser;
import java.util.HashSet;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;

import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;

/*
 * @author  timur
 */
public class GetRequest extends ContainerRequest {
    /** array of protocols supported by client or server (copy) */
    protected String[] protocols;
    
    
    public GetRequest(SRMUser user,
    Long requestCredentialId,
    JobStorage jobStorage,
    String[] surls,
    String[] protocols,
    Configuration configuration,
    long lifetime,
    JobStorage jobFileRequestStorage,
    long max_update_period,
    int max_number_of_retries,
    String description,
    String client_host
    ) throws Exception {
        super(user,
                requestCredentialId,
                jobStorage,
                configuration, 
                max_number_of_retries,
                max_update_period,
                lifetime,
                description,
                client_host);
        say("constructor");
        say("user = "+user);
        say("requestCredetialId="+requestCredentialId);
        int len = protocols.length;
        this.protocols = new String[len];
        System.arraycopy(protocols,0,this.protocols,0,len);
        len = surls.length;
        fileRequests = new FileRequest[len];
        for(int i = 0; i<len; ++i) {
            GetFileRequest fileRequest =
            new GetFileRequest(getId(),requestCredentialId,
            configuration, surls[i],  lifetime, jobFileRequestStorage  , storage,max_number_of_retries);
            
            fileRequests[i] = fileRequest;
        }
        storeInSharedMemory();
    }
    
    /**
     * restore constructor
     */
    public  GetRequest(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    SRMUser user,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    FileRequest[] fileRequests,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    Configuration configuration,
    String[] protocols
    )  throws java.sql.SQLException {
        super( id,
        nextJobId,
        jobStorage,
        creationTime,
        lifetime,
        stateId,
        errorMessage,
        user,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,
        maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray,
        credentialId,
        fileRequests,
        retryDeltaTime, 
        should_updateretryDeltaTime,
        description,
        client_host,
        statusCodeString,
        configuration );
        this.protocols = protocols;
        
    }
    
      public FileRequest getFileRequestBySurl(String surl) throws java.sql.SQLException, SRMException{
        if(surl == null) {
           throw new SRMException("surl is null");
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(((GetFileRequest)fileRequests[i]).getSurlString().equals(surl)) {
                return fileRequests[i];
            }
        }
        throw new SRMException("file request for surl ="+surl +" is not found");
    }
  
    @Override
    public void schedule() throws InterruptedException,
    IllegalStateTransition {

        // save this request in request storage unconditionally
        // file requests will get stored as soon as they are
        // scheduled, and the saved state needs to be consistent
        saveJob(true);
        
        for(int i = 0; i < fileRequests.length ;++ i) {
            GetFileRequest fileRequest = (GetFileRequest) fileRequests[i];
            fileRequest.schedule();
        }
    }

    // ContainerRequest request;
    /**
     * for each file request in the request
     * call storage.PrepareToGet() which should do all the work
     */
    public int getNumOfFileRequest() {
        if(fileRequests == null) {
            return 0;
        }
        return fileRequests.length;
    }
    
    
    
    public void proccessRequest() {
        say("proccessing get request");
        String supported_protocols[];
        try {
            supported_protocols = storage.supportedGetProtocols();
        }
        catch(org.dcache.srm.SRMException srme) {
            esay(" protocols are not supported");
            esay(srme);
            //setFailedStatus ("protocols are not supported");
            return;
        }
        java.util.HashSet supported_protocols_set = new HashSet(java.util.Arrays.asList(supported_protocols));
        supported_protocols_set.retainAll(java.util.Arrays.asList(protocols));
        if(supported_protocols_set.isEmpty()) {
            esay(" protocols are not supported");
            //setFailedStatus ("protocols are not supported");
            return;
        }
        //do not need it, let it be garbagecollected
        supported_protocols_set = null;
        
    }
    
    public HashSet callbacks_set =  new HashSet();
    
    private static final long serialVersionUID = -3739166738239918248L;
    
    /**
     * storage.PrepareToGet() is given this callbacks
     * implementation
     * it will call the method of GetCallbacks to indicate
     * progress
     */
    
    public String getMethod() {
        return "Get";
    }
    
    //we do not want to stop handler if the
    //the request is ready (all file reqs are ready), since the actual transfer migth
    // happen any time after that
    // the handler, by staing in running state will prevent other queued
    // req from being executed
    public boolean shouldStopHandlerIfReady() {
        return false;
    }
    
    public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure, org.dcache.srm.scheduler.FatalJobFailure {
    }
    
    protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
        State state = getState();
        if(State.isFinalState(state)) {
            say("get request state changed to "+state);
            for(int i = 0 ; i < fileRequests.length; ++i) {
                try {
                    FileRequest fr = fileRequests[i];
                    State fr_state = fr.getState();
                    if(!State.isFinalState(fr_state ))
                    {

                        say("changing fr#"+fileRequests[i].getId()+" to "+state);
                            fr.setState(state,"changing file state becase requests state changed");
                    }
                }
                catch(IllegalStateTransition ist) {
                    esay(ist);
                }
            }
           
        }
            
    }
    
    public String[] getProtocols() {
        String[] copy = new String[protocols.length];
        System.arraycopy(protocols, 0, copy, 0, protocols.length);
        return copy;
    }
        
    public synchronized final SrmPrepareToGetResponse getSrmPrepareToGetResponse()  
    throws SRMException ,java.sql.SQLException {
        //say("getRequestStatus()");
        SrmPrepareToGetResponse response = new SrmPrepareToGetResponse();
        // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the 
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());

        ArrayOfTGetRequestFileStatus arrayOfTGetRequestFileStatus =
            new ArrayOfTGetRequestFileStatus();
        arrayOfTGetRequestFileStatus.setStatusArray(getArrayOfTGetRequestFileStatus(null));
        response.setArrayOfFileStatuses(arrayOfTGetRequestFileStatus);
        return response;
    }
    
    public synchronized final SrmStatusOfGetRequestResponse getSrmStatusOfGetRequestResponse()  
    throws SRMException, java.sql.SQLException {
        //say("getRequestStatus()");
        return getSrmStatusOfGetRequestResponse(null);
    }
    
    public synchronized final SrmStatusOfGetRequestResponse getSrmStatusOfGetRequestResponse(
            String[] surls)  
    throws SRMException, java.sql.SQLException {
        //say("getRequestStatus()");
        SrmStatusOfGetRequestResponse response = new SrmStatusOfGetRequestResponse();
        // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the 
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        ArrayOfTGetRequestFileStatus arrayOfTGetRequestFileStatus =
            new ArrayOfTGetRequestFileStatus();
        arrayOfTGetRequestFileStatus.setStatusArray(
            getArrayOfTGetRequestFileStatus(surls));
        response.setArrayOfFileStatuses(arrayOfTGetRequestFileStatus);
        String s ="getSrmStatusOfGetRequestResponse:";
        s+= " StatusCode = "+response.getReturnStatus().getStatusCode().toString();
        for(TGetRequestFileStatus fs :arrayOfTGetRequestFileStatus.getStatusArray()) {
            s += " FileStatusCode = "+fs.getStatus().getStatusCode();
        }
        say(s);
        return response;
    }
    
   
    private String getTRequestToken() {
        return getId().toString();
    }
    
   /* private ArrayOfTGetRequestFileStatus getArrayOfTGetRequestFileStatus()throws SRMException,java.sql.SQLException {
        return getArrayOfTGetRequestFileStatus(null);
    }
    */
    private TGetRequestFileStatus[] getArrayOfTGetRequestFileStatus(String[] surls) throws SRMException,java.sql.SQLException {
        int len = surls == null ? getNumOfFileRequest():surls.length;
        TGetRequestFileStatus[] getFileStatuses
            = new TGetRequestFileStatus[len];
        if(surls == null) {
            for(int i = 0; i< len; ++i) {
                //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                GetFileRequest fr =(GetFileRequest)fileRequests[i];
                //say("getRequestStatus() received FileRequest frs");
                getFileStatuses[i] = fr.getTGetRequestFileStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                GetFileRequest fr =(GetFileRequest)getFileRequestBySurl(surls[i]);
                //say("getRequestStatus() received FileRequest frs");
                getFileStatuses[i] = fr.getTGetRequestFileStatus();
            }
            
        }
        return getFileStatuses;
    }
    /*public TSURLReturnStatus[] getArrayOfTSURLReturnStatus()
    throws SRMException,java.sql.SQLException {
        return null;
    }
     */
    public TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) throws SRMException,java.sql.SQLException {
        int len ;
        TSURLReturnStatus[] surlLReturnStatuses;
        if(surls == null) {
            len = getNumOfFileRequest();
           surlLReturnStatuses = new TSURLReturnStatus[len];
        }
        else {
            len = surls.length;
           surlLReturnStatuses = new TSURLReturnStatus[surls.length];
        }
        if(surls == null) {
            for(int i = 0; i< len; ++i) {
                //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                GetFileRequest fr =(GetFileRequest)fileRequests[i];
                //say("getRequestStatus() received FileRequest frs");
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
                GetFileRequest fr =(GetFileRequest)getFileRequestBySurl(surls[i]);
                //say("getRequestStatus() received FileRequest frs");
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }
            
        }
        return surlLReturnStatuses;
    }

    public TRequestType getRequestType() {
        return TRequestType.PREPARE_TO_GET;
    }

}
