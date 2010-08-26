// $Id$
// $Log: ReserveSpaceRequest.java,v $
// Revision 1.13  2007/10/19 20:57:04  tdh
// Merge of caching of gPlazma authorization in srm.
//
// Revision 1.12  2007/10/08 18:29:11  timur
// Fix to the Flavia issue:  Space reservation with retentionpolicy=REPLICA and unspecified accesslatency resolves in reservation of CUSTODIAL-NEARLINE type of space when the access latency optional parameter is not specified. It happens both at Edinburgh and NDGF.The problem was that if either retention policy or access latency were not specified, we used default values for both.
//
// Revision 1.11  2007/08/22 20:27:34  timur
// space manager understand lifetime=-1 as infinite, get-space-tokens does not check ownership
//
// Revision 1.10  2007/08/03 15:47:58  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.9  2007/03/03 00:43:05  timur
// make srm reserve space and space get metadata return correct values, set status before changing request state, to make it save its value in database
//
// Revision 1.8  2007/02/17 05:44:25  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace, refactored database code a bit
//
// Revision 1.7  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.6  2006/08/23 20:57:26  timur
// set default values for access latency and retention policy
//
// Revision 1.5  2006/08/15 22:06:40  timur
// got the messages to get through to space manager
//
// Revision 1.4  2006/08/07 21:03:59  timur
// implemented srmStatusOfReserveSpaceRequest
//
// Revision 1.3  2006/08/02 22:08:22  timur
// more work for space management
//
// Revision 1.2  2006/08/01 00:09:51  timur
// more space reservation code
//
// Revision 1.1  2006/07/29 18:10:41  timur
// added schedulable requests for execution reserve space requests
//
// Revision 1.9  2006/06/15 20:51:28  moibenko
// changed method names
//
// Revision 1.8  2006/04/26 17:17:55  timur
// store the history of the state transitions in the database
//
// Revision 1.7  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.6  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.5  2006/03/23 22:47:33  moibenko
// included Lambda Station fuctionalitiy
//
// Revision 1.4  2005/03/30 22:42:10  timur
// more database schema changes
//
// Revision 1.3  2005/03/11 21:16:25  timur
// making srm compatible with cern tools again
//
// Revision 1.2  2005/03/01 23:10:38  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.6  2005/01/04 20:09:33  timur
// correct transfer from transferring to done state
//
// Revision 1.5  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.4  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.3  2004/10/28 02:41:30  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.12  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.11  2004/07/09 01:58:40  timur
// fixed a syncronization problem, added auto dirs creation for copy function
//
// Revision 1.1.2.10  2004/07/02 20:10:24  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.9  2004/06/23 21:56:00  timur
// Get Requests are now stored in database, Request Credentials are now stored in database too
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

import java.net.InetAddress;
import java.net.MalformedURLException;

import diskCacheV111.srm.RequestFileStatus;
import diskCacheV111.srm.FileMetaData;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.globus.util.GlobusURL;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TRetentionPolicyInfo;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.SrmReserveSpaceResponse;
import org.dcache.srm.v2_2.SrmStatusOfReserveSpaceRequestResponse;
import org.apache.axis.types.UnsignedLong;
import org.dcache.srm.SRMInvalidRequestException;
import org.apache.log4j.Logger;
/**
 * File request is an abstract "SRM file request"
 * its concrete subclasses are GetFileRequest,PutFileRequest and CopyFileRequest
 * File request is one in a set of file requests within a request
 * each Request is identified by its requestId
 * and each file request is identified by its fileRequestId within Request
 * File Request contains  a reference to its Request
 *
 *
 * @author  timur
 * @version
 */
public class ReserveSpaceRequest extends Request {
    private static final Logger logger =
            Logger.getLogger (ReserveSpaceRequest.class);
    
    private long sizeInBytes ;
    private TRetentionPolicy retentionPolicy =null;
    private TAccessLatency accessLatency = null;
    private String spaceToken;
    private long spaceReservationLifetime;
    
    
    /** Creates new ReserveSpaceRequest */
    public ReserveSpaceRequest(
            Long  requestCredentalId,
            SRMUser user,
            Configuration configuration,
            long lifetime,
            JobStorage jobStorage,
            int maxNumberOfRetries,
            long sizeInBytes ,
            long spaceReservationLifetime,
            TRetentionPolicy retentionPolicy,
            TAccessLatency accessLatency,
            String description,
            String clienthost) throws Exception {
        /*Request(String userId,
    Long requestCredentalId,
    JobStorage requestJobsStorage,
    Configuration configuration,
    int max_number_of_retries,
    long max_update_period,
    long lifetime,
    String description)*/
              super(user,
              requestCredentalId,
              jobStorage,
              configuration,
              maxNumberOfRetries,
              0,
              lifetime,
              description,
              clienthost);
        
        this.sizeInBytes = sizeInBytes ;
        if(retentionPolicy != null ) {
            this.retentionPolicy = retentionPolicy;
        }
        
        if( accessLatency != null) {
            this.accessLatency = accessLatency;
        }
        
        this.spaceReservationLifetime = spaceReservationLifetime;
        storeInSharedMemory();
        say("created");
        
    }
    
    /** this constructor is used for restoring the previously
     * saved FileRequest from persitance storage
     */
    
    
    public ReserveSpaceRequest(
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
            Long  requestCredentalId,
            long sizeInBytes,
            long spaceReservationLifetime,
            String spaceToken,
            String retentionPolicy,
            String accessLatency,
            String description,
            String clienthost,
            String statusCodeString,
            Configuration configuration
            ) {
                /*
                 *protected Request(
                    Long credentialId,
                    int retryDeltaTime,
                    boolean should_updateretryDeltaTime,
                    String description,
                    Configuration configuration
                    )
                 */
                super(id,
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
                jobHistoryArray,//VVV
                requestCredentalId,
                0,
                false,
                description,
                clienthost,
                statusCodeString,
                configuration);
        this.sizeInBytes = sizeInBytes;
        this.spaceToken = spaceToken;
        
        this.retentionPolicy = retentionPolicy == null?null: TRetentionPolicy.fromString(retentionPolicy);
        this.accessLatency = accessLatency == null ?null :TAccessLatency.fromString(accessLatency);
        this.spaceReservationLifetime = spaceReservationLifetime;
        
        say("restored");
    }
    
    
    public void say(String s) {
        storage.log("ReserveSpaceRequest id # "+getId()+" :"+s);
    }
    
    public void esay(String s) {
        storage.elog("ReserveSpaceRequest id #"+getId()+" :"+s);
    }
    
    public RequestCredential getCredential() {
        return RequestCredential.getRequestCredential(credentialId);
    }
    
    /**
     * Getter for property credentialId.
     * @return Value of property credentialId.
     */
    public Long getCredentialId() {
        return credentialId;
    }
    
    public void esay(Throwable t) {
        storage.elog("ReserveSpaceRequest id #"+getId()+" Throwable:"+t);
        storage.elog(t);
    }
    
    
    public String toString() {
        return toString(false);
    }
    
    public String toString(boolean longformat) {
        StringBuffer sb = new StringBuffer();
        sb.append(" ReserveSpaceRequest ");
        sb.append(" id =").append(getId());
        sb.append(" state=").append(getState());
        if(longformat) {
            sb.append('\n').append("History of State Transitions: \n");
            sb.append(getHistory());
        }
        return sb.toString();
    }
    
    
    protected void stateChanged(State oldState) {
    }
    
    
    public void run() throws NonFatalJobFailure, FatalJobFailure {
        try{
            SrmReserveSpaceCallbacks callbacks = new SrmReserveSpaceCallbacks(this.getId());
            storage.srmReserveSpace(
                    getUser(),
                    sizeInBytes,
                    spaceReservationLifetime,
                    retentionPolicy == null ? null:retentionPolicy.getValue(),
                    accessLatency == null ? null:accessLatency.getValue(),
                    getDescription(),
                    callbacks
                    );
            synchronized(this) {
                
                State state = getState();
                if(!State.isFinalState(state)) {
                    setState(State.ASYNCWAIT,
                            "waiting Space Reservation completion");
                }
            }
        } catch(Exception e) {
            if(e instanceof NonFatalJobFailure ) {
                throw (NonFatalJobFailure) e;
            }
            if(e instanceof FatalJobFailure ) {
                throw (FatalJobFailure) e;
            }
            
            esay("can not reserve space: ");
            esay(e);
            try {
                synchronized(this) {
                    
                    State state = getState();
                    if(!State.isFinalState(state)) {
                        setState(State.FAILED,e.toString());
                    }
                }
            } catch(IllegalStateTransition ist) {
                esay("can not set fail state:"+ist);
            }
        }
        
    }
    
    public SrmStatusOfReserveSpaceRequestResponse getSrmStatusOfReserveSpaceRequestResponse() {
        SrmStatusOfReserveSpaceRequestResponse response = 
                new SrmStatusOfReserveSpaceRequestResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRetentionPolicyInfo(new TRetentionPolicyInfo(retentionPolicy, accessLatency));
        response.setSpaceToken(getSpaceToken());
        response.setSizeOfTotalReservedSpace(new UnsignedLong(sizeInBytes) );
        response.setSizeOfGuaranteedReservedSpace(new UnsignedLong(sizeInBytes));
        response.setLifetimeOfReservedSpace(new Integer((int)(spaceReservationLifetime/1000L)));
        return response;
        
    }
    
    public SrmReserveSpaceResponse getSrmReserveSpaceResponse() {
        SrmReserveSpaceResponse response = new SrmReserveSpaceResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRetentionPolicyInfo(new TRetentionPolicyInfo(retentionPolicy, accessLatency));
        response.setRequestToken(String.valueOf(getId()));
        response.setSpaceToken(getSpaceToken());
        response.setSizeOfTotalReservedSpace(new UnsignedLong(sizeInBytes) );
        response.setSizeOfGuaranteedReservedSpace(new UnsignedLong(sizeInBytes));
        response.setLifetimeOfReservedSpace(new Integer((int)(spaceReservationLifetime/1000L)));
        return response;
    }
    
    public synchronized final TReturnStatus getTReturnStatus()   {
        TReturnStatus status = new TReturnStatus();
        status.setExplanation(getErrorMessage());
        State state = getState();
        if(getStatusCode() != null) {
            status.setStatusCode(getStatusCode());
        }
        else if(state == State.FAILED) {
            status.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        else if(state == State.CANCELED) {
            status.setStatusCode(TStatusCode.SRM_ABORTED);
        }
        else if(state == State.DONE) {
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        }
        else {
            status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        }
        return status;
    }
    
    public static ReserveSpaceRequest getRequest(Long requestId)
            throws SRMInvalidRequestException {
        Job job = Job.getJob( requestId);
        if(job == null || !(job instanceof ReserveSpaceRequest)) {
            return null;
        }
        return (ReserveSpaceRequest) job;
    }

    private class SrmReserveSpaceCallbacks implements org.dcache.srm.SrmReserveSpaceCallbacks {
        Long requestJobId;
        public SrmReserveSpaceCallbacks(Long requestJobId){
            this.requestJobId = requestJobId;
        }
        
        public ReserveSpaceRequest getReserveSpacetRequest()
                throws SRMInvalidRequestException {
            Job job = Job.getJob(requestJobId);
            if(job != null) {
                return (ReserveSpaceRequest) job;
            }
            return null;
        }
        
        public void ReserveSpaceFailed(String reason) {

            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
                return;
            }
            try {
                synchronized(request) {
                    
                    State state = request.getState();
                    if(!State.isFinalState(state)) {
                        request.setState(State.FAILED,reason);
                    }
                }
            } catch(IllegalStateTransition ist) {
                request.esay("can not fail state:"+ist);
            }
            
            request.esay("ReserveSpace error: "+ reason);
        }
        
        public void NoFreeSpace(String  reason) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
                return;
            }
            
            try {
                synchronized(request) {
                    
                    State state = request.getState();
                    if(!State.isFinalState(state)) {
                        request.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
                        request.setState(State.FAILED,reason);
                    }
                }
            } catch(IllegalStateTransition ist) {
                request.esay("can not fail state:"+ist);
            }
            
            request.esay("ReserveSpace failed (NoFreeSpace), no free space : "+reason);
        }
 
        public void ReserveSpaceFailed(Exception e) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
                return;
            }
            
            try {
                synchronized(request) {
                    
                    State state = request.getState();
                    if(!State.isFinalState(state)) {
                        request.setState(State.FAILED,e.toString());
                    }
                }
            } catch(IllegalStateTransition ist) {
                request.esay("can not fail state:"+ist);
            }
            
            request.esay("ReserveSpace exception: ");
            request.esay(e);
        }
        
        public void SpaceReserved(String spaceReservationToken, long reservedSpaceSize) {
            ReserveSpaceRequest request;
            try {
                  request  = getReserveSpacetRequest();
            } catch (SRMInvalidRequestException ire) {
                logger.error(ire);
                return;
            }
            try {
                synchronized(request) {
                    
                    State state = request.getState();
                    if(!State.isFinalState(state)) {
                        
                        request.setSpaceToken(spaceReservationToken);
                        request.setSizeInBytes(reservedSpaceSize);
                        request.setState(State.DONE,"space reservation succeeded" );
                    }
                }
            } catch(IllegalStateTransition ist) {
                request.esay(ist);
            }
        }
        
    }
    
    public long getSizeInBytes() {
        return sizeInBytes;
    }
    
    public void setSizeInBytes(long sizeInBytes) {
        this.sizeInBytes = sizeInBytes;
    }
    
    public TRetentionPolicy getRetentionPolicy() {
        return retentionPolicy;
    }
    
    public void setRetentionPolicy(TRetentionPolicy retentionPolicy) {
        this.retentionPolicy = retentionPolicy;
    }
    
    public TAccessLatency getAccessLatency() {
        return accessLatency;
    }
    
    public void setAccessLatency(TAccessLatency accessLatency) {
        this.accessLatency = accessLatency;
    }
       
    public String getSpaceToken() {
        return spaceToken;
    }
    
    public void setSpaceToken(String spaceToken) {
        this.spaceToken = spaceToken;
    }
    
    public long getSpaceReservationLifetime() {
        return spaceReservationLifetime;
    }
    
    public void setSpaceReservationLifetime(long spaceReservationLifetime) {
        this.spaceReservationLifetime = spaceReservationLifetime;
    }
    
    public String getMethod(){
        return "srmReserveSpace";
    }
}
