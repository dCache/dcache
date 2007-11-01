// $Id: FileRequest.java,v 1.9.2.2 2007-10-11 23:10:27 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.9.2.1  2007/01/04 02:58:54  timur
// changes to database layer to improve performance and reduce number of updates
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
import org.globus.util.GlobusURL;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.JobCreator;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.lambdastation.*;
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
public abstract class FileRequest extends Job {
    //file Request is being processed
    // for get and put it means that file turl
    // is not available yet
    //for copy - file is being copied
    public static final String SFN_STRING="?SFN=";
    
    //request which contains this fileRequest (which is different from request number)
    protected Long requestId;
    protected Long credentialId;
    
    //pointer to underlying storage
    protected AbstractStorageElement storage;
    //srm configuration
    protected Configuration configuration;
    //error message if error, or just information message
    private String errorMsg;
    
    private static RequestsPropertyStorage requestsproperties = null;
    private LambdaStationMap LsMap;
    private LambdaStationTicket LsTicket;

    private static final long serialVersionUID = -5737484917461810463L;
    
    /** Creates new FileRequest */
    protected FileRequest(Long requestId,
    Long  requestCredentalId,
    String userId,
    Configuration configuration,long lifetime,
    JobStorage jobStorage,int maxNumberOfRetries) throws Exception {
        super(lifetime,userId, jobStorage,maxNumberOfRetries,
        requestsproperties == null
        ?
            requestsproperties =
            RequestsPropertyStorage.getPropertyStorage(
            configuration.getJdbcUrl(),
            configuration.getJdbcClass(),
            configuration.getJdbcUser(),
            configuration.getJdbcPass(),
            configuration.getNextRequestIdStorageTable(),
            configuration.getStorage())
        :
            requestsproperties, 
            configuration.getStorage()
        );
        this.credentialId = requestCredentalId;
        this.configuration = configuration;
        this.storage = configuration.getStorage();
        this.requestId = requestId;
        
        say("created");
        
    }
    
    /** this constructor is used for restoring the previously
     * saved FileRequest from persitance storage
     */
    
    
    protected FileRequest(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,long lifetime,
    int stateId,String errorMessage,
    String creatorId,String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long requestId,
    Long  requestCredentalId,
    Configuration configuration
    ) {
        super(id,nextJobId,jobStorage, 
        creationTime,  lifetime, 
        stateId, errorMessage, creatorId,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,maxNumberOfRetries, 
        lastStateTransitionTime, 
        jobHistoryArray, 
        configuration.getStorage(),
        requestsproperties == null
        ?
            requestsproperties =
            RequestsPropertyStorage.getPropertyStorage(
            configuration.getJdbcUrl(),
            configuration.getJdbcClass(),
            configuration.getJdbcUser(),
            configuration.getJdbcPass(),
            configuration.getNextRequestIdStorageTable(),
            configuration.getStorage())
        :
            requestsproperties);
        this.credentialId = requestCredentalId;
        this.configuration = configuration;
        this.storage = configuration.getStorage();
        this.requestId = requestId;
        say("restored");
    }
    
    
    public void say(String s) {
        storage.log("FileRequest reqId # "+requestId+" fileId # "+getId()+" : "+s);
    }
    
    public void esay(String s) {
        /*
        String claass = getClass().getName();
        int idx = claass.lastIndexOf(".");
        idx = idx > 0? idx:0;
        claass = claass.substring(idx);
         
        storage.elog(claass+"reqId # "+request.getRequestId()+" fileId # "+fileRequestId+" : "+s);
         */
        storage.elog("FileRequest reqId #"+requestId+" id #"+getId()+" :"+s);
        
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
        storage.elog("FileRequest reqId #"+requestId+" id #"+getId()+" Throwable:"+t);
        storage.elog(t);
    }
    
    
    public abstract RequestFileStatus getRequestFileStatus() ;
    public String toString() {
     return toString(false);
    }
    
    public String toString(boolean longformat) {
        StringBuffer sb = new StringBuffer();
        sb.append(" FileRequest ");
        sb.append(" id =").append(getId());
        sb.append(" state=").append(getState());
        if(longformat) {
            sb.append('\n').append(getRequestFileStatus());
            sb.append('\n').append("History of State Transitions: \n");
            sb.append(getHistory());
        }
        return sb.toString();
    }
    
    public boolean equals(Object o) {
        if(o == this) {
            return true;
        }
        
        return false;
    }
    
    public void setStatus(String status) throws SRMException, java.sql.SQLException {
        say("("+status+")");
        try {
            synchronized(this)
            {
                if(status.equalsIgnoreCase("Done")) {
                    State state = getState();
                    if( state  != State.FAILED &&
                    state != State.DONE &&
                    state != State.CANCELED) {
                        if(state == State.READY || 
                           state == State.TRANSFERRING ||
                           state == State.RUNNING) {
                            setState(State.DONE,"set by setStatus to \"Done\"");
                        }
                        else {
                            setState(State.CANCELED,"set by setStatus to \"Done\"");
                        }
                    }
                }
                else if(status.equalsIgnoreCase("Running")) {
                    setState(State.TRANSFERRING,"set by setStatus to \"Running\"");
                }
                else {
                    String error =  "Can't set Status to "+status;
                    esay(error);
                    throw new SRMException(error);

                }
            }
        }
        catch(IllegalStateTransition ist) {
            String error =  "Can't set Status to "+status+" reason: "+ist;
            esay(error);
            esay(ist);
            throw new SRMException(error);
        }
        
    }
    
    public RequestUser getUser() {
        RequestUser user =  (RequestUser) this.getCreator();
        if(user != null)
        {
            return user;
        }

        return null;
    }
    
    public Request getRequest()   {
        return Request.getRequest(requestId);
    }
    
    /**
     * Getter for property requestId.
     * @return Value of property requestId.
     */
    public Long getRequestId() {
        return requestId;
    }
    
    public void setLSMap(LambdaStationMap LsMap) {
        this.LsMap = LsMap;
    }

    public LambdaStationMap getLSMap() {
        return LsMap;
    }

    public void setLSTicket(LambdaStationTicket LsTicket) {
        this.LsTicket = LsTicket;
    }

    public LambdaStationTicket getLSTicket() {
        return LsTicket;
    }

    
}
