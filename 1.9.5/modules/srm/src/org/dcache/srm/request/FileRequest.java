// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.18  2007/09/13 19:12:24  timur
// more verbose admin ls -l for request and file
//
// Revision 1.17  2007/08/03 15:47:58  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.16  2007/02/20 01:37:55  timur
// more changes to report status according to the spec and make ls report lifetime as -1 (infinite)
//
// Revision 1.15  2007/02/09 21:24:23  timur
// srmExtendFileLifeTime is about 70% done
//
// Revision 1.14  2007/01/06 00:23:54  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
//
// Revision 1.13  2006/11/13 18:44:15  litvinse
// implemented SetPermission
//
// Revision 1.12  2006/10/16 19:50:15  litvinse
// add casting to calls to ContainerRequest.getRequest() where necessary
//
// Revision 1.11  2006/10/04 21:20:33  timur
// different calculation of the v2.2 status
//
// Revision 1.10  2006/10/02 23:29:16  timur
// implemented srmPing and srmBringOnline (not tested yet), refactored Request.java
//
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
// Get Requests are now stored in database, ContainerRequest Credentials are now stored in database too
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


import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.AbstractStorageElement;
import org.globus.util.GlobusURL;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;


import org.dcache.srm.qos.QOSTicket;
/**
 * File request is an abstract "SRM file request"
 * its concrete subclasses are GetFileRequest,PutFileRequest and CopyFileRequest
 * File request is one in a set of file requests within a request
 * each ContainerRequest is identified by its requestId
 * and each file request is identified by its fileRequestId within ContainerRequest
 * File ContainerRequest contains  a reference to its ContainerRequest
 * 
 * @author timur
 * @version 
 */
public abstract class FileRequest extends Job {
    //file ContainerRequest is being processed
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

    private QOSTicket qosTicket;

    private static final long serialVersionUID = -5737484917461810463L;
    
    /**
     * Status code from version 2.2
     * provides a better description of 
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;
    
   /** Creates new FileRequest */
    protected FileRequest(Long requestId,
    Long  requestCredentalId,
    Configuration configuration,long lifetime,
    JobStorage jobStorage,int maxNumberOfRetries) throws Exception {
        super(lifetime, jobStorage,maxNumberOfRetries);
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
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long requestId,
    Long  requestCredentalId,
    String statusCodeString,
    Configuration configuration
    ) {
        super(id,
        nextJobId,
        jobStorage,
        creationTime,  lifetime,
        stateId, errorMessage,
        scheduelerId,
        schedulerTimeStamp,
        numberOfRetries,maxNumberOfRetries,
        lastStateTransitionTime,
        jobHistoryArray);
        this.credentialId = requestCredentalId;
        this.configuration = configuration;
        this.storage = configuration.getStorage();
        this.requestId = requestId;
        this.statusCode = statusCodeString==null
                ?null
                :TStatusCode.fromString(statusCodeString);
        say("restored");
        
    }
    
    public void addDebugHistoryEvent(String description) {
        if(configuration.isJdbcLogRequestHistoryInDBEnabled()) {
            addHistoryEvent( description);
        }
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
    
    public abstract TReturnStatus getReturnStatus();
    public String toString() {
     return toString(false);
    }
    
    public String toString(boolean longformat) {
        StringBuilder sb = new StringBuilder();
        sb.append(" FileRequest ");
        sb.append(" id =").append(getId());
        sb.append(" job priority  =").append(getPriority());

        sb.append(" crteator priority  =");
        try {
            sb.append(getUser().getPriority());
        } catch (SRMInvalidRequestException ire) {
            sb.append("Unknown");
        }
        sb.append(" state=").append(getState());
        if(longformat) {
            sb.append('\n').append(getRequestFileStatus());
            sb.append('\n').append("status code = ").append(getStatusCode());
            sb.append('\n').append("error message = ").append(getErrorMessage());
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
    
    public int hashCode() {
        return getId().hashCode();
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
    
    public SRMUser getUser() throws SRMInvalidRequestException {
        return getRequest().getUser();
    }

    public Request getRequest() throws SRMInvalidRequestException  {
        return Request.getRequest(requestId);
    }
    
    /**
     * Getter for property requestId.
     * @return Value of property requestId.
     */
    public Long getRequestId() {
        return requestId;
    } 
    
    public void setQOSTicket(QOSTicket qosTicket) {
        this.qosTicket = qosTicket;
    }

    public QOSTicket getQOSTicket() {
        return qosTicket;
    }
    
   /**
     * @param newLifetime  new lifetime in millis
     *  -1 stands for infinite lifetime
     * @return int lifetime left in millis
     *    -1 stands for infinite lifetime
     */

    public abstract long extendLifetime(long newLifetime) throws SRMException ;

    public TStatusCode getStatusCode() {
        return statusCode;
    }

     public String getStatusCodeString() {
        return statusCode==null ? null:statusCode.getValue() ;
    }

    public void setStatusCode(TStatusCode statusCode) {
        this.statusCode = statusCode;
    }
    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode==null?null:TStatusCode.fromString(statusCode);
    }   
    
    public static String getPath(GlobusURL surl) {
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

    /**
     *
     * @return
     */
     public String getSubmitterId() {
         try {
            return Long.toString(getUser().getId());
         } catch (Exception e) {
             //
             // Throwing the exception from this method
             //  will prevent the change of the status of this request
             //  to canceled, done or failed.
             // Therefore we catch the exception and
             // just report the submitter id as unknown
             //
             esay(e);
             return "unknown";
         }
     }

}
