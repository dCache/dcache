// $Id: Request.java,v 1.18.2.4 2007-10-11 23:10:28 timur Exp $
// $Log: not supported by cvs2svn $
// Revision 1.18.2.3  2007/06/21 23:45:28  timur
// remove 2 unused instructions in error message calculation
//
// Revision 1.18.2.2  2007/01/04 02:58:54  timur
// changes to database layer to improve performance and reduce number of updates
//
// Revision 1.18.2.1  2006/09/11 22:25:55  timur
// reduce number of sql communications
//
// Revision 1.18  2006/07/10 22:03:28  timur
// updated some of the error codes
//
// Revision 1.17  2006/06/22 20:13:47  timur
// removed bug preventing returning of the error message to the client
//
// Revision 1.16  2006/06/20 15:42:17  timur
// initial v2.2 commit, code is based on a week old wsdl, will update the wsdl and code next
//
// Revision 1.15  2006/06/12 22:04:06  timur
// made fts happier by  returning empty error message for non-failed requests
//
// Revision 1.13  2006/04/26 17:17:56  timur
// store the history of the state transitions in the database
//
// Revision 1.12  2006/04/18 00:53:47  timur
// added the job execution history storage for better diagnostics and profiling
//
// Revision 1.11  2006/04/12 23:16:23  timur
// storing state transition time in database, storing transferId for copy requests in database, renaming tables if schema changes without asking
//
// Revision 1.10  2006/03/28 00:20:48  timur
// added srmAbortFiles support
//
// Revision 1.9  2006/03/14 17:44:19  timur
// moving toward the axis 1_3
//
// Revision 1.8  2005/12/09 00:26:09  timur
// srmPrepareToGet works
//
// Revision 1.7  2005/12/02 22:20:51  timur
// working on srmReleaseFiles
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
// Revision 1.2  2005/03/01 23:10:39  timur
// Modified the database scema to increase database operations performance and to account for reserved space"and to account for reserved space
//
// Revision 1.1  2005/01/14 23:07:14  timur
// moving general srm code in a separate repository
//
// Revision 1.10  2005/01/03 20:46:51  timur
// use function
//
// Revision 1.9  2004/11/15 17:00:40  timur
// removed the debug thread stack printing
//
// Revision 1.8  2004/11/09 08:04:47  tigran
// added SerialVersion ID
//
// Revision 1.7  2004/11/08 23:02:41  timur
// remote gridftp manager kills the mover when the mover thread is killed,  further modified the srm database handling
//
// Revision 1.6  2004/10/28 02:41:31  timur
// changed the database scema a little bit, fixed various synchronization bugs in the scheduler, added interactive shell to the File System srm
//
// Revision 1.5  2004/08/20 20:07:53  timur
// set file status to Failed if all requests are done or failed
//
// Revision 1.4  2004/08/17 16:01:14  timur
// simplifying scheduler, removing some bugs, and redusing the amount of logs
//
// Revision 1.3  2004/08/10 22:17:16  timur
// added indeces creation for state field, update postgres driver
//
// Revision 1.2  2004/08/06 19:35:24  timur
// merging branch srm-branch-12_May_2004 into the trunk
//
// Revision 1.1.2.16  2004/08/03 16:37:51  timur
// removing unneeded dependancies on dcache
//
// Revision 1.1.2.15  2004/07/29 22:17:29  timur
// Some functionality for disk srm is working
//
// Revision 1.1.2.14  2004/07/14 18:58:51  timur
// get/put requests status' are set to done when all file requests are done
//
// Revision 1.1.2.13  2004/07/12 21:52:06  timur
// remote srm error handling is improved, minor issues fixed
//
// Revision 1.1.2.12  2004/07/09 01:58:40  timur
// fixed a syncronization problem, added auto dirs creation for copy function
//
// Revision 1.1.2.11  2004/07/02 20:10:24  timur
// fixed the leak of sql connections, added propogation of srm errors
//
// Revision 1.1.2.10  2004/06/30 20:37:24  timur
// added more monitoring functions, added retries to the srm client part, adapted the srmclientv1 for usage in srmcp
//
// Revision 1.1.2.9  2004/06/23 21:56:01  timur
// Get Requests are now stored in database, Request Credentials are now stored in database too
//
// Revision 1.1.2.8  2004/06/22 01:38:07  timur
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
 * Request.java
 *
 * Created on July 5, 2002, 12:03 PM
 */

package org.dcache.srm.request;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.HashtableJobStorage;
import org.dcache.srm.scheduler.JobCreator;
import org.dcache.srm.scheduler.JobCreatorStorage;
import org.dcache.srm.scheduler.HashtableJobCreatorStorage;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import diskCacheV111.srm.RequestStatus;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.SRMAuthorization;
import org.dcache.srm.SRMUser;
import java.util.WeakHashMap;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Iterator;
import java.util.Observable;
import java.util.Observer;
import java.util.HashSet;
import java.util.Collection;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMException;

/** This abstract class represents an "SRM request"
 * We currently support "get","put", and "copy" requests
 * which are the subclasses of this class
 * Each Requests contains a set (array) of FileRequests
 * each Request is identified by its requestId
 * and each FileRequest within Request is identified by its fileRequestId
 * The actual FileRequest arrays are in subclasses too.
 * @author timur
 * @version 1.0
 *
 */

public abstract class Request extends Job {
    protected FileRequest[] fileRequests;
    
    
    
    /*
     * public static (class) variables
     */
    protected Long credentialId;
    /** general srm server configuration settings */
    protected  Configuration configuration;
    /** instance of the AbstractStorageElement implementation,
     * class that gives us an interface to the
     * underlying storage system
     */
    protected AbstractStorageElement storage;
    
    /*
     * private instance variables
     */
    private int retryDeltaTime = 1;
    private boolean should_updateretryDeltaTime = true;
    private long max_update_period= 10*60*60;
    // dcache  requires that once client created a connection to a dcache door,
    // it uses the same door to make all following dcap transfers
    // therefore we need to synchronize the recept of dcap turls
    private String firstDcapTurl;
    
    
    
    /*
     * public constructors
     */
    private static RequestsPropertyStorage requestsproperties = null;
    /**
     * Create a  new request
     * @param user
     *  srm user
     * @param configuration
     *   srm configuration
     * @param numberOfRetries
     * max number of retries
     */
    public Request(String userId,
    Long requestCredentalId,
    JobStorage requestJobsStorage,
    Configuration configuration,
    int max_number_of_retries,
    long max_update_period,
    long lifetime) throws Exception{
        /*
        if(requestsproperties == null)
        {
            requestsproperties = new RequestsPropertyStorage( configuration.getJdbcUrl(),
        configuration.getJdbcClass(),
        configuration.getJdbcUser(),
        configuration.getJdbcPass(),
        configuration.getNextRequestIdStorageTable()
        );
        }
        */
        super(lifetime,userId,requestJobsStorage,max_number_of_retries,
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
            configuration.getStorage());
        this.credentialId = requestCredentalId;
        this.storage = configuration.getStorage();
        this.configuration = configuration;
        this.max_update_period = max_update_period;
        
    }
    
    
    /** this constructor is used for restoring the previously
     * saved Request from persitance storage
     */
    
    
    protected Request(
    Long id,
    Long nextJobId,
    JobStorage jobStorage,
    long creationTime,
    long lifetime,
    int stateId,
    String errorMessage,
    String creatorId,String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    FileRequest[] fileRequests,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    Configuration configuration
    ) {
        super(id,nextJobId,jobStorage, creationTime,  lifetime, stateId, errorMessage, creatorId,
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
            requestsproperties
        
        );
        this.configuration = configuration;
        this.storage = configuration.getStorage();
        this.credentialId = credentialId;
        this.fileRequests = fileRequests;
        this.retryDeltaTime = retryDeltaTime;
        this.should_updateretryDeltaTime = should_updateretryDeltaTime;
        say("restored");
    }
    
    
    public static Request getRequest(int requestNum)  {
        return (Request)Job.getJob(new Long((long)requestNum));
    }
    
    public static Request getRequest(Long requestId)  {
        Job job = Job.getJob( requestId);
        if(job == null || !(job instanceof Request)) {
            return null;
        }
        return (Request) job;
    }
    
    
    public  FileRequest getFileRequest(int fileRequestId){
        if(fileRequests == null) {
            throw new NullPointerException("fileRequestId is null");
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(fileRequests[i].getId().equals(new Long(fileRequestId))) {
                return fileRequests[i];
            }
        }
        throw new IllegalArgumentException("FileRequest fileRequestId ="+fileRequestId+"does not belong to this Request" );
        
    }
    
    public FileRequest getFileRequest(Long fileRequestId)  throws java.sql.SQLException{
        if(fileRequestId == null) {
            return null;
        }
        for(int i =0; i<fileRequests.length;++i) {
            if(fileRequests[i].getId().equals(fileRequestId)) {
                return fileRequests[i];
            }
        }
        return null;
    }
    
   
    public abstract void schedule(Scheduler scheduler) throws InterruptedException,IllegalStateTransition, java.sql.SQLException;
    
    /*
     * public abstract instance methods
     */
    
    /**
     *  gets a number of file requests  in this request
     * @return
     * a number of file requests
     */
    public abstract int getNumOfFileRequest();
    
    /**
     * get file request by the file request id
     * @param fileRequestNum
     * file req id
     * @return
     * file request
     */
    //public abstract FileRequest getFileRequest(int fileRequestId);
    
    
    
    /**
     * gets first dcap turl
     * <p>
     * in case of dcap protocol transfers all transfers performed
     * by the same client should use the same dcap door,
     * therefore we store the first dcap turl,
     * and set the host and port of the rest dcap turls to
     * host  and port of the first dcap turl
     * @return
     * first dcap turl
     */
    public String getFirstDcapTurl() {
        return firstDcapTurl;
    }
    /**
     * stores the first dcap turl
     * in case of dcap protocol transfers all transfers performed
     * by the same client should use the same dcap door,
     * therefore we store the first dcap turl,
     * and set the host and port of the rest dcap turls to
     * host  and port of the first dcap turl
     * @param s
     * first dcap turl
     */
    public void setFirstDcapTurl(String s) {
        firstDcapTurl = s;
    }
    
    /**
     * gets srm user who issued the request
     * @return
     * srm user
     */
    public RequestUser getUser() {
        RequestUser user =  (RequestUser) this.getCreator();
        if(user != null)
        {
            return user;
        }
        return null;
    }
    
    /**
     * gets request id as int
     * @return
     * request id
     */
    public int getRequestNum() {
        return (int)(getId().longValue());
    }
    
    public abstract String getMethod();
    
    /** 
     * we need this methid to send notifications to the concrete instances of 
     *  Request that the client creating this request is still alive 
     *  and if the request was in the RESTORED state, this will cause the 
     * scheduling of the request
     * 
     */
    
    private void getRequestStatusCalled() {
        scheduleIfRestored();
        int len = getNumOfFileRequest();
        for(int i = 0; i< len; ++i) {
            FileRequest fr =fileRequests[i];
            fr.scheduleIfRestored();
        }
        updateRetryDeltaTime();
    }    
    
    public synchronized final RequestStatus getRequestStatus()  throws java.sql.SQLException {
        getRequestStatusCalled();
        //say("getRequestStatus()");
        RequestStatus rs = new RequestStatus();
        rs.requestId = getRequestNum();
        //say("getRequestStatus() rs.requestId="+rs.requestId );
        rs.errorMessage = getErrorMessage();
        if(rs.errorMessage == null)
        {
            rs.errorMessage="";
        }
        //say("getRequestStatus() rs.errorMessage="+rs.errorMessage );
        int len = getNumOfFileRequest();
        rs.fileStatuses = new RequestFileStatus[len];
        boolean failed_req = false;
        boolean pending_req = false;
        boolean running_req = false;
        boolean ready_req = false;
        boolean done_req = false;
        String fr_error="";
        for(int i = 0; i< len; ++i) {
            //say("getRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
            FileRequest fr =fileRequests[i];
            //say("getRequestStatus() received FileRequest frs");
            RequestFileStatus rfs = fr.getRequestFileStatus();
            //say("getRequestStatus()  calling frs.getRequestFileStatus()");
            rs.fileStatuses[i] = rfs;
            String state = rfs.state;
            //say("getRequestStatus() rs.fileStatuses["+i+"] received with id="+rfs.fileId );
            if(state.equals("Pending")) {
                pending_req = true;
            }
            else if(state.equals("Running")) {
                running_req = true;
            }
            else if(state.equals("Ready")) {
                ready_req = true;
            }
            else if(state.equals("Done")) {
                done_req = true;
            }
            else if(state.equals("Failed")) {
                failed_req = true;
                fr_error += "RequestFileStatus#"+rfs.fileId+" failed with error:[ "+fr.getErrorMessage()+"]\n";
            }
            else {
                esay("File Request state is unknown!!! state  == "+state);
                esay("fr is "+fr);
            }
        }
        
        if(failed_req){
            rs.errorMessage += "\n"+fr_error;
        }
            
        if (pending_req) {
            rs.state = "Pending";
        } else if(failed_req) {
            
            if(!running_req && !ready_req ){
                rs.state = "Failed";
                synchronized(this) {
                    State state = getState();
                    if(!State.isFinalState(state)) {
                        stopUpdating();
                        try
                        { 
                            setState(State.FAILED,
			    rs.errorMessage);
                        }
                        catch(IllegalStateTransition ist)
                        {
                            esay(ist);
                        }
                    }
                }
            }

            
        } else if (running_req || ready_req) {
            rs.state = "Active";
        } else if (done_req) {
           synchronized(this) {
           
            State state = getState();
            if(!State.isFinalState(state)) {
                stopUpdating();
                try
                { 
                    setState(State.DONE,"All files are done");
                }
                catch(IllegalStateTransition ist)
                {
                    esay(ist);
                }
            }
           }
            rs.state = "Done";
        } else {
            esay("request state is unknown or no files in request!!!");
           synchronized(this) {
           
            State state = getState();
            if(!State.isFinalState(state)) {
                stopUpdating();
                try
                { 
                    setState(State.FAILED,"request state is unknown or no files in request!!!");
                }
                catch(IllegalStateTransition ist)
                {
                    esay(ist);
                }
            }
           }
            rs.state = "Failed";
        }
        
        // the following it the hack to make FTS happy
        //FTS expects the errorMessage to be "" if the state is not Failed
        if(!rs.state.equals("Failed")) {
            rs.errorMessage="";
        }
        
        
        rs.type = getMethod();
        //say("getRequestStatus() rs.type = "+rs.type);
        rs.retryDeltaTime = retryDeltaTime;
        rs.submitTime = new java.util.Date(getCreationTime());
        rs.finishTime = new java.util.Date(getCreationTime() +getLifetime() );
        //say("getRequestStatus() calling updateRetryDeltaTime()");

        rs.startTime = new java.util.Date(System.currentTimeMillis()+retryDeltaTime*1000);
        //say("getRequestStatus() returning");
        return rs;
    }
    
    public synchronized final TReturnStatus getTReturnStatus()  throws java.sql.SQLException {
        getRequestStatusCalled();
        //say("getTRequestStatus() " );
        TReturnStatus status = new TReturnStatus();
        
        //say("getTRequestStatus() setExplanation:"+rs.errorMessage );
        int len = getNumOfFileRequest();
        boolean failed_req = false;
        boolean pending_req = false;
        boolean running_req = false;
        boolean ready_req = false;
        boolean done_req = false;
        // in srm 2.2 we do not need this
        // since each file status carries its own error
        //String fr_error="";
        for(int i = 0; i< len; ++i) {
            //say("getTRequestStatus() getFileRequest("+fileRequestsIds[i]+" );");
            FileRequest fr =fileRequests[i];
            //say("getTRequestStatus() received FileRequest frs");
            RequestFileStatus rfs = fr.getRequestFileStatus();
            //say("getTRequestStatus()  calling frs.getRequestFileStatus()");
            String state = rfs.state;
            //say("getTRequestStatus() rs.fileStatuses["+i+"] received with id="+rfs.fileId );
            if(state.equals("Pending")) {
                pending_req = true;
            }
            else if(state.equals("Running")) {
                running_req = true;
            }
            else if(state.equals("Ready")) {
                ready_req = true;
            }
            else if(state.equals("Done")) {
                done_req = true;
            }
            else if(state.equals("Failed")) {
                failed_req = true;
                //fr_error += "RequestFileStatus#"+rfs.fileId+" failed with error:[ "+fr.getErrorMessage()+"]\n";
            }
            else {
                esay("File Request state is unknown!!! state  == "+state);
                esay("fr is "+fr);
            }
        }
        
        if (pending_req) {
            if(!running_req && !ready_req && !done_req && !failed_req) {
                status.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
            } else {
                status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
            }
                
            
        } else if(failed_req) {
            if(!running_req && !ready_req ){
                if(done_req) {
                    status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
                } else {
                    status.setStatusCode(TStatusCode.SRM_FAILURE);
                }
                synchronized(this) {
                    State state = getState();
                    if(!State.isFinalState(state)) {
                        stopUpdating();
                        try
                        { 
                            setState(State.FAILED, 
                            "all file requests are completed, at least some are failed");
                        }
                        catch(IllegalStateTransition ist)
                        {
                            esay(ist);
                        }
                    }
                }
            }
            else {
                status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
            }
            
        } else if (running_req || ready_req) {
            status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
        } else if (done_req) {
           synchronized(this) {
           
            State state = getState();
            if(!State.isFinalState(state)) {
                stopUpdating();
                try
                { 
                    setState(State.DONE,"all files are done");
                }
                catch(IllegalStateTransition ist)
                {
                    esay(ist);
                }
            }
           }
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        } else {
            esay("request state is unknown or no files in request!!!");
           synchronized(this) {
           
            State state = getState();
            if(!State.isFinalState(state)) {
                stopUpdating();
                try
                { 
                    setState(State.FAILED,"request state is unknown or no files in request!!!");
                }
                catch(IllegalStateTransition ist)
                {
                    esay(ist);
                }
            }
           }
            status.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
            
        }
        
        status.setExplanation(getErrorMessage());
        
        return status;
    }
    
    /**
     * reset retryDeltaTime to 1
     */
    public void resetRetryDeltaTime() {
        retryDeltaTime = 1;
    }
    
    /**
     * status is not going to change
     * set retry delta time to 1
     */
    public void stopUpdating() {
        retryDeltaTime = 1;
        should_updateretryDeltaTime = false;
    }
    
    /**
     * this is called every time user gets RequestStatus
     * so the next time user waits longer (up to MAX_RETRY_TIME secs)
     * if nothing has been happening for a while
     * The algoritm of incrising retryDeltaTime is absolutely arbitrary
     */
    private int i =0;
    
    private static final long serialVersionUID = -5497111637295541321L;
    
    private  void updateRetryDeltaTime() {
        if(should_updateretryDeltaTime && i == 0) {
            
            if(retryDeltaTime <100) {
                retryDeltaTime +=3;
            }
            else if(retryDeltaTime <300) {
                retryDeltaTime +=6;
            }
            else {
                retryDeltaTime *= 2;
            }
            if(retryDeltaTime > max_update_period) {
                retryDeltaTime = (int)max_update_period;
            }
        }
        i = (i+1)%5;
    }
    
    
    /**
     * check the object for the equality with this request
     * <p>
     * we return true only if object is this request
     * @param o
     * object to check for equality with
     * @return
     * result of the check
     */
    public boolean equals(Object o) {
        if(o == this) {
            return true;
        }
        
        return false;
    }
    
    
    /**
     * log a message
     * @param words
     * message
     */
    public void say(String words) {
        storage.log("Request id="+getId()+": "+words);
    }
    
    
    /**
     * log an error message
     * @param words
     *error message
     */
    public void esay(String words) {
        storage.elog("Request id="+getId()+": "+words);
    }
    
    /**
     * log an instance of throwable
     * @param t
     * an instance of throwable
     */
    public void esay(Throwable t) {
        storage.elog("Request id="+getId()+" error: ");
        storage.elog(t);
    }
    
    public RequestUser getRequestUser() {
        return (RequestUser) getCreator();
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
    
    /**
     * Getter for property fileRequestsIds.
     * @return Value of property fileRequestsIds.
     */
    /*public java.lang.String[] getFileRequestsIds() {
        String[] copy = new String[fileRequestsIds.length];
        System.arraycopy(fileRequestsIds, 0, copy, 0, fileRequestsIds.length);
        return copy;
    }
     */
    
    
    public String toString() {
        return toString(false);
    }
    
     public String toString(boolean longformat) {
        try {
            String s = getMethod()+"Request #"+getId()+" created by "+getCreatorId()+
            " with credentials : "+getCredential()+
            " state = "+getState();
            
            if(longformat) {
                s += '\n'+ getRequestStatus().toString();
                s += "\n History of State Transitions: \n";
                s += getHistory();
                for(int i = 0; i<fileRequests.length; ++i){
                    s += "\n History of State Transitions for file "+
                    fileRequests[i].getId()+": \n";
                    s += fileRequests[i].getHistory();
                }
                
            }
            return s;
        }catch(Exception e) {
            esay(e);
            return e.toString();
        }
        
    }
   
    /**
     * Getter for property retryDeltaTime.
     * @return Value of property retryDeltaTime.
     */
    public int getRetryDeltaTime() {
        return retryDeltaTime;
    }
    
    /**
     * Getter for property should_updateretryDeltaTime.
     * @return Value of property should_updateretryDeltaTime.
     */
    public boolean isShould_updateretryDeltaTime() {
        return should_updateretryDeltaTime;
    }
    
    public abstract FileRequest getFileRequestBySurl(String surl)  throws java.sql.SQLException, SRMException ;
    public abstract TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) throws SRMException,java.sql.SQLException ;
        
    
}
