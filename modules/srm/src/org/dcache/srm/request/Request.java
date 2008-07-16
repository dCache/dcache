// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.26  2007/02/20 01:37:56  timur
// more changes to report status according to the spec and make ls report lifetime as -1 (infinite)
//
// Revision 1.25  2007/02/17 05:44:25  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace, refactored database code a bit
//
// Revision 1.24  2007/01/10 23:00:24  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.23  2007/01/06 00:23:55  timur
// merging production branch changes to database layer to improve performance and reduce number of updates
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
 * Created on September 29, 2006, 3:12 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.dcache.srm.request;

import java.sql.SQLException;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.request.sql.RequestsPropertyStorage;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.SRMUser;

/**
 *
 * @author timur
 */
public abstract class Request extends Job {
    private String client_host;
    private SRMUser user;
    public Request(SRMUser user,
    Long requestCredentalId,
    JobStorage requestJobsStorage,
    Configuration configuration,
    int max_number_of_retries,
    long max_update_period,
    long lifetime,
    String description,
    String client_host
        ) throws Exception{
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
        super(lifetime,requestJobsStorage,max_number_of_retries,
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
        this.description = description;
        this.client_host = client_host;
        this.user = user;
    }
    
   /**
     * this constructor is used for restoring the previously
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
    SRMUser user,
    String scheduelerId,
    long schedulerTimeStamp,
    int numberOfRetries,
    int maxNumberOfRetries,
    long lastStateTransitionTime,
    JobHistory[] jobHistoryArray,
    Long credentialId,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    Configuration configuration
    ) {
        super(id,
        nextJobId,
        jobStorage, 
        creationTime,  
        lifetime, 
        stateId, 
        errorMessage, 
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
        this.retryDeltaTime = retryDeltaTime;
        this.should_updateretryDeltaTime = should_updateretryDeltaTime;
        this.description = description;
        this.client_host = client_host;
        this.statusCode = statusCodeString==null
                ?null
                :TStatusCode.fromString(statusCodeString);
        this.user = user;
        say("restored");
    }
    
    
    /** general srm server configuration settings */
    protected Configuration configuration;

    
    
    
    /*
     * public static (class) variables
     */
    protected Long credentialId;


    
    /**
     * this is called every time user gets RequestStatus
     * so the next time user waits longer (up to MAX_RETRY_TIME secs)
     * if nothing has been happening for a while
     * The algoritm of incrising retryDeltaTime is absolutely arbitrary
     */
    protected int i = 0;

    protected long max_update_period = 10*60*60;

    
    
    
    /*
     * public constructors
     */
    protected static RequestsPropertyStorage requestsproperties = null;

    
    /*
     * private instance variables
     */
    protected int retryDeltaTime = 1;

    protected boolean should_updateretryDeltaTime = true;

    /** instance of the AbstractStorageElement implementation,
     * class that gives us an interface to the
     * underlying storage system
     */
    protected AbstractStorageElement storage;
    /**
     * Status code from version 2.2
     * provides a better description of 
     * reasons for failure, etc
     * need this to comply with the spec
     */
    private TStatusCode statusCode;
    
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

    public abstract String getMethod();
    
    private String description;
    
    public void addDebugHistoryEvent(String description) {
        if(configuration.isJdbcMonitoringDebugLevel()) {
            addHistoryEvent( description);
        }
    }

    
    public static Request getRequest(Long requestId)  {
        Job job = Job.getJob( requestId);
        if (job == null || !(job instanceof Request)) {
            return null;
        }
        return (Request) job;
    }

    
    
    public static Request getRequest(int requestNum)  {
        return (Request) Job.getJob(new Long((long) requestNum));
    }

    
    /**
     * gets request id as int
     * @return
     * request id
     */
    public int getRequestNum() {
        return (int) (getId().longValue());
    }

    
    public SRMUser getSRMUser() {
        return user;
    }

   
    /**
     * Getter for property retryDeltaTime.
     * @return Value of property retryDeltaTime.
     */
    public int getRetryDeltaTime() {
        return retryDeltaTime;
    }

    
    /**
     * gets srm user who issued the request
     * @return
     * srm user
     */
    public SRMUser getUser() {
    return user;
    }

    
    /**
     * Getter for property should_updateretryDeltaTime.
     * @return Value of property should_updateretryDeltaTime.
     */
    public boolean isShould_updateretryDeltaTime() {
        return should_updateretryDeltaTime;
    }

    
    /**
     * reset retryDeltaTime to 1
     */
    public void resetRetryDeltaTime() {
        retryDeltaTime = 1;
    }

    public abstract void schedule(Scheduler scheduler) throws InterruptedException, IllegalStateTransition, SQLException;

    
    /**
     * status is not going to change
     * set retry delta time to 1
     */
    public void stopUpdating() {
        retryDeltaTime = 1;
        should_updateretryDeltaTime = false;
    }

    
    protected void updateRetryDeltaTime() {
        if (should_updateretryDeltaTime && i == 0) {
            
            if(retryDeltaTime <100) {
                retryDeltaTime +=3;
            }
            else if(retryDeltaTime <300) {
                retryDeltaTime +=6;
            }
            else {
                retryDeltaTime *= 2;
            }
            if (retryDeltaTime > max_update_period) {
                retryDeltaTime = (int) max_update_period;
            }
        }
        i = (i+1)%5;
    }

    public final String getDescription() {
        return description;
    }

    public final void setDescription(String description) {
        this.description = description;
    }

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

    public String getClient_host() {
        return client_host;
    }
    
    public String getSubmitterId() {
         return Long.toString(user.getId());
    }

    
}
