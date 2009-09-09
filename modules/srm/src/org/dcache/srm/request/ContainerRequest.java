// $Id$
// $Log: not supported by cvs2svn $
// Revision 1.19  2007/10/21 18:49:31  litvinse
// fix syntax error
//
// Revision 1.18  2007/10/21 18:43:52  litvinse
// add one more condidtion
//
// Revision 1.17  2007/10/21 18:39:18  litvinse
// continue to work on StatusOfPartialEx
//
// Revision 1.16  2007/10/19 15:32:06  litvinse
// add more debug info and make getTReturnStatus synchroinized
//
// Revision 1.15  2007/10/15 21:30:36  litvinse
// address issue #5 on Flavia's list
//
// Revision 1.14  2007/10/07 04:27:34  litvinse
// found out that on abort I returned SRM_PARTIAL_SUCCESS if some
// files in the list were successfull. Looks like this is against
// spec. Fixed.
//
// Revision 1.13  2007/10/07 03:14:27  litvinse
// found a bug that set running request to SRM_INTERNAL_ERROR :)
//
// Revision 1.12  2007/10/05 15:16:14  litvinse
// return SRM_ABORTED at requets level if aborted
//
// Revision 1.11  2007/09/28 21:42:58  litvinse
// addressed item 8 on Flavia's list
//
// Revision 1.10  2007/09/13 19:12:23  timur
// more verbose admin ls -l for request and file
//
// Revision 1.9  2007/08/03 15:47:58  timur
// closing sql statement, implementing hashCode functions, not passing null args, resing classes, not comparing objects using == or !=,  etc, per findbug recommendations
//
// Revision 1.8  2007/06/18 21:44:57  timur
// better reporting of the expired space reservations
//
// Revision 1.7  2007/04/06 21:35:02  litvinse
// added method getFileRequests returning array of file requests
//
// Revision 1.6  2007/02/23 17:05:25  timur
// changes to comply with the spec and appear green on various tests, mostly propogating the errors as correct SRM Status Codes, filling in correct fields in srm ls, etc
//
// Revision 1.5  2007/02/17 05:44:24  timur
// propagate SRM_NO_FREE_SPACE to reserveSpace, refactored database code a bit
//
// Revision 1.4  2007/01/10 23:00:24  timur
// implemented srmGetRequestTokens, store request description in database, fixed several srmv2 issues
//
// Revision 1.3  2006/12/20 15:38:47  timur
// implemented getRequestSummary
//
// Revision 1.2  2006/10/04 21:20:33  timur
// different calculation of the v2.2 status
//
// Revision 1.1  2006/10/02 23:29:16  timur
// implemented srmPing and srmBringOnline (not tested yet), refactored Request.java
//
// Revision 1.19  2006/09/11 22:28:04  timur
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
// Get Requests are now stored in database, ContainerRequest Credentials are now stored in database too
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
 * ContainerRequest.java
 *
 * Created on July 5, 2002, 12:03 PM
 */

package org.dcache.srm.request;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.RequestStatusTool;
import diskCacheV111.srm.RequestStatus;
import org.dcache.srm.SRMUser;
import diskCacheV111.srm.RequestFileStatus;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMException;

/**
 * This abstract class represents an "SRM request"
 * We currently support "get","put", and "copy" requests
 * which are the subclasses of this class
 * Each Requests contains a set (array) of FileRequests
 * each ContainerRequest is identified by its requestId
 * and each FileRequest within ContainerRequest is identified by its fileRequestId
 * The actual FileRequest arrays are in subclasses too.
 *
 * @author timur
 * @version 1.0
 */

public abstract class ContainerRequest extends Request {
    // dcache  requires that once client created a connection to a dcache door,
    // it uses the same door to make all following dcap transfers
    // therefore we need to synchronize the recept of dcap turls
    private String firstDcapTurl;

     protected FileRequest[] fileRequests;


    /*
     * public constructors
     */
    /**
     * Create a  new request
     * @param user
     *  srm user
     * @param configuration
     *   srm configuration
     * @param numberOfRetries
     * max number of retries
     */
    public ContainerRequest(SRMUser user,
    Long requestCredentalId,
    JobStorage requestJobsStorage,
    Configuration configuration,
    int max_number_of_retries,
    long max_update_period,
    long lifetime,
    String description,
    String client_host) throws Exception{
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
         super(user ,
         requestCredentalId,
         requestJobsStorage,
         configuration,
         max_number_of_retries,
         max_update_period,
         lifetime,
         description,
         client_host);

    }


    /**
     * this constructor is used for restoring the previously
     * saved ContainerRequest from persitance storage
     */


    protected ContainerRequest(
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
    Configuration configuration
    ) {
     super(     id,
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
     retryDeltaTime,
     should_updateretryDeltaTime,
     description,
     client_host,
     statusCodeString,
     configuration
     );
        this.fileRequests = fileRequests;
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

    public abstract String getMethod();

    /**
     *
     * we need this methid to send notifications to the concrete instances of
     *  ContainerRequest that the client creating this request is still alive
     *  and if the request was in the RESTORED state, this will cause the
     * scheduling of the request
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

    public synchronized final RequestStatus getRequestStatus() {
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
            if(rfs == null){
                failed_req = true;
                fr_error += "RequestFileStatus is null : fr.errorMessage= [ "+fr.getErrorMessage()+"]\n";
                continue;
            }
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

    public synchronized TReturnStatus getTReturnStatus()  {
        //
        // We want everthing that getRequestStatus does to happen
        //
        getRequestStatus();

        //say("getTRequestStatus() " );
        TReturnStatus status = new TReturnStatus();

       if(getStatusCode() != null) {
            status.setStatusCode(getStatusCode());
            say("getTReturnStatus() assigned status.statusCode : "+status.getStatusCode());
            status.setExplanation(getErrorMessage());
            say("getTReturnStatus() assigned status.explanation : "+status.getExplanation());
            return status;
       }

        int len = getNumOfFileRequest();

        if (len == 0) {
            //no single failure - we should not get to this piece if code
            status.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
            status.setExplanation("Could not find (deserialize) files in the request," +
                " NumOfFileRequest is 0");
            say("assigned status.statusCode : "+status.getStatusCode());
            say("assigned status.explanation : "+status.getExplanation());
            return status;

        }


        int failed_req           = 0;
        int failed_space_expired = 0;
        int failed_no_free_space = 0;
        int canceled_req         = 0;
        int pending_req          = 0;
        int running_req          = 0;
        int ready_req            = 0;
        int done_req             = 0;
	int got_exception        = 0;
	boolean failure = false;
        for(int i = 0; i< len; ++i) {
            FileRequest fr =fileRequests[i];
            TReturnStatus fileReqRS = fr.getReturnStatus();
            TStatusCode fileReqSC   = fileReqRS.getStatusCode();
            say("getTReturnStatus() file["+i+"] statusCode : "+fileReqSC);
            try{
                if( fileReqSC == TStatusCode.SRM_REQUEST_QUEUED) {
                    pending_req++;
                }
                else if(fileReqSC == TStatusCode.SRM_REQUEST_INPROGRESS) {
                    running_req++;
                }
                else if(fileReqSC == TStatusCode.SRM_FILE_PINNED ||
                        fileReqSC == TStatusCode.SRM_SPACE_AVAILABLE) {
                    ready_req++;
                }
                else if(fileReqSC == TStatusCode.SRM_SUCCESS ||
                        fileReqSC == TStatusCode.SRM_RELEASED) {
                    done_req++;
                }
                else if(fileReqSC == TStatusCode.SRM_ABORTED) {
                     canceled_req++;
		     failure=true;
                }
                else if(fileReqSC == TStatusCode.SRM_NO_FREE_SPACE) {
                    failed_no_free_space++;
		    failure=true;
		}
                else if(fileReqSC == TStatusCode.SRM_SPACE_LIFETIME_EXPIRED) {
                    failed_space_expired++;
		    failure=true;
                }
                else if(RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                    failed_req++;
		    failure=true;
                }
                else {
                    esay("File Request StatusCode is unknown!!! state  == "+fr.getState());
                    esay("fr is "+fr);
                }
            }catch (Exception e) {
                esay(e);
                got_exception++;
		failure=true;
            }
        }

        status.setExplanation(getErrorMessage());

	if (canceled_req == len ) {
	    status.setStatusCode(TStatusCode.SRM_ABORTED);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}

	if (failed_req==len || got_exception==len) {
	    status.setStatusCode(TStatusCode.SRM_FAILURE);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}
	if (ready_req==len || done_req==len || ready_req+done_req==len ) {
	    if (failure) {
		status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
		say("assigned status.statusCode : "+status.getStatusCode());
		say("assigned status.explanation : "+status.getExplanation());
		return status;
	    }
	    else {
		status.setStatusCode(TStatusCode.SRM_SUCCESS);
		say("assigned status.statusCode : "+status.getStatusCode());
		say("assigned status.explanation : "+status.getExplanation());
		return status;
	    }
	}
	if (pending_req==len) {
	    status.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}
	// SRM space is not enough to hold all requested SURLs for free. (so me thinks one fails - all fail)
	if (failed_no_free_space>0) {
	    status.setStatusCode(TStatusCode.SRM_NO_FREE_SPACE);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}
	// space associated with the targetSpaceToken is expired. (so me thinks one fails - all fail)
	if (failed_space_expired>0) {
	    status.setStatusCode(TStatusCode.SRM_SPACE_LIFETIME_EXPIRED);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}
	// we still have work to do:
	if (running_req > 0 || pending_req > 0) {
	    status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
	    say("assigned status.statusCode : "+status.getStatusCode());
	    say("assigned status.explanation : "+status.getExplanation());
	    return status;
	}
	else {
	    // all are done here
	    if (failure) {
		if (ready_req > 0 || done_req > 0 ) {
		    //some succeeded some not
		    status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
		    say("assigned status.statusCode : "+status.getStatusCode());
		    say("assigned status.explanation : "+status.getExplanation());
		    return status;
		}
		else {
		    //none succeeded
		    status.setStatusCode(TStatusCode.SRM_FAILURE);
		    say("assigned status.statusCode : "+status.getStatusCode());
		    say("assigned status.explanation : "+status.getExplanation());
		    return status;
		}
	    }
	    else {
		    //no single failure - we should not get to this piece if code
		    status.setStatusCode(TStatusCode.SRM_SUCCESS);
		    say("assigned status.statusCode : "+status.getStatusCode());
		    say("assigned status.explanation : "+status.getExplanation());
		    return status;
	    }
	}
    }
	

    public TRequestSummary getRequestSummary() {
        TRequestSummary summary = new TRequestSummary();
        summary.setStatus(getTReturnStatus());
        summary.setRequestType(getRequestType());
        summary.setRequestToken(getId().toString());
        int total_num = getNumOfFileRequest();
        summary.setTotalNumFilesInRequest(Integer.valueOf(total_num));

        int num_of_failed=0;
        int num_of_completed = 0;
        int num_of_waiting = 0;
        for(int i = 0; i< total_num; ++i) {
            FileRequest fr =fileRequests[i];
            TReturnStatus fileReqRS = fr.getReturnStatus();
            TStatusCode fileReqSC = fileReqRS.getStatusCode();
            try{
                if( fileReqSC == TStatusCode.SRM_REQUEST_QUEUED) {
                    num_of_waiting++;
                }
                /*
                    //not counted
                    // but if we do it the way Jean-Philippe does it
                    // then uncomment this code
                else if(fileReqSC == TStatusCode.SRM_REQUEST_INPROGRESS) {
                    then num_of_waiting++;
                }
                else if(fileReqSC == TStatusCode.SRM_FILE_PINNED ||
                        fileReqSC == TStatusCode.SRM_SPACE_AVAILABLE) {
                    num_of_waiting++;
                }
                 */
                else if(fileReqSC == TStatusCode.SRM_SUCCESS ||
                        fileReqSC == TStatusCode.SRM_RELEASED) {
                    num_of_completed ++;
                }
                else if(RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                    num_of_failed ++;
                }
            }catch (Exception e) {
                esay(e);
                num_of_failed ++;
            }
        }
        summary.setNumOfFailedFiles(Integer.valueOf(num_of_failed));
        summary.setNumOfCompletedFiles(Integer.valueOf(num_of_completed));
        summary.setNumOfWaitingFiles(Integer.valueOf(num_of_waiting));

        return summary;

    }

    public abstract TRequestType getRequestType();

    private static final long serialVersionUID = -5497111637295541321L;


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

    public int hashCode() {
        return getId().hashCode();
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
            String s = getMethod()+"Request #"+getId()+" created by "+getUser()+
            " with credentials : "+getCredential()+
            " state = "+getState();

            if(longformat) {
                s += '\n'+ getRequestStatus().toString();
                s += '\n'+"status code = "+ getStatusCode();
                s += '\n'+"error message = "+ getErrorMessage();
                s += "\n History of State Transitions: \n";
                s += getHistory();
                for(int i = 0; i<fileRequests.length; ++i){
                    FileRequest fr = fileRequests[i];
                    s += "\n    status code  for file "+
                    fr.getId()+":"+ fr.getStatusCode();
                    s += "\n    error message for file "+
                    fr.getId()+":"+ fr.getErrorMessage();
                    s += "\n    History of State Transitions for file "+
                    fr.getId()+": \n";
                    s += fr.getHistory();
                }

            }
            return s;
        }catch(Exception e) {
            esay(e);
            return e.toString();
        }

    }


    public abstract FileRequest getFileRequestBySurl(String surl)  throws java.sql.SQLException, SRMException ;
    public abstract TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) throws SRMException,java.sql.SQLException;

    public FileRequest[] getFileRequests()  {
	return fileRequests;
    }

}

