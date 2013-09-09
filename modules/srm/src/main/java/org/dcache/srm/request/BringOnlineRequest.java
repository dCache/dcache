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

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
import org.dcache.srm.v2_2.SrmReleaseFilesResponse;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.v2_2.TBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
/*
 * @author  timur
 */
public final class BringOnlineRequest extends ContainerRequest<BringOnlineFileRequest> {
    private static final Logger logger = LoggerFactory.getLogger(BringOnlineRequest.class);
    /** array of protocols supported by client or server (copy) */
    private final String[] protocols;
    private final long desiredOnlineLifetimeInSeconds;


    public BringOnlineRequest(SRMUser user,
                              Long requestCredentialId,
                              URI[] surls,
                              String[] protocols,
                              long lifetime,
                              long desiredOnlineLifetimeInSeconds,
                              long max_update_period,
                              int max_number_of_retries,
                              String description,
                              String client_host)
    {
        super(user,requestCredentialId,
            max_number_of_retries,
            max_update_period,
            lifetime,
            description,
            client_host);
        logger.debug("constructor");
        logger.debug("user = "+user);
        logger.debug("requestCredetialId="+requestCredentialId);
        if(protocols != null) {
            int len = protocols.length;
            this.protocols = new String[len];
            System.arraycopy(protocols,0,this.protocols,0,len);
        } else {
            this.protocols = null;
        }
        this.desiredOnlineLifetimeInSeconds = desiredOnlineLifetimeInSeconds;
        List<BringOnlineFileRequest> requests = Lists.newArrayListWithCapacity(surls.length);
        for(URI surl : surls) {
            BringOnlineFileRequest request = new BringOnlineFileRequest(getId(),
                    requestCredentialId, surl, lifetime, max_number_of_retries);
            requests.add(request);
        }
        setFileRequests(requests);
    }

    /**
     * restore constructor
     */
    public  BringOnlineRequest(
    long id,
    Long nextJobId,
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
    BringOnlineFileRequest[] fileRequests,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    String[] protocols
   )  throws SQLException {
        super( id,
        nextJobId,
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
        statusCodeString);
        this.protocols = protocols;
        this.desiredOnlineLifetimeInSeconds = 0;

    }

    @Override
    public BringOnlineFileRequest getFileRequestBySurl(URI surl) throws SQLException, SRMException{
        if(surl == null) {
           throw new SRMException("surl is null");
        }
        for (BringOnlineFileRequest request : getFileRequests()) {
            if (request.getSurl().equals(surl)) {
                return request;
            }
        }
        throw new SRMFileRequestNotFoundException("file request for surl ="+surl +" is not found");
    }

    @Override
    public void schedule() throws InterruptedException,
    IllegalStateTransition {

        // save this request in request storage unconditionally
        // file requests will get stored as soon as they are
        // scheduled, and the saved state needs to be consistent
        saveJob(true);

        for (BringOnlineFileRequest request : getFileRequests()) {
            request.schedule();
        }
    }

     public String[] getProtocols() {
        if(protocols == null) {
            return null;
        }
        String[] copy = new String[protocols.length];
        System.arraycopy(protocols, 0, copy, 0, protocols.length);
        return copy;
    }

    /**
     * storage.PrepareToGet() is given this callbacks
     * implementation
     * it will call the method of GetCallbacks to indicate
     * progress
     */

    @Override
    public String getMethod() {
        return "BringOnline";
    }

    //we do not want to stop handler if the
    //the request is ready (all file reqs are ready), since the actual transfer migth
    // happen any time after that
    // the handler, by staing in running state will prevent other queued
    // req from being executed
    public boolean shouldStopHandlerIfReady() {
        return false;
    }

    @Override
    public void run() throws NonFatalJobFailure, FatalJobFailure {
    }

    @Override
    protected void stateChanged(State oldState) {
        State state = getState();
        if(State.isFinalState(state)) {

            logger.debug("get request state changed to "+state);
            for (BringOnlineFileRequest fr: getFileRequests()) {
                try {
                    logger.debug("changing fr#"+fr.getId()+" to "+state);
                    fr.setState(state,"changing file state because request state has changed");
                }
                catch(IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
            }

        }

    }

    /**
     * Waits for up to timeout milliseconds for the request to reach a
     * non-queued state and then returns the current
     * SrmBringOnlineResponse for this BringOnlineRequest.
     */
    public final SrmBringOnlineResponse
        getSrmBringOnlineResponse(long timeout)
        throws SRMException, SQLException, InterruptedException
    {
        /* To avoid a race condition between us querying the current
         * response and us waiting for a state change notification,
         * the notification scheme is counter based. This guarantees
         * that we do not loose any notifications. A simple lock
         * around the whole loop would not have worked, as the call to
         * getSrmBringOnlineResponse may itself trigger a state change
         * and thus cause a deadlock when the state change is
         * signaled.
         */
        Date deadline = getDateRelativeToNow(timeout);
        int counter = _stateChangeCounter.get();
        SrmBringOnlineResponse response = getSrmBringOnlineResponse();
        while (response.getReturnStatus().getStatusCode().isProcessing() &&
               _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
            counter = _stateChangeCounter.get();
            response = getSrmBringOnlineResponse();
        }
        return response;
    }

    public  final SrmBringOnlineResponse getSrmBringOnlineResponse()
    throws SRMException ,SQLException {
        SrmBringOnlineResponse response = new SrmBringOnlineResponse();
      // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());

        ArrayOfTBringOnlineRequestFileStatus arrayOfTBringOnlineRequestFileStatus =
            new ArrayOfTBringOnlineRequestFileStatus();
        arrayOfTBringOnlineRequestFileStatus.setStatusArray(getArrayOfTBringOnlineRequestFileStatus(null));
        response.setArrayOfFileStatuses(arrayOfTBringOnlineRequestFileStatus);
        return response;
    }


    public final SrmStatusOfBringOnlineRequestResponse
            getSrmStatusOfBringOnlineRequestResponse()
    throws SRMException, SQLException {
        return getSrmStatusOfBringOnlineRequestResponse(null);
    }


    public final SrmStatusOfBringOnlineRequestResponse
            getSrmStatusOfBringOnlineRequestResponse(
            URI[] surls)
    throws SRMException, SQLException {
        SrmStatusOfBringOnlineRequestResponse response =
                new SrmStatusOfBringOnlineRequestResponse();
      // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        ArrayOfTBringOnlineRequestFileStatus arrayOfTBringOnlineRequestFileStatus =
            new ArrayOfTBringOnlineRequestFileStatus();
        arrayOfTBringOnlineRequestFileStatus.setStatusArray(
            getArrayOfTBringOnlineRequestFileStatus(surls));
        response.setArrayOfFileStatuses(arrayOfTBringOnlineRequestFileStatus);
        StringBuilder s = new StringBuilder("getSrmStatusOfBringOnlineRequestResponse:");
        s.append(" StatusCode = ")
                .append(response.getReturnStatus().getStatusCode());
        for(TBringOnlineRequestFileStatus fs :arrayOfTBringOnlineRequestFileStatus.getStatusArray()) {
            s.append(" FileStatusCode = ")
                    .append(fs.getStatus().getStatusCode());
        }
        logger.debug(s.toString());
        return response;
    }


    private String getTRequestToken() {
        return String.valueOf(getId());
    }

   /* private ArrayOfTGetRequestFileStatus getArrayOfTGetRequestFileStatus()throws SRMException,java.sql.SQLException {
        return getArrayOfTGetRequestFileStatus(null);
    }
    */

    private TBringOnlineRequestFileStatus[] getArrayOfTBringOnlineRequestFileStatus(URI[] surls) throws SRMException,SQLException {
        int len = surls == null ? getNumOfFileRequest():surls.length;
         TBringOnlineRequestFileStatus[] getFileStatuses
            = new TBringOnlineRequestFileStatus[len];
        if(surls == null) {
            List<BringOnlineFileRequest> requests = getFileRequests();
            for(int i = 0; i< len; ++i) {
                BringOnlineFileRequest fr = requests.get(i);
                getFileStatuses[i] = fr.getTGetRequestFileStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                BringOnlineFileRequest fr = getFileRequestBySurl(surls[i]);
                getFileStatuses[i] = fr.getTGetRequestFileStatus();
            }

        }
        return getFileStatuses;
    }

    @Override
    public TSURLReturnStatus[] getArrayOfTSURLReturnStatus(URI[] surls) throws SRMException,SQLException {
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
            List<BringOnlineFileRequest> requests = getFileRequests();
            for(int i = 0; i< len; ++i) {
                BringOnlineFileRequest fr = requests.get(i);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                BringOnlineFileRequest fr = getFileRequestBySurl(surls[i]);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }

        }
        return surlLReturnStatuses;
    }

    public SrmReleaseFilesResponse releaseFiles(URI[] surls)
        throws SRMInvalidRequestException
    {
        logger.debug("releaseFiles");
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
            logger.debug("releaseFiles, surls is null, releasing all "+len+" files");
            for(int i = 0; i< len; ++i) {
                List<BringOnlineFileRequest> requests = getFileRequests();
                BringOnlineFileRequest fr = requests.get(i);
                surlLReturnStatuses[i] = fr.releaseFile();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                org.apache.axis.types.URI surl;
                try {
                    surl = new org.apache.axis.types.URI(surls[i].toASCIIString());
                } catch (org.apache.axis.types.URI.MalformedURIException e) {
                    throw new SRMInvalidRequestException("Invalid surl: " + e.getMessage());
                }
                logger.debug("releaseFiles, releasing file " + surl);
                BringOnlineFileRequest fr;
                try {
                    fr = getFileRequestBySurl(surls[i]);
                } catch (SRMFileRequestNotFoundException sfrnfe ) {
                    try {
                        SRMUser user =getUser();
                        long theId =getId();
                        BringOnlineFileRequest.unpinBySURLandRequestId(
                            getStorage(),user, theId, surls[i]);
                        TSURLReturnStatus surlStatus = new TSURLReturnStatus();
                        TReturnStatus surlReturnStatus = new TReturnStatus();
                        surlReturnStatus.setStatusCode(TStatusCode.SRM_SUCCESS);
                        surlStatus.setSurl(surl);
                        surlStatus.setStatus(surlReturnStatus);
                        surlLReturnStatuses[i] = surlStatus;
                    } catch (Exception e) {
                        TSURLReturnStatus surlStatus = new TSURLReturnStatus();
                        TReturnStatus surlReturnStatus = new TReturnStatus();
                        surlReturnStatus.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
                        surlReturnStatus.setExplanation(
                            "could not release file, neither file request " +
                            "for surl, nor pin is found: "+e);
                        surlStatus.setSurl(surl);
                        surlStatus.setStatus(surlReturnStatus);
                        surlLReturnStatuses[i] = surlStatus;
                    }
                    continue;
                }
                catch (Exception e) {
                    TSURLReturnStatus surlStatus = new TSURLReturnStatus();
                    TReturnStatus surlReturnStatus = new TReturnStatus();
                    surlReturnStatus.setStatusCode(TStatusCode.SRM_INVALID_PATH);
                    surlReturnStatus.setExplanation(
                        "error retrieving a file request for an surl: "+e);
                    surlStatus.setSurl(surl);
                    surlStatus.setStatus(surlReturnStatus);
                    surlLReturnStatuses[i] = surlStatus;
                    continue;
                }

                try {
                    surlLReturnStatuses[i] = fr.releaseFile();
                }
                catch (Exception e) {
                    TSURLReturnStatus surlStatus = new TSURLReturnStatus();
                    TReturnStatus surlReturnStatus = new TReturnStatus();
                    surlReturnStatus.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
                    surlReturnStatus.setExplanation("could not releaseFile file: "+e);
                    surlStatus.setSurl(surl);
                    surlStatus.setStatus(surlReturnStatus);
                    surlLReturnStatuses[i] = surlStatus;
                }
            }

        }

       try{
       // we do this to make the srm update the status of the request if it changed
       // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
            getTReturnStatus();
        }
        catch(Exception e) {
            logger.error(e.toString());
        }
        int errors_cnt = 0;

        for (TSURLReturnStatus surlLReturnStatus: surlLReturnStatuses) {
            if(surlLReturnStatus == null || !surlLReturnStatus.getStatus().
                getStatusCode().equals(TStatusCode.SRM_SUCCESS)) {
                errors_cnt++;
            }
        }
        TReturnStatus status = new TReturnStatus();
        if(errors_cnt == 0) {
            status.setStatusCode(TStatusCode.SRM_SUCCESS);
        } else if (errors_cnt < len) {
            status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
        } else {
            status.setStatusCode(TStatusCode.SRM_FAILURE);
        }
        SrmReleaseFilesResponse srmReleaseFilesResponse = new SrmReleaseFilesResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        srmReleaseFilesResponse.setArrayOfFileStatuses(new ArrayOfTSURLReturnStatus(surlLReturnStatuses));
        return srmReleaseFilesResponse;

    }


    @Override
    public TRequestType getRequestType() {
        return TRequestType.BRING_ONLINE;
    }

    public long getDesiredOnlineLifetimeInSeconds() {
        return desiredOnlineLifetimeInSeconds;
    }

}
