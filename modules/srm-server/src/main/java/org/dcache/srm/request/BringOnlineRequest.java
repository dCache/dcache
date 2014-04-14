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

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTBringOnlineRequestFileStatus;
import org.dcache.srm.v2_2.SrmBringOnlineResponse;
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
   ) {
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

    @Nonnull
    @Override
    public BringOnlineFileRequest getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException
    {
        for (BringOnlineFileRequest request : getFileRequests()) {
            if (request.getSurl().equals(surl)) {
                return request;
            }
        }
        throw new SRMFileRequestNotFoundException("file request for surl ="+surl +" is not found");
    }

    @Override
    public Class<? extends Job> getSchedulerType()
    {
        return BringOnlineFileRequest.class;
    }

    @Override
    public void scheduleWith(Scheduler scheduler) throws InterruptedException,
            IllegalStateTransition
    {
        // save this request in request storage unconditionally
        // file requests will get stored as soon as they are
        // scheduled, and the saved state needs to be consistent
        saveJob(true);

        for (BringOnlineFileRequest request : getFileRequests()) {
            request.scheduleWith(scheduler);
        }
    }

    @Override
    public void onSrmRestart(Scheduler scheduler)
    {
        // Nothing to do.
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
        if (state.isFinal()) {
            logger.debug("Get request state changed to {}", state);
            for (BringOnlineFileRequest fr: getFileRequests()) {
                fr.wlock();
                try {
                    if (!fr.getState().isFinal()) {
                        logger.debug("Changing fr#{} to {}", fr.getId(), state);
                        fr.setState(state, "Changing file state because request state has changed.");
                    }
                } catch (IllegalStateTransition e) {
                    logger.error(e.getMessage());
                } finally {
                    fr.wunlock();
                }
            }
        }
    }

    /**
     * Waits for up to timeout milliseconds for the request to reach a
     * non-queued state and then returns the current
     * SrmBringOnlineResponse for this BringOnlineRequest.
     */
    public final SrmBringOnlineResponse getSrmBringOnlineResponse(long timeout)
            throws InterruptedException, SRMInvalidRequestException
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

    public final SrmBringOnlineResponse getSrmBringOnlineResponse()
            throws SRMInvalidRequestException
    {
        SrmBringOnlineResponse response = new SrmBringOnlineResponse();
      // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());

        ArrayOfTBringOnlineRequestFileStatus arrayOfTBringOnlineRequestFileStatus =
            new ArrayOfTBringOnlineRequestFileStatus();
        arrayOfTBringOnlineRequestFileStatus.setStatusArray(getArrayOfTBringOnlineRequestFileStatus());
        response.setArrayOfFileStatuses(arrayOfTBringOnlineRequestFileStatus);
        return response;
    }


    public final SrmStatusOfBringOnlineRequestResponse
            getSrmStatusOfBringOnlineRequestResponse()
            throws SRMInvalidRequestException
    {
        return getSrmStatusOfBringOnlineRequestResponse(null);
    }

    public final SrmStatusOfBringOnlineRequestResponse
            getSrmStatusOfBringOnlineRequestResponse(org.apache.axis.types.URI[] surls)
            throws SRMInvalidRequestException
    {
        SrmStatusOfBringOnlineRequestResponse response =
                new SrmStatusOfBringOnlineRequestResponse();
      // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        TBringOnlineRequestFileStatus[] statusArray = getArrayOfTBringOnlineRequestFileStatus(surls);
        response.setArrayOfFileStatuses(new ArrayOfTBringOnlineRequestFileStatus(statusArray));
        if (logger.isDebugEnabled()) {
            StringBuilder s = new StringBuilder("getSrmStatusOfBringOnlineRequestResponse:");
            s.append(" StatusCode = ").append(response.getReturnStatus().getStatusCode());
            for (TBringOnlineRequestFileStatus fs : statusArray) {
                s.append(" FileStatusCode = ").append(fs.getStatus().getStatusCode());
            }
            logger.debug(s.toString());
        }
        return response;
    }


    private String getTRequestToken() {
        return String.valueOf(getId());
    }

    private TBringOnlineRequestFileStatus[] getArrayOfTBringOnlineRequestFileStatus()
            throws SRMInvalidRequestException
    {
        List<BringOnlineFileRequest> requests = getFileRequests();
        int len = requests.size();
        TBringOnlineRequestFileStatus[] getFileStatuses = new TBringOnlineRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            getFileStatuses[i] = requests.get(i).getTGetRequestFileStatus();
        }
        return getFileStatuses;
    }

    private TBringOnlineRequestFileStatus[] getArrayOfTBringOnlineRequestFileStatus(org.apache.axis.types.URI[] surls)
            throws SRMInvalidRequestException
    {
        if (surls == null) {
            return getArrayOfTBringOnlineRequestFileStatus();
        }
        int len = surls.length;
        TBringOnlineRequestFileStatus[] getFileStatuses = new TBringOnlineRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            try {
                getFileStatuses[i] = getFileRequestBySurl(URI.create(surls[i].toString())).getTGetRequestFileStatus();
            } catch (SRMFileRequestNotFoundException e) {
                getFileStatuses[i] = new TBringOnlineRequestFileStatus();
                getFileStatuses[i].setSourceSURL(surls[i]);
                getFileStatuses[i].setStatus(new TReturnStatus(TStatusCode.SRM_INVALID_PATH,
                        "SURL does not refer to an existing known file request associated with the request token."));
            }
        }
        return getFileStatuses;
    }

    public TSURLReturnStatus[] release()
            throws SRMInternalErrorException
    {
        SRMUser user = getUser();
        int len = getNumOfFileRequest();
        TSURLReturnStatus[] surlReturnStatuses = new TSURLReturnStatus[len];
        logger.debug("releaseFiles, releasing all {} files", len);
        List<BringOnlineFileRequest> requests = getFileRequests();
        for (int i = 0; i < len; i++) {
            BringOnlineFileRequest request = requests.get(i);
            org.apache.axis.types.URI surl;
            try {
                surl = new org.apache.axis.types.URI(request.getSurlString());
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                throw new RuntimeException("Failed to convert Java URI to Axis URI. " +
                        "Please report this to support@dcache.org: " + e.getMessage(), e);
            }
            surlReturnStatuses[i] = new TSURLReturnStatus(surl, request.release(user));
        }

        try {
            // we do this to make the srm update the status of the request if it changed
            // getTReturnStatus should be called before we get the
            // statuses of the each file, as the call to the
            // getTReturnStatus() can now trigger the update of the statuses
            // in particular move to the READY state, and TURL availability
            getTReturnStatus();
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return surlReturnStatuses;
    }

    public TSURLReturnStatus[] releaseFiles(org.apache.axis.types.URI[] surls)
            throws SRMInternalErrorException
    {
        SRMUser user = getUser();
        int len = surls.length;
        TSURLReturnStatus[] surlReturnStatuses = new TSURLReturnStatus[len];
        for (int i = 0; i < len; i++) {
            org.apache.axis.types.URI surl = surls[i];
            URI uri = URI.create(surl.toString());
            logger.debug("releaseFiles, releasing {}", surl);
            try {
                BringOnlineFileRequest fr = getFileRequestBySurl(uri);
                surlReturnStatuses[i] = new TSURLReturnStatus(surl, fr.release(user));
            } catch (SRMFileRequestNotFoundException e) {
                String requestToken = String.valueOf(getId());
                TReturnStatus status = BringOnlineFileRequest.unpinBySURLandRequestToken(
                        getStorage(), user, requestToken, uri);
                surlReturnStatuses[i] = new TSURLReturnStatus(surl, status);
            }
        }

        try {
            // we do this to make the srm update the status of the request if it changed
            // getTReturnStatus should be called before we get the
            // statuses of the each file, as the call to the
            // getTReturnStatus() can now trigger the update of the statuses
            // in particular move to the READY state, and TURL availability
            getTReturnStatus();
        } catch (Exception e) {
            logger.error(e.toString());
        }

        return surlReturnStatuses;
    }

    @Override
    public TRequestType getRequestType() {
        return TRequestType.BRING_ONLINE;
    }

    public long getDesiredOnlineLifetimeInSeconds() {
        return desiredOnlineLifetimeInSeconds;
    }

    @Override
    public String getNameForRequestType() {
        return "Bring online";
    }
}
