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
 * PutRequestHandler.java
 *
 * Created on July 15, 2003, 2:18 PM
 */

package org.dcache.srm.request;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTPutRequestFileStatus;
import org.dcache.srm.v2_2.SrmPrepareToPutResponse;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.TAccessLatency;
import org.dcache.srm.v2_2.TOverwriteMode;
import org.dcache.srm.v2_2.TPutRequestFileStatus;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TRetentionPolicy;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

/**
 *
 * @author  timur
 */
public final class PutRequest extends ContainerRequest<PutFileRequest> {
    private final static Logger logger =
            LoggerFactory.getLogger(PutRequest.class);

    // private PutFileRequest fileRequests[];
    private final String[] protocols;
    private TOverwriteMode overwriteMode;

    public PutRequest(SRMUser user,
    Long requestCredentialId,
    String[] srcFileNames,
    String[] destUrls,
    long[] sizes,
    boolean[] wantPermanent,
    String[] protocols,
    long lifetime,
    long max_update_period,
    int max_number_of_retries,
    String client_host,
    String spaceToken,
    TRetentionPolicy retentionPolicy,
    TAccessLatency accessLatency,
    String description
    ) throws Exception {

        super(user,
                requestCredentialId,
                max_number_of_retries,
                max_update_period,
                lifetime,
                description,
                client_host);
        int len = protocols.length;
        this.protocols = new String[len];
        System.arraycopy(protocols,0,this.protocols,0,len);

        len = srcFileNames.length;
        if(len != destUrls.length || len != sizes.length ||
        len != wantPermanent.length) {
            throw new IllegalArgumentException(
            "srcFileNames, destUrls, sizes,"+
            " wantPermanent arrays dimensions mismatch");
        }
        List<PutFileRequest> requests = Lists.newArrayListWithCapacity(len);
        for(int i = 0; i < len; ++i) {
            PutFileRequest request = new PutFileRequest(getId(),
                    requestCredentialId, destUrls[i], sizes[i], lifetime,
                    max_number_of_retries, spaceToken, retentionPolicy,
                    accessLatency);
            requests.add(request);
        }
        setFileRequests(requests);
    }

    public  PutRequest(
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
    PutFileRequest[] fileRequests,
    int retryDeltaTime,
    boolean should_updateretryDeltaTime,
    String description,
    String client_host,
    String statusCodeString,
    List<String> protocols
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
        this.protocols = protocols.toArray(new String[protocols.size()]);

    }

    @Nonnull
    @Override
    public PutFileRequest getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException
    {
        for (PutFileRequest request : getFileRequests()) {
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
        for (PutFileRequest request : getFileRequests()) {
            request.schedule();
        }
    }

    public void proccessRequest() {
        logger.debug("proccessing put request");
        String supported_protocols[];
        try {
            supported_protocols = getStorage().supportedGetProtocols();
        }
        catch(SRMException srme) {
            logger.error(" protocols are not supported");
            logger.error(srme.toString());
            //setFailedStatus ("protocols are not supported");
            return;
        }
        Set<String> supported_protocols_set = new HashSet<>(Arrays.asList(supported_protocols));
        supported_protocols_set.retainAll(Arrays.asList(protocols));
        if(supported_protocols_set.isEmpty()) {
            logger.error("processPutRequest() : error selecting protocol");
            //setFailedStatus ("protocols are not supported");
            return;
        }
        //do not need it, let it be garbagecollected
        supported_protocols_set = null;

    }

    /**
     * this callbacks are given to storage.prepareToPut
     * storage.prepareToPut calls methods of callbacks to indicate progress
     */

    @Override
    public String getMethod() {
        return "Put";
    }

    @Override
    public TReturnStatus abort()
    {
        wlock();
        try {
            /* [ SRM 2.2, 5.11.2 ]
             *
             * a) srmAbortRequest terminates all files in the request regardless of the file
             *    state. Remove files from the queue, and release cached files if a limited
             *    lifetime is associated with the file.
             * c) Abort must be allowed to all requests with requestToken.
             * f) When aborting srmPrepareToPut request before srmPutDone and before the file
             *    transfer, the SURL must not exist as the result of the successful abort on
             *    the SURL. Any srmRm request on the SURL must fail.
             * g) When aborting srmPrepareToPut request before srmPutDone and after the file
             *    transfer, the SURL may exist, and a srmRm request on the SURL may remove
             *    the requested SURL.
             * h) When aborting srmPrepareToPut request after srmPutDone, it must be failed
             *    for those files. An explicit srmRm is required to remove those successfully
              *   completed files for srmPrepareToPut.
             * i) When duplicate abort request is issued on the same request, SRM_SUCCESS
             *    may be returned to all duplicate abort requests and no operations on
             *    duplicate abort requests are performed.
             */

            boolean hasSuccess = false;
            boolean hasFailure = false;
            boolean hasCompleted = false;
            // FIXME: we do this to make the srm update the status of the request if it changed
            getRequestStatus();
            State state = getState();
            if (!State.isFinalState(state)) {
                for (PutFileRequest file : getFileRequests()) {
                    try {
                        file.abort();
                        hasSuccess = true;
                    } catch (IllegalStateTransition e) {
                        if (e.getFromState() == State.DONE) {
                            hasCompleted = true;
                        }
                        hasFailure = true;
                    }
                }
                // FIXME: Trigger state update now that we aborted the file requests
                getRequestStatus();
            } else if (state == State.DONE) {
                return new TReturnStatus(TStatusCode.SRM_FAILURE,
                        "Put request completed successfully and cannot be aborted");
            }
            TReturnStatus returnStatus = getSummaryReturnStatus(hasFailure, hasSuccess);
            if (hasCompleted) {
                returnStatus = new TReturnStatus(returnStatus.getStatusCode(),
                        "Some SURLs have completed successfully and cannot be aborted");
            }
            return returnStatus;
        } finally {
            wunlock();
        }
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

            logger.debug("copy request state changed to "+state);
            for (PutFileRequest request : getFileRequests()) {
                try {
                    State fr_state = request.getState();
                    if(!State.isFinalState(fr_state ))
                    {
                        logger.debug("changing fr#"+request.getId()+" to "+state);
                        request.setState(state,"changing file state because request state has changed");
                    }
                }
                catch(IllegalStateTransition ist) {
                    logger.error("Illegal State Transition : " +ist.getMessage());
                }
            }

        }

    }

    /**
     * Getter for property protocols.
     * @return Value of property protocols.
     */
    public String[] getProtocols() {
        return this.protocols;
    }


    /**
     * Waits for up to timeout milliseconds for the request to reach a
     * non-queued state and then returns the current
     * SrmPrepareToPutResponse for this PutRequest.
     */
    public final SrmPrepareToPutResponse
        getSrmPrepareToPutResponse(long timeout)
        throws SRMException, InterruptedException
    {
        /* To avoid a race condition between us querying the current
         * response and us waiting for a state change notification,
         * the notification scheme is counter based. This guarantees
         * that we do not loose any notifications. A simple lock
         * around the whole loop would not have worked, as the call to
         * getSrmPrepareToPutResponse may itself trigger a state
         * change and thus cause a deadlock when the state change is
         * signaled.
         */
        Date deadline = getDateRelativeToNow(timeout);
        int counter = _stateChangeCounter.get();
        SrmPrepareToPutResponse response = getSrmPrepareToPutResponse();
        while (response.getReturnStatus().getStatusCode().isProcessing() &&
               _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
            counter = _stateChangeCounter.get();
            response = getSrmPrepareToPutResponse();
        }
        return response;
    }

    public final SrmPrepareToPutResponse getSrmPrepareToPutResponse()
    throws SRMException {
        SrmPrepareToPutResponse response = new SrmPrepareToPutResponse();
       // getTReturnStatus should be called before we get the
       // statuses of the each file, as the call to the
       // getTReturnStatus() can now trigger the update of the statuses
       // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());
        ArrayOfTPutRequestFileStatus  arrayOfTPutRequestFileStatus =
            new ArrayOfTPutRequestFileStatus();
        arrayOfTPutRequestFileStatus.setStatusArray(
            getArrayOfTPutRequestFileStatus(null));
        response.setArrayOfFileStatuses(arrayOfTPutRequestFileStatus);
        return response;
    }

    public final SrmStatusOfPutRequestResponse getSrmStatusOfPutRequestResponse()
            throws SRMException
    {
            return getSrmStatusOfPutRequestResponse(null);
    }

    public final SrmStatusOfPutRequestResponse getSrmStatusOfPutRequestResponse(URI[] surls)
            throws SRMException
    {
        SrmStatusOfPutRequestResponse response = new SrmStatusOfPutRequestResponse();

        // getTReturnStatus should be called before we get the
        // statuses of the each file, as the call to the
        // getTReturnStatus() can now trigger the update of the statuses
        // in particular move to the READY state, and TURL availability
        response.setReturnStatus(getTReturnStatus());

        TPutRequestFileStatus[] statusArray = getArrayOfTPutRequestFileStatus(surls);

        if (logger.isDebugEnabled()) {
            StringBuilder s = new StringBuilder("getSrmStatusOfPutRequestResponse:");
            s.append(" StatusCode = ").append(response.getReturnStatus().getStatusCode());
            for (TPutRequestFileStatus fs : statusArray) {
                s.append(" FileStatusCode = ").append(fs.getStatus().getStatusCode());
            }
            logger.debug(s.toString());
        }

        response.setArrayOfFileStatuses(new ArrayOfTPutRequestFileStatus(statusArray));
        return response;
    }

    private String getTRequestToken() {
        return String.valueOf(getId());
    }

    private TPutRequestFileStatus[] getArrayOfTPutRequestFileStatus(URI[] surls) throws SRMException {
         int len = surls == null ? getNumOfFileRequest():surls.length;
        TPutRequestFileStatus[] putFileStatuses
            = new TPutRequestFileStatus[len];
        if(surls == null) {
            for(int i = 0; i< len; ++i) {
                PutFileRequest fr = getFileRequests().get(i);
                putFileStatuses[i] = fr.getTPutRequestFileStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                PutFileRequest fr = getFileRequestBySurl(surls[i]);
                putFileStatuses[i] = fr.getTPutRequestFileStatus();
            }

        }
        return putFileStatuses;
    }


    @Override
    public TSURLReturnStatus[] getArrayOfTSURLReturnStatus(URI[] surls) throws SRMException {
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
        boolean failed_req = false;
        boolean pending_req = false;
        boolean running_req = false;
        boolean ready_req = false;
        boolean done_req = false;
        String fr_error="";
        if(surls == null) {
            for(int i = 0; i< len; ++i) {
                PutFileRequest fr = getFileRequests().get(i);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }
        } else {
            for(int i = 0; i< len; ++i) {
                PutFileRequest fr = getFileRequestBySurl(surls[i]);
                surlLReturnStatuses[i] = fr.getTSURLReturnStatus();
            }

        }
        return surlLReturnStatuses;
    }

    @Override
    public TRequestType getRequestType() {
        return TRequestType.PREPARE_TO_PUT;
    }

    public TOverwriteMode getOverwriteMode() {
        rlock();
        try {
            return overwriteMode;
        } finally {
            runlock();
        }
    }

    public void setOverwriteMode(TOverwriteMode overwriteMode) {
        wlock();
        try {
            this.overwriteMode = overwriteMode;
        } finally {
            wunlock();
        }
    }

    public final boolean isOverwrite() {
        if(getConfiguration().isOverwrite()) {
            TOverwriteMode mode = getOverwriteMode();
            if(mode == null) {
                return getConfiguration().isOverwrite_by_default();
            }
            return mode.equals(TOverwriteMode.ALWAYS);
        }
        return false;
    }

    @Override
    public long extendLifetimeMillis(long newLifetimeInMillis) throws SRMException {
        /* [ SRM 2.2, 5.16.2 ]
         *
         * h) Lifetime cannot be extended on the released files, aborted files, expired
         *    files, and suspended files. For example, pin lifetime cannot be extended
         *    after srmPutDone is requested on SURLs for srmPrepareToPut request. In
         *    such case, SRM_INVALID_REQUEST at the file level must be returned, and
         *    SRM_PARTIAL_SUCCESS or SRM_FAILURE must be returned at the request level.
         */
        try {
            return super.extendLifetimeMillis(newLifetimeInMillis);
        } catch(SRMReleasedException releasedException) {
            throw new SRMInvalidRequestException(releasedException.getMessage());
        }
    }
}
