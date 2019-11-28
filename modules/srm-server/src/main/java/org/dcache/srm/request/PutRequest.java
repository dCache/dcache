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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMReleasedException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
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
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkArgument;
import static org.dcache.srm.handler.ReturnStatuses.getSummaryReturnStatus;

/**
 *
 * @author  timur
 */
public final class PutRequest extends ContainerRequest<PutFileRequest> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PutRequest.class);

    // private PutFileRequest fileRequests[];
    private final String[] protocols;
    private TOverwriteMode overwriteMode;

    public PutRequest(@Nonnull String srmId, SRMUser user, URI[] surls,
            Long[] sizes, boolean[] wantPermanent, String[] protocols,
            long lifetime, long max_update_period, String client_host,
            @Nullable String spaceToken,
            @Nullable TRetentionPolicy retentionPolicy,
            @Nullable TAccessLatency accessLatency,
            @Nullable String description)
    {
        super(srmId, user, max_update_period, lifetime, description, client_host,
              id -> {
                  checkArgument(surls.length == sizes.length);
                  ImmutableList.Builder<PutFileRequest> requests = ImmutableList.builder();
                  for(int i = 0; i < surls.length; ++i) {
                      requests.add(new PutFileRequest(id, surls[i], sizes[i], lifetime, spaceToken,
                                                      retentionPolicy, accessLatency));
                  }
                  return requests.build();
              });
        this.protocols = Arrays.copyOf(protocols, protocols.length);
    }

    public PutRequest(@Nonnull String srmId, long id, Long nextJobId,
            long creationTime, long lifetime, int stateId, SRMUser user,
            String scheduelerId, long schedulerTimeStamp, int numberOfRetries,
            long lastStateTransitionTime, JobHistory[] jobHistoryArray,
            ImmutableList<PutFileRequest> fileRequests, int retryDeltaTime,
            boolean should_updateretryDeltaTime, String description,
            String client_host, String statusCodeString, List<String> protocols)
    {
        super(srmId, id, nextJobId, creationTime, lifetime, stateId, user, scheduelerId,
                schedulerTimeStamp, numberOfRetries, lastStateTransitionTime,
                jobHistoryArray, fileRequests, retryDeltaTime,
                should_updateretryDeltaTime, description, client_host,
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
    public Class<? extends Job> getSchedulerType()
    {
        return PutFileRequest.class;
    }

    @Override
    public void scheduleWith(Scheduler scheduler) throws IllegalStateTransition
    {
        // save this request in request storage unconditionally
        // file requests will get stored as soon as they are
        // scheduled, and the saved state needs to be consistent
        saveJob(true);

        for (PutFileRequest request : getFileRequests()) {
            request.scheduleWith(scheduler);
        }
    }

    @Override
    public void onSrmRestart(Scheduler scheduler, boolean shouldFailJobs)
    {
        // Nothing to do.
    }

    /**
     * this callbacks are given to storage.prepareToPut
     * storage.prepareToPut calls methods of callbacks to indicate progress
     */

    @Override
    public TReturnStatus abort(String reason)
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
            updateStatus();
            State state = getState();
            if (!state.isFinal()) {
                for (PutFileRequest file : getFileRequests()) {
                    try {
                        file.abort(reason);
                        hasSuccess = true;
                    } catch (SRMException e) {
                        hasFailure = true;
                    } catch (IllegalStateTransition e) {
                        if (e.getFromState() == State.DONE) {
                            hasCompleted = true;
                        }
                        hasFailure = true;
                    }
                }
                try {
                    setStateAndStatusCode(State.CANCELED, "Request aborted", TStatusCode.SRM_ABORTED);
                } catch (IllegalStateTransition e) {
                    hasFailure = true;
                }
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

    @Override
    public void run() {
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        if (newState.isFinal()) {
            LOGGER.debug("put request state changed to {}", newState);
            for (PutFileRequest request : getFileRequests()) {
                request.wlock();
                try {
                    State fr_state = request.getState();
                    if (!fr_state.isFinal()) {
                        LOGGER.debug("changing fr#{} to {}", request.getId(), newState);
                        request.setState(newState, "Request changed: " + description);
                    }
                } catch (IllegalStateTransition ist) {
                    LOGGER.error(ist.getMessage());
                } finally {
                    request.wunlock();
                }
            }
        }

        super.processStateChange(newState, description);
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
            throws InterruptedException, SRMInvalidRequestException
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
        tryToReady();
        SrmPrepareToPutResponse response = getSrmPrepareToPutResponse();
        while (response.getReturnStatus().getStatusCode().isProcessing() && deadline.after(new Date())
               && _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
            counter = _stateChangeCounter.get();
            tryToReady();
            response = getSrmPrepareToPutResponse();
        }
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));

        return response;
    }

    private final SrmPrepareToPutResponse getSrmPrepareToPutResponse()
            throws SRMInvalidRequestException
    {
        SrmPrepareToPutResponse response = new SrmPrepareToPutResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());
        response.setArrayOfFileStatuses(new ArrayOfTPutRequestFileStatus(getArrayOfTPutRequestFileStatus()));
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }

    public final SrmStatusOfPutRequestResponse getSrmStatusOfPutRequestResponse()
            throws SRMFileRequestNotFoundException, SRMInvalidRequestException
    {
            return getSrmStatusOfPutRequestResponse(null);
    }

    public final SrmStatusOfPutRequestResponse getSrmStatusOfPutRequestResponse(org.apache.axis.types.URI[] surls)
            throws SRMFileRequestNotFoundException, SRMInvalidRequestException
    {
        SrmStatusOfPutRequestResponse response = new SrmStatusOfPutRequestResponse();
        response.setReturnStatus(getTReturnStatus());

        TPutRequestFileStatus[] statusArray = getArrayOfTPutRequestFileStatus(surls);

        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder("getSrmStatusOfPutRequestResponse:");
            s.append(" StatusCode = ").append(response.getReturnStatus().getStatusCode());
            for (TPutRequestFileStatus fs : statusArray) {
                s.append(" FileStatusCode = ").append(fs.getStatus().getStatusCode());
            }
            LOGGER.debug(s.toString());
        }

        response.setArrayOfFileStatuses(new ArrayOfTPutRequestFileStatus(statusArray));
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }

    private String getTRequestToken() {
        return String.valueOf(getId());
    }

    private TPutRequestFileStatus[] getArrayOfTPutRequestFileStatus()
            throws SRMInvalidRequestException
    {
        List<PutFileRequest> fileRequests = getFileRequests();
        int len = fileRequests.size();
        TPutRequestFileStatus[] putFileStatuses = new TPutRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            putFileStatuses[i] = fileRequests.get(i).getTPutRequestFileStatus();
        }
        return putFileStatuses;
    }

    private TPutRequestFileStatus[] getArrayOfTPutRequestFileStatus(org.apache.axis.types.URI[] surls)
            throws SRMInvalidRequestException, SRMFileRequestNotFoundException
    {
        if(surls == null) {
            return getArrayOfTPutRequestFileStatus();
        }
        int len = surls.length;
        TPutRequestFileStatus[] putFileStatuses
                = new TPutRequestFileStatus[len];
        for (int i = 0; i< len; ++i) {
            URI surl = URI.create(surls[i].toString());
            putFileStatuses[i] = getFileRequestBySurl(surl).getTPutRequestFileStatus();
        }
        return putFileStatuses;
    }

    @Override
    protected TRequestType getRequestType() {
        return TRequestType.PREPARE_TO_PUT;
    }

    private TOverwriteMode getOverwriteMode() {
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

    protected final boolean isOverwrite() {
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

    @Override
    protected String getNameForRequestType() {
        return "Put";
    }
}
