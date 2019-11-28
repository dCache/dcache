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

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.ArrayOfTGetRequestFileStatus;
import org.dcache.srm.v2_2.SrmPrepareToGetResponse;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TGetRequestFileStatus;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/*
 * @author  timur
 */
public final class GetRequest extends ContainerRequest<GetFileRequest> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(GetRequest.class);
    /** array of protocols supported by client or server (copy) */
    protected final String[] protocols;
    private final boolean isStagingAllowed;

    public GetRequest(@Nonnull String srmId, SRMUser user, URI[] surls,
            String[] protocols, long lifetime, long max_update_period,
            String description, String client_host, boolean isStagingAllowed)
    {
        super(srmId, user, max_update_period, lifetime, description, client_host,
              id -> {
                  ImmutableList.Builder<GetFileRequest> requests = ImmutableList.builder();
                  Stream.of(surls).distinct()
                          .map(surl -> new GetFileRequest(id, surl, lifetime))
                          .forEachOrdered(requests::add);
                  return requests.build();
              });
        this.protocols = Arrays.copyOf(protocols, protocols.length);
        this.isStagingAllowed = isStagingAllowed;
    }

    /**
     * restore constructor
     */
    public GetRequest(@Nonnull String srmId, long id, Long nextJobId,
            long creationTime, long lifetime, int stateId, SRMUser user,
            String scheduelerId, long schedulerTimeStamp, int numberOfRetries,
            long lastStateTransitionTime, JobHistory[] jobHistoryArray,
            ImmutableList<GetFileRequest> fileRequests, int retryDeltaTime,
            boolean should_updateretryDeltaTime, String description,
            String client_host, String statusCodeString, List<String> protocols)
    {
        super(srmId, id, nextJobId, creationTime, lifetime, stateId, user,
                scheduelerId, schedulerTimeStamp, numberOfRetries,
                lastStateTransitionTime, jobHistoryArray, fileRequests,
                retryDeltaTime, should_updateretryDeltaTime, description,
                client_host, statusCodeString);
        this.protocols = protocols.toArray(new String[protocols.size()]);
        this.isStagingAllowed = true; // FIXME record this value in the database.
    }

    @Nonnull
    @Override
    public GetFileRequest getFileRequestBySurl(URI surl) throws SRMFileRequestNotFoundException
    {
        for (GetFileRequest request : getFileRequests()) {
            if (request.getSurl().equals(surl)) {
                return request;
            }
        }
        throw new SRMFileRequestNotFoundException("file request for surl ="+surl +" is not found");
    }

    @Override
    public Class<? extends Job> getSchedulerType()
    {
        return GetFileRequest.class;
    }

    @Override
    public void scheduleWith(Scheduler scheduler) throws IllegalStateTransition
    {
        // save this request in request storage unconditionally
        // file requests will get stored as soon as they are
        // scheduled, and the saved state needs to be consistent
        saveJob(true);

        for (GetFileRequest request : getFileRequests()) {
            request.scheduleWith(scheduler);
        }
    }

    @Override
    public void onSrmRestart(Scheduler scheduler, boolean shouldFailJobs)
    {
        // Nothing to do.
    }

    /**
     * storage.PrepareToGet() is given this callbacks
     * implementation
     * it will call the method of GetCallbacks to indicate
     * progress
     */

    @Override
    public void run() {
    }

    @Override
    protected void processStateChange(State newState, String description)
    {
        if (newState.isFinal()) {
            LOGGER.debug("Get request state changed to {}.", newState);
            for (GetFileRequest request : getFileRequests()) {
                request.wlock();
                try {
                    State fr_state = request.getState();
                    if (!fr_state.isFinal()) {
                        LOGGER.debug("Changing fr#{} to {}.", request.getId(), newState);
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

    public String[] getProtocols() {
        String[] copy = new String[protocols.length];
        rlock();
        try {
            System.arraycopy(protocols, 0, copy, 0, protocols.length);
        } finally {
            runlock();
        }
        return copy;
    }

    /**
     * Waits for up to timeout milliseconds for the request to reach a
     * non-queued state and then returns the current
     * SrmPrepareToGetResponse for this GetRequest.
     */
    public final SrmPrepareToGetResponse
        getSrmPrepareToGetResponse(long timeout)
            throws InterruptedException, SRMInvalidRequestException
    {
        /* To avoid a race condition between us querying the current
         * response and us waiting for a state change notification,
         * the notification scheme is counter based. This guarantees
         * that we do not loose any notifications. A simple lock
         * around the whole loop would not have worked, as the call to
         * getSrmPrepareToGetResponse may itself trigger a state
         * change and thus cause a deadlock when the state change is
         * signaled.
         */
        Date deadline = getDateRelativeToNow(timeout);
        int counter = _stateChangeCounter.get();
        tryToReady();
        SrmPrepareToGetResponse response = getSrmPrepareToGetResponse();
        while (response.getReturnStatus().getStatusCode().isProcessing() && deadline.after(new Date())
               && _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
            counter = _stateChangeCounter.get();
            tryToReady();
            response = getSrmPrepareToGetResponse();
        }
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));

        return response;
    }

    private final SrmPrepareToGetResponse getSrmPrepareToGetResponse()
            throws SRMInvalidRequestException
    {
        SrmPrepareToGetResponse response = new SrmPrepareToGetResponse();
        response.setReturnStatus(getTReturnStatus());
        response.setRequestToken(getTRequestToken());
        response.setArrayOfFileStatuses(new ArrayOfTGetRequestFileStatus(getArrayOfTGetRequestFileStatus()));
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }

    public final SrmStatusOfGetRequestResponse getSrmStatusOfGetRequestResponse()
            throws SRMInvalidRequestException
    {
        return getSrmStatusOfGetRequestResponse(null);
    }

    public final SrmStatusOfGetRequestResponse getSrmStatusOfGetRequestResponse(org.apache.axis.types.URI[] surls)
            throws SRMInvalidRequestException
    {
        SrmStatusOfGetRequestResponse response = new SrmStatusOfGetRequestResponse();
        response.setReturnStatus(getTReturnStatus());

        TGetRequestFileStatus[] statusArray = getArrayOfTGetRequestFileStatus(surls);
        response.setArrayOfFileStatuses(new ArrayOfTGetRequestFileStatus(statusArray));

        if (LOGGER.isDebugEnabled()) {
            StringBuilder s = new StringBuilder("getSrmStatusOfGetRequestResponse:");
            s.append(" StatusCode = ").append(response.getReturnStatus().getStatusCode());
            for (TGetRequestFileStatus fs : statusArray) {
                s.append(" FileStatusCode = ").append(fs.getStatus().getStatusCode());
            }
            LOGGER.debug(s.toString());
        }
        response.setRemainingTotalRequestTime(getRemainingLifetimeIn(TimeUnit.SECONDS));
        return response;
    }


    private String getTRequestToken() {
        return String.valueOf(getId());
    }

    private TGetRequestFileStatus[] getArrayOfTGetRequestFileStatus()
            throws SRMInvalidRequestException
    {
        TGetRequestFileStatus[] getFileStatuses;
        List<GetFileRequest> fileRequests = getFileRequests();
        int len = fileRequests.size();
        getFileStatuses = new TGetRequestFileStatus[len];
        for(int i = 0; i < len; ++i) {
            getFileStatuses[i] = fileRequests.get(i).getTGetRequestFileStatus();
        }
        return getFileStatuses;
    }

    private TGetRequestFileStatus[] getArrayOfTGetRequestFileStatus(org.apache.axis.types.URI[] surls)
            throws SRMInvalidRequestException
    {
        if (surls == null) {
            return getArrayOfTGetRequestFileStatus();
        }
        int len = surls.length;
        TGetRequestFileStatus[] getFileStatuses = new TGetRequestFileStatus[len];
        for (int i = 0; i < len; ++i) {
            try {
                getFileStatuses[i] = getFileRequestBySurl(URI.create(surls[i].toString())).getTGetRequestFileStatus();
            } catch (SRMFileRequestNotFoundException e) {
                TGetRequestFileStatus fileStatus = new TGetRequestFileStatus();
                fileStatus.setStatus(new TReturnStatus(TStatusCode.SRM_INVALID_PATH, "No such file request associated with request token"));
                fileStatus.setSourceSURL(surls[i]);
                getFileStatuses[i] = fileStatus;
            }
        }
        return getFileStatuses;
    }

    @Override
    public TRequestType getRequestType() {
        return TRequestType.PREPARE_TO_GET;
    }

    public TSURLReturnStatus[] release()
            throws SRMInternalErrorException
    {
        int len = getNumOfFileRequest();
        TSURLReturnStatus[] surlReturnStatuses = new TSURLReturnStatus[len];
        LOGGER.debug("releaseFiles, releasing all {} files", len);
        List<GetFileRequest> requests = getFileRequests();
        for (int i = 0; i < len; i++) {
            GetFileRequest fr = requests.get(i);
            org.apache.axis.types.URI surl;
            try {
                surl = new org.apache.axis.types.URI(fr.getSurlString());
            } catch (org.apache.axis.types.URI.MalformedURIException e) {
                throw new RuntimeException("Failed to convert Java URI to Axis URI. " +
                        "Please report this to support@dcache.org: " + e.getMessage(), e);
            }
            surlReturnStatuses[i] = new TSURLReturnStatus(surl, fr.release());
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
            LOGGER.debug("releaseFiles, releasing {}", surl);
            try {
                GetFileRequest fr = getFileRequestBySurl(uri);
                surlReturnStatuses[i] = new TSURLReturnStatus(surl, fr.release());
            } catch (SRMFileRequestNotFoundException e) {
                String requestToken = String.valueOf(getId());
                TReturnStatus status = BringOnlineFileRequest.unpinBySURLandRequestToken(
                        getStorage(), user, requestToken, uri);
                surlReturnStatuses[i] = new TSURLReturnStatus(surl, status);
            }
        }

        return surlReturnStatuses;
    }

    @Override
    public String getNameForRequestType() {
        return "Get";
    }

    protected boolean isStagingAllowed() {
        return isStagingAllowed;
    }
}
