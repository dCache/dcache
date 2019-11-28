package org.dcache.srm.request;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Date;
import java.util.stream.Stream;

import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static org.dcache.util.TimeUtils.relativeTimestamp;

public final class LsRequest extends ContainerRequest<LsFileRequest> {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LsRequest.class);

        private final long offset;         // starting entry number
        private final long count;          // max number of entries to be returned as set by client
        private final int maxNumOfResults; // max number of entries allowed by server, settable via configuration
        private int numberOfResults;    // counts only entries allowed to be returned
        private long counter;           // counts all entries
        private final int numOfLevels;    // recursion level
        private final boolean longFormat;
        private String explanation;

        public LsRequest(@Nonnull String srmId,
                         SRMUser user,
                         URI[] surls,
                         long lifetime,
                         long max_update_period,
                         String client_host,
                         long count,
                         long offset,
                         int numOfLevels,
                         boolean longFormat,
                         int maxNumOfResults)
        {
                super(srmId, user, max_update_period, lifetime, "Ls request", client_host,
                      id -> {
                          ImmutableList.Builder<LsFileRequest> requests = ImmutableList.builder();
                          Stream.of(surls)
                                  .map(surl -> new LsFileRequest(id, surl, lifetime))
                                  .forEachOrdered(requests::add);
                          return requests.build();
                      });
                this.count = count;
                this.offset = offset;
                this.numOfLevels = numOfLevels;
                this.longFormat = longFormat;
                this.maxNumOfResults = maxNumOfResults;
        }

        public  LsRequest(@Nonnull String srmId, long id, Long nextJobId,
                long creationTime, long lifetime, int stateId, SRMUser user,
                String scheduelerId, long schedulerTimeStamp, int numberOfRetries,
                long lastStateTransitionTime, JobHistory[] jobHistoryArray,
                ImmutableList<LsFileRequest> fileRequests, int retryDeltaTime,
                boolean should_updateretryDeltaTime, String description,
                String client_host, String statusCodeString, String explanation,
                boolean longFormat, int numOfLevels, long count, long offset)
        {
            super(srmId, id, nextJobId, creationTime, lifetime, stateId, user,
                    scheduelerId, schedulerTimeStamp, numberOfRetries,
                    lastStateTransitionTime, jobHistoryArray, fileRequests,
                    retryDeltaTime, should_updateretryDeltaTime, description,
                    client_host, statusCodeString);
            this.explanation=explanation;
            this.longFormat=longFormat;
            this.numOfLevels=numOfLevels;
            this.maxNumOfResults = 100;
            this.count=count;
            this.offset=offset;
        }

        @Nonnull
        @Override
        public LsFileRequest getFileRequestBySurl(URI surl)
                throws SRMFileRequestNotFoundException
        {
                for (LsFileRequest request : getFileRequests()) {
                        if (request.getSurl().equals(surl)) {
                                return request;
                        }
                }
                throw new SRMFileRequestNotFoundException("ls file request for surl ="+surl +" is not found");
        }

        @Override
        public Class<? extends Job> getSchedulerType()
        {
            return LsFileRequest.class;
        }

        @Override
        public void scheduleWith(Scheduler scheduler) throws IllegalStateTransition
        {
            // save this request in request storage unconditionally
            // file requests will get stored as soon as they are
            // scheduled, and the saved state needs to be consistent
            saveJob(true);

            for (LsFileRequest request : getFileRequests()) {
                request.scheduleWith(scheduler);
            }
        }

        @Override
        public void onSrmRestart(Scheduler scheduler, boolean shouldFailJobs)
        {
            // Nothing to do.
        }

        public String kill() {
                return "request was ready, set all ready file statuses to done";
        }

        @Override
        public void run() {
        }

        @Override
        protected void processStateChange(State newState, String description)
        {
            if (newState.isFinal()) {
                for (LsFileRequest fr : getFileRequests()) {
                    fr.wlock();
                    try {
                        State fr_state = fr.getState();
                        if (!fr_state.isFinal()) {
                            fr.setState(newState, "Request changed: " + description);
                        }
                    } catch(IllegalStateTransition ist) {
                        LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                    } finally {
                        fr.wunlock();
                    }
                }
            }

            super.processStateChange(newState, description);
        }

        /**
         * Waits for up to timeout milliseconds for the request to
         * reach a non-queued state and then returns the current
         * SrmLsResponse for this LsRequest.
         */
        public final SrmLsResponse getSrmLsResponse(long timeout)
                throws InterruptedException, SRMInvalidRequestException
        {
                /* To avoid a race condition between us querying the
                 * current response and us waiting for a state change
                 * notification, the notification scheme is counter
                 * based. This guarantees that we do not loose any
                 * notifications. A simple lock around the whole loop
                 * would not have worked, as the call to
                 * getSrmLsResponse may itself trigger a state change
                 * and thus cause a deadlock when the state change is
                 * signaled.
                 */
                Date deadline = getDateRelativeToNow(timeout);
                int counter = _stateChangeCounter.get();
                SrmLsResponse response = getSrmLsResponse();
                while (response.getReturnStatus().getStatusCode().isProcessing() && deadline.after(new Date()) &&
                       _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
                        counter = _stateChangeCounter.get();
                        response = getSrmLsResponse();
                }
                return response;
        }

        public final SrmLsResponse getSrmLsResponse() throws SRMInvalidRequestException
        {
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(getTReturnStatus());
                if (!response.getReturnStatus().getStatusCode().isProcessing()) {
                    response.setDetails(new ArrayOfTMetaDataPathDetail(getPathDetailArray()));
                } else {
                    response.setDetails(null);
                }
                response.setRequestToken(getTRequestToken());
                return response;
        }

        public final SrmStatusOfLsRequestResponse getSrmStatusOfLsRequestResponse()
                throws SRMInvalidRequestException
        {
                return new SrmStatusOfLsRequestResponse(
                        getTReturnStatus(), new ArrayOfTMetaDataPathDetail(getPathDetailArray()));
        }

        private String getTRequestToken() {
                return String.valueOf(getId());
        }

        public TMetaDataPathDetail[] getPathDetailArray()
                throws SRMInvalidRequestException
        {
                int len = getFileRequests().size();
                TMetaDataPathDetail detail[] = new TMetaDataPathDetail[len];
                for(int i = 0; i<len; ++i) {
                        LsFileRequest fr = getFileRequests().get(i);
                        detail[i] = fr.getMetaDataPathDetail();
                }
                return detail;
        }

        public final boolean increaseResultsNumAndContinue()
                throws SRMTooManyResultsException
        {
            wlock();
            try {
                setNumberOfResults(getNumberOfResults() + 1);
                if(getNumberOfResults() > getMaxNumOfResults()) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("max results number of ").append(getMaxNumOfResults());
                        sb.append(" exceeded. Try to narrow down with count and use offset to get complete listing");
                        setExplanation(sb.toString());
                        setStatusCode(TStatusCode.SRM_TOO_MANY_RESULTS);
                        throw new SRMTooManyResultsException(sb.toString());
                }
                return getNumberOfResults() <= getCount() || getCount() == 0;
            } finally {
                wunlock();
            }
        }

        @Override
        public TRequestType getRequestType() {
                return TRequestType.LS;
        }

        public long getCount(){
                // final, no need to synchronize
                return count;
        }

        public long getOffset(){
                // final, no need to synchronize
                return offset;
        }

        public int getNumOfLevels() {
                // final, no need to synchronize
                return numOfLevels;
        }

        public boolean getLongFormat() {
                return isLongFormat();
        }

        public int getMaxNumOfResults() {
            rlock();
            try {
                return maxNumOfResults;
            } finally {
                runlock();
            }

        }

        public String getExplanation() {
            rlock();
            try {
                return explanation;
            } finally {
                runlock();
            }
        }

        public void setExplanation(String txt) {
            wlock();
            try {
                this.explanation=txt;
            } finally {
                wunlock();
            }

        }

        @Override
        public  final TReturnStatus getTReturnStatus()  {
            wlock();
            try {
                updateStatus();
                if(getStatusCode() != null) {
                        return new TReturnStatus(getStatusCode(),getExplanation());
                }
                int len = getNumOfFileRequest();
                if (len == 0) {
                    return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR,
                            "Could not find (deserialize) files in the request,  NumOfFileRequest is 0");
                }
                int failed_req           = 0;
                int canceled_req         = 0;
                int pending_req          = 0;
                int running_req          = 0;
                int done_req             = 0;
                int got_exception        = 0;
                int auth_failure         = 0;
                for (LsFileRequest fr : getFileRequests()) {
                        TReturnStatus fileReqRS = fr.getReturnStatus();
                        TStatusCode fileReqSC   = fileReqRS.getStatusCode();
                        try {
                                if (fileReqSC == TStatusCode.SRM_REQUEST_QUEUED) {
                                        pending_req++;
                                }
                                else if(fileReqSC == TStatusCode.SRM_REQUEST_INPROGRESS) {
                                        running_req++;
                                }
                                else if (fileReqSC == TStatusCode.SRM_ABORTED) {
                                        canceled_req++;
                                }
                                else if (fileReqSC == TStatusCode.SRM_AUTHORIZATION_FAILURE) {
                                        auth_failure++;
                                }
                                else if (RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                                        failed_req++;
                                }
                                else  {
                                        done_req++;
                                }
                        }
                        catch (Exception e) {
                                LOGGER.error(e.toString());
                                got_exception++;
                        }
                }
                boolean isFinalState = getState().isFinal();
                if (done_req == len ) {
                        if (!isFinalState) {
                                try {
                                       setState(State.DONE, "Operation completed.");
                                 }
                                 catch(IllegalStateTransition ist) {
                                         LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                                 }
                         }
                         return new TReturnStatus(TStatusCode.SRM_SUCCESS, null);
                }
                if (canceled_req == len ) {
                        return new TReturnStatus(TStatusCode.SRM_ABORTED, "All ls file requests were cancelled");
                }
                if ((pending_req==len)||(pending_req+running_req==len)||running_req==len) {
                        return new TReturnStatus(TStatusCode.SRM_REQUEST_QUEUED, "All ls file requests are pending");
                }
                if (auth_failure==len) {
                        return new TReturnStatus(TStatusCode.SRM_AUTHORIZATION_FAILURE,
                                "Client is not authorized to request information");
                }
                if (got_exception==len) {
                        return new TReturnStatus(TStatusCode.SRM_INTERNAL_ERROR,
                                "SRM has an internal transient error, and client may try again");
                }
                if (running_req > 0 || pending_req > 0) {
                        return new TReturnStatus(TStatusCode.SRM_REQUEST_INPROGRESS,
                                "Some files are completed, and some files are still on the queue. Details are on the files status");
                }
                else {
                        if (done_req>0) {
                            if (!isFinalState) {
                                 try {
                                         setState(State.DONE,State.DONE.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                                 }
                            }
                            return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS,
                                    "Some SURL requests successfully completed, and some SURL requests failed. Details are on the files status");
                        }
                        else {
                            if (!isFinalState) {
                                 try {
                                         setState(State.FAILED,State.FAILED.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         LOGGER.error("Illegal State Transition : {}", ist.getMessage());
                                 }
                            }
                            return new TReturnStatus(TStatusCode.SRM_FAILURE, "All ls requests failed in some way or another");
                        }
                }
            } finally {
                wunlock();
            }
        }

        @Override
        public void toString(StringBuilder sb, boolean longformat) {
            sb.append(getNameForRequestType()).append(" id:").append(getClientRequestId());
            sb.append(" files:").append(getFileRequests().size());
            sb.append(" state:").append(getState());
            TStatusCode code = getStatusCode();
            if (code != null) {
                sb.append(" status:").append(code);
            }
            sb.append(" by:").append(getUser().getDisplayName());
            if (longformat) {
                sb.append('\n');
                long now = System.currentTimeMillis();
                sb.append("   Submitted: ").append(relativeTimestamp(getCreationTime(), now)).append('\n');
                sb.append("   Expires: ").append(relativeTimestamp(getCreationTime() + getLifetime(), now)).append('\n');
                sb.append("   Count      : ").append(getCount()).append('\n');
                sb.append("   Offset     : ").append(getOffset()).append('\n');
                sb.append("   LongFormat : ").append(getLongFormat()).append('\n');
                sb.append("   NumOfLevels: ").append(getNumOfLevels()).append('\n');
                sb.append("   History:\n");
                sb.append(getHistory("   "));
                for (LsFileRequest fr:getFileRequests()) {
                    sb.append("\n");
                    fr.toString(sb, "   ", true);
                }
            }
        }

    /**
     * @return the numberOfResults
     */
    private int getNumberOfResults() {
        rlock();
        try {
            return numberOfResults;
        } finally {
            runlock();
        }
    }


    /**
     * @param numberOfResults the numberOfResults to set
     */
    private void setNumberOfResults(int numberOfResults) {
        wlock();
        try {
            this.numberOfResults = numberOfResults;
        } finally {
            wunlock();
        }
    }

    /**
     * @return the counter
     */
    public long getCounter() {
        rlock();
        try {
            return counter;
        }
        finally {
            runlock();
        }
    }
    /**
     * @return set the counter
     */
    public void setCounter(long c) {
        wlock();
        try {
            counter=c;
        }
        finally {
            wunlock();
        }
    }

    public void incrementGlobalEntryCounter() {
        wlock();
        try {
            setCounter(getCounter()+1);
        }
        finally {
            wunlock();
        }
    }
    /**
     * @return check if we skip this record if the counter is less than offset
     */
    public boolean shouldSkipThisRecord() {
        rlock();
        try {
            return getCounter()<getOffset();
        }
        finally {
            runlock();
        }
    }
    /**
     * @return the longFormat
     */
    private boolean isLongFormat() {
        // final, no need to synchronize
        return longFormat;
    }

    @Override
    public String getNameForRequestType() {
        return "Ls";
    }
}
