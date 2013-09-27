package org.dcache.srm.request;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.net.URI;
import java.util.Date;
import java.util.List;

import org.dcache.srm.SRMException;
import org.dcache.srm.SRMFileRequestNotFoundException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.scheduler.FatalJobFailure;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.NonFatalJobFailure;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.RequestStatusTool;
import org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

public final class LsRequest extends ContainerRequest<LsFileRequest> {
    private final static Logger logger =
            LoggerFactory.getLogger(LsRequest.class);

        private final long offset;         // starting entry number
        private final long count;          // max number of entries to be returned as set by client
        private int maxNumOfResults=100;  // max number of entries allowed by server, settable via configuration
        private int numberOfResults;    // counts only entries allowed to be returned
        private long counter;           // counts all entries
        private final int numOfLevels;    // recursion level
        private final boolean longFormat;
        private String explanation;

        public LsRequest(SRMUser user,
                         Long requestCredentialId,
                         SrmLsRequest request,
                         long lifetime,
                         long max_update_period,
                         int max_number_of_retries,
                         String client_host,
                         long count,
                         long offset,
                         int numOfLevels,
                         boolean longFormat,
                         int maxNumOfResults ) throws Exception {
                super(user,
                      requestCredentialId,
                      max_number_of_retries,
                      max_update_period,
                      lifetime,
                      "Ls request",
                      client_host);
                this.count = count;
                this.offset = offset;
                this.numOfLevels = numOfLevels;
                this.longFormat = longFormat;
                this.maxNumOfResults = maxNumOfResults;
                org.apache.axis.types.URI[] urls = request.getArrayOfSURLs().getUrlArray();
                List<LsFileRequest> requests = Lists.newArrayListWithCapacity(urls.length);
                for(org.apache.axis.types.URI url : urls) {
                    LsFileRequest fileRequest = new LsFileRequest(getId(),
                            requestCredentialId, url, lifetime,
                            max_number_of_retries);
                    requests.add(fileRequest);
                }
                setFileRequests(requests);
        }

        public  LsRequest(
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
                LsFileRequest[] fileRequests,
                int retryDeltaTime,
                boolean should_updateretryDeltaTime,
                String description,
                String client_host,
                String statusCodeString,
                String explanation,
                boolean longFormat,
                int numOfLevels,
                long count,
                long offset) {
                super(id,
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
                this.explanation=explanation;
                this.longFormat=longFormat;
                this.numOfLevels=numOfLevels;
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
        public void schedule() throws InterruptedException,
                IllegalStateTransition {

                // save this request in request storage unconditionally
                // file requests will get stored as soon as they are
                // scheduled, and the saved state needs to be consistent

                saveJob(true);
                for (LsFileRequest request : getFileRequests()) {
                        request.schedule();
                }
        }

        @Override
        public String getMethod() {
                return "Ls";
        }

        public boolean shouldStopHandlerIfReady() {
                return true;
        }

        public String kill() {
                return "request was ready, set all ready file statuses to done";
        }

        @Override
        public void run() throws NonFatalJobFailure, FatalJobFailure {
        }

        @Override
        protected void stateChanged(State oldState) {
                State state = getState();
                if(State.isFinalState(state)) {
                        for (LsFileRequest fr : getFileRequests() ) {
                                try {
                                        State fr_state = fr.getState();
                                        if(!State.isFinalState(fr_state)) {
                                                fr.setState(state,"changing file state because request state has changed");
                                        }
                                }
                                catch(IllegalStateTransition ist) {
                                        logger.error("Illegal State Transition : " +ist.getMessage());
                                }
                        }
                }
        }

        /**
         * Waits for up to timeout milliseconds for the request to
         * reach a non-queued state and then returns the current
         * SrmLsResponse for this LsRequest.
         */
        public final SrmLsResponse
                getSrmLsResponse(long timeout)
                throws SRMException, InterruptedException
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
                while (response.getReturnStatus().getStatusCode().isProcessing() &&
                       _stateChangeCounter.awaitChangeUntil(counter, deadline)) {
                        counter = _stateChangeCounter.get();
                        response = getSrmLsResponse();
                }
                return response;
        }

        public final SrmLsResponse getSrmLsResponse()
                throws SRMException {
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(getTReturnStatus());
                if (!response.getReturnStatus().getStatusCode().isProcessing()) {
                    ArrayOfTMetaDataPathDetail details =
                        new ArrayOfTMetaDataPathDetail();
                    details.setPathDetailArray(getPathDetailArray());
                    response.setDetails(details);
                } else {
                    response.setDetails(null);
                    response.setRequestToken(getTRequestToken());
                }
                return response;
        }

        public final SrmStatusOfLsRequestResponse getSrmStatusOfLsRequestResponse()
                throws SRMException {
                SrmStatusOfLsRequestResponse response = new SrmStatusOfLsRequestResponse();
                response.setReturnStatus(getTReturnStatus());
                ArrayOfTMetaDataPathDetail details = new ArrayOfTMetaDataPathDetail();
                details.setPathDetailArray(getPathDetailArray());
                response.setDetails(details);
                return response;
        }

        private String getTRequestToken() {
                return String.valueOf(getId());
        }

        public TMetaDataPathDetail[] getPathDetailArray()
                throws SRMException {
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
                if (getNumberOfResults() > getCount() && getCount()!=0) {
                        return false;
                }
                return true;
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

        public void setErrorMessage(String txt) {
            wlock();
            try {
                errorMessage.append(txt);
            } finally {
                wunlock();
            }
        }

        @Override
        public String getErrorMessage() {
            rlock();
            try {
                return errorMessage.toString();
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
        public synchronized final TReturnStatus getTReturnStatus()  {
                getRequestStatus();
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
                                logger.error(e.toString());
                                got_exception++;
                        }
                }
                if (done_req == len ) {
                        if (!State.isFinalState(getState())) {
                                try {
                                       setState(State.DONE,State.DONE.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                         }
                         return new TReturnStatus(TStatusCode.SRM_SUCCESS, "All ls file requests completed");
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
                                 try {
                                         setState(State.DONE,State.DONE.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                                 return new TReturnStatus(TStatusCode.SRM_PARTIAL_SUCCESS,
                                         "Some SURL requests successfully completed, and some SURL requests failed. Details are on the files status");
                        }
                        else {
                                 try {
                                         setState(State.FAILED,State.FAILED.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                                 return new TReturnStatus(TStatusCode.SRM_FAILURE, "All ls requests failed in some way or another");
                        }
                }
        }

        @Override
        public  TSURLReturnStatus[] getArrayOfTSURLReturnStatus(URI[] surls)
        {
                return null;
        }

        @Override
        public void toString(StringBuilder sb, boolean longformat) {
                sb.append(getMethod()).append("Request #").append(getId()).append(" created by ").append(getUser());
                sb.append(" with credentials : ").append(getCredential()).append(" state = ").append(getState());
                sb.append("\n SURL(s) : ");
                for (LsFileRequest fr: getFileRequests()) {
                        sb.append(fr.getSurlString()).append(" ");
                }
                sb.append("\n count      : ").append(getCount());
                sb.append("\n offset     : ").append(getOffset());
                sb.append("\n longFormat : ").append(getLongFormat());
                sb.append("\n numOfLevels: ").append(getNumOfLevels());
                if(longformat) {
                        sb.append("\n status code=").append(getStatusCode());
                        sb.append("\n error message=").append(getErrorMessage());
                        sb.append("\n History of State Transitions: \n");
                        sb.append(getHistory());
                        for (LsFileRequest fr: getFileRequests()) {
                                fr.toString(sb,longformat);
                        }
                } else {
                    sb.append(" number of surls in request:").append(getFileRequests().size());
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

}
