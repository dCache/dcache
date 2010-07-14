package org.dcache.srm.request;

import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.ArrayOfTMetaDataPathDetail;
import org.dcache.srm.v2_2.TMetaDataPathDetail;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.util.RequestStatusTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Date;

public final class LsRequest extends ContainerRequest {
    private final static Logger logger =
            LoggerFactory.getLogger(LsRequest.class);

        private final long offset;         // starting entry number
        private final long count;          // max number of entries to be returned as set by client
        private int maxNumOfResults=100;  // max number of entries allowed by server, settable via configuration
        private int numberOfResults=0;    // counts only entries allowed to be returned
        private long counter=0;           // counts all entries
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
                int len = request.getArrayOfSURLs().getUrlArray().length;
                fileRequests = new FileRequest[len];
                for(int i = 0; i<len; ++i) {
                        fileRequests[i] =
                                new LsFileRequest(getId(),
                                                  requestCredentialId,
                                                  request.getArrayOfSURLs().getUrlArray()[i],
                                                  lifetime,
                                                  max_number_of_retries);
                }
                storeInSharedMemory();
        }

        public  LsRequest(
                Long id,
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
                FileRequest[] fileRequests,
                int retryDeltaTime,
                boolean should_updateretryDeltaTime,
                String description,
                String client_host,
                String statusCodeString,
                String explanation,
                boolean longFormat,
                int numOfLevels,
                long count,
                long offset) throws java.sql.SQLException {
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

        public FileRequest getFileRequestBySurl(String surl)
                throws java.sql.SQLException,
                SRMException{
                if(surl == null) {
                        throw new SRMException("surl is null");
                }
                for(int i=0; i<fileRequests.length;++i) {
                        if(((LsFileRequest)fileRequests[i]).getSurlString().equals(surl)) {
                                return fileRequests[i];
                        }
                }
                throw new SRMException("ls file request for surl ="+surl +" is not found");
        }

        @Override
        public void schedule() throws InterruptedException,
                IllegalStateTransition {

                // save this request in request storage unconditionally
                // file requests will get stored as soon as they are
                // scheduled, and the saved state needs to be consistent

                saveJob(true);
                for(int i = 0; i < fileRequests.length ;++ i) {
                        LsFileRequest fileRequest = (LsFileRequest) fileRequests[i];
                        fileRequest.schedule();
                }
        }

        public int getNumOfFileRequest() {
                return (fileRequests==null?0:fileRequests.length);
        }

        public String getMethod() {
                return "Ls";
        }

        public boolean shouldStopHandlerIfReady() {
                return true;
        }

        public String kill() {
                return "request was ready, set all ready file statuses to done";
        }

        public void run() throws org.dcache.srm.scheduler.NonFatalJobFailure, org.dcache.srm.scheduler.FatalJobFailure {
        }

        protected void stateChanged(org.dcache.srm.scheduler.State oldState) {
                State state = getState();
                if(State.isFinalState(state)) {
                        for(FileRequest fr : fileRequests ) {
                                try {
                                        State fr_state = fr.getState();
                                        if(!State.isFinalState(fr_state)) {
                                                fr.setState(state,"changing file state becase requests state changed");
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
                throws SRMException, java.sql.SQLException, InterruptedException
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
                throws SRMException ,java.sql.SQLException {
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(getTReturnStatus());
                response.setRequestToken(getTRequestToken());
                if (!response.getReturnStatus().getStatusCode().isProcessing()) {
                    ArrayOfTMetaDataPathDetail details =
                        new ArrayOfTMetaDataPathDetail();
                    details.setPathDetailArray(getPathDetailArray());
                    response.setDetails(details);
                } else {
                    response.setDetails(null);
                }
                return response;
        }

        public final SrmStatusOfLsRequestResponse getSrmStatusOfLsRequestResponse()
                throws SRMException, java.sql.SQLException {
                SrmStatusOfLsRequestResponse response = new SrmStatusOfLsRequestResponse();
                response.setReturnStatus(getTReturnStatus());
                ArrayOfTMetaDataPathDetail details = new ArrayOfTMetaDataPathDetail();
                details.setPathDetailArray(getPathDetailArray());
                response.setDetails(details);
                return response;
        }

        private String getTRequestToken() {
                return getId().toString();
        }

        public TMetaDataPathDetail[] getPathDetailArray()
                throws SRMException,java.sql.SQLException {
                int len = fileRequests.length;
                TMetaDataPathDetail detail[] = new TMetaDataPathDetail[len];
                for(int i = 0; i<len; ++i) {
                        LsFileRequest fr =(LsFileRequest)fileRequests[i];
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
                TReturnStatus status = new TReturnStatus();
                if(getStatusCode() != null) {
                        return new TReturnStatus(getStatusCode(),getExplanation());
                }
                int len = getNumOfFileRequest();
                if (len == 0) {
                        status.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
                        status.setExplanation("Could not find (deserialize) files in the request," +
                                              " NumOfFileRequest is 0");
                        logger.debug("assigned status.statusCode : "+status.getStatusCode());
                        logger.debug("assigned status.explanation : "+status.getExplanation());
                        return status;
                }
                int failed_req           = 0;
                int canceled_req         = 0;
                int pending_req          = 0;
                int running_req          = 0;
                int done_req             = 0;
                int got_exception        = 0;
                int auth_failure         = 0;
                for(FileRequest fr : fileRequests) {
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
                        status.setStatusCode(TStatusCode.SRM_SUCCESS);
                        status.setExplanation("All ls file requests completed");
                        if (!State.isFinalState(getState())) {
                                try {
                                       setState(State.DONE,State.DONE.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                         }
                        return status;
                }
                if (canceled_req == len ) {
                        status.setStatusCode(TStatusCode.SRM_ABORTED);
                        status.setExplanation("All ls file requests were cancelled");
                        return status;
                }
                if ((pending_req==len)||(pending_req+running_req==len)||running_req==len) {
                        status.setStatusCode(TStatusCode.SRM_REQUEST_QUEUED);
                        status.setExplanation("All ls file requests are pending");
                        return status;
                }
                if (auth_failure==len) {
                        status.setStatusCode(TStatusCode.SRM_AUTHORIZATION_FAILURE);
                        status.setExplanation("Client is not authorized to request information");
                        return status;
                }
                if (got_exception==len) {
                        status.setStatusCode(TStatusCode.SRM_INTERNAL_ERROR);
                        status.setExplanation("SRM has an internal transient error, and client may try again");
                        return status;
                }
                if (running_req > 0 || pending_req > 0) {
                        status.setStatusCode(TStatusCode.SRM_REQUEST_INPROGRESS);
                        status.setExplanation("Some files are completed, and some files are still on the queue. Details are on the files status");
                        return status;
                }
                else {
                        if (done_req>0) {
                                status.setStatusCode(TStatusCode.SRM_PARTIAL_SUCCESS);
                                status.setExplanation("Some SURL requests successfully completed, and some SURL requests failed. Details are on the files status");
                                 try {
                                         setState(State.DONE,State.DONE.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                                return status;
                        }
                        else {
                                status.setStatusCode(TStatusCode.SRM_FAILURE);
                                status.setExplanation("All ls requests failed in some way or another");
                                 try {
                                         setState(State.FAILED,State.FAILED.toString());
                                 }
                                 catch(IllegalStateTransition ist) {
                                         logger.error("Illegal State Transition : " +ist.getMessage());
                                 }
                                return status;
                        }
                }
        }

        public  TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls)
                throws SRMException,java.sql.SQLException {
                return null;
        }

        @Override
        public void toString(StringBuilder sb, boolean longformat) {
                sb.append(getMethod()).append("Request #").append(getId()).append(" created by ").append(getUser());
                sb.append(" with credentials : ").append(getCredential()).append(" state = ").append(getState());
                sb.append("\n SURL(s) : ");
                for(FileRequest fr: fileRequests) {
                        LsFileRequest lsfr = (LsFileRequest) fr;
                        sb.append(lsfr.getSurlString()).append(" ");
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
                        for(FileRequest fr: fileRequests) {
                                fr.toString(sb,longformat);
                        }
                } else {
                    sb.append(" number of surls in request:").append(fileRequests.length);
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
