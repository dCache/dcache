package org.dcache.srm.request;

import org.dcache.srm.v2_2.TRequestType;
import org.dcache.srm.SRMUser;
import org.dcache.srm.SRMException;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.JobStorage;
import org.dcache.srm.scheduler.Scheduler;
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



public class LsRequest extends ContainerRequest {

        int offset=0;
        int count=0;
        int maxNumOfResults=100;
        int numberOfResults=0;
        int numOfLevels=1;
        int maxNumberOfLevels=100;
        boolean longFormat=false;
        String explanation;

        public LsRequest(SRMUser user,
                         Long requestCredentialId,
                         JobStorage jobStorage,
                         SrmLsRequest request,
                         Configuration configuration,
                         long lifetime,
                         JobStorage jobFileRequestStorage,
                         long max_update_period,
                         int max_number_of_retries,
                         String client_host ) throws Exception {
                super(user,
                      requestCredentialId,
                      jobStorage,
                      configuration,
                      max_number_of_retries,
                      max_update_period,
                      lifetime,
                      "Ls request",
                      client_host);
                int len = request.getArrayOfSURLs().getUrlArray().length;
                fileRequests = new FileRequest[len];
                for(int i = 0; i<len; ++i) {
                        fileRequests[i] =
                                new LsFileRequest(this,
                                                  requestCredentialId,
                                                  configuration,
                                                  request.getArrayOfSURLs().getUrlArray()[i],
                                                  lifetime,
                                                  jobFileRequestStorage,
                                                  storage,max_number_of_retries);
                }
                if(configuration.isAsynchronousLs()) {
                    storeInSharedMemory();
        }
        }

        public  LsRequest(
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
                Configuration configuration, 
                String explanation,
                boolean longFormat,
                int numOfLevels,
                int count, 
                int offset) throws java.sql.SQLException {
                super(id,
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
                      fileRequests,
                      retryDeltaTime,
                      should_updateretryDeltaTime,
                      description,
                      client_host,
                      statusCodeString,
                      configuration );
                this.explanation=explanation;
                this.longFormat=longFormat;
                this.numOfLevels=numOfLevels;
                this.count=count;
                this.offset=offset;
                for (FileRequest fr : fileRequests) { 
                        ((LsFileRequest)fr).setLsRequest(this);
                }
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
                                        esay(ist);
                                }
                        }
                }
        }

        public synchronized final SrmLsResponse getSrmLsResponse()
                throws SRMException ,java.sql.SQLException {
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(getTReturnStatus());
                response.setRequestToken(getTRequestToken());
//                 ArrayOfTMetaDataPathDetail details = new ArrayOfTMetaDataPathDetail();
//                 details.setPathDetailArray(getPathDetailArray());
                response.setDetails(null);
                return response;
        }

        public synchronized final SrmStatusOfLsRequestResponse getSrmStatusOfLsRequestResponse()
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

        public synchronized boolean increaseResultsNumAndContinue(){
                if(numberOfResults > maxNumOfResults) {
                        return false;
                }
                numberOfResults++;
                return true;
        }

        public synchronized boolean checkCounter(){
                if (numberOfResults > count && count!=0) {
                        return false;
                }
                return true;
        }

        public TRequestType getRequestType() {
                return TRequestType.LS;
        }

        public void setCount(int count) {
                this.count=count;
        }

        public int getCount(){
                return count;
        }

        public void setOffset(int offset) {
                this.offset=offset;
        }

        public int getOffset(){
                return offset;
        }

        public void setNumOfLevels(int number_of_levels){
                this.numOfLevels=number_of_levels;
        }

        public int getNumOfLevels() {
                return numOfLevels;
        }

        public void setLongFormat(boolean yes){
                this.longFormat = yes;
        }

        public boolean getLongFormat() {
                return longFormat;
        }

        public void setMaxNumOfResults(int max_number_of_results){
                this.maxNumOfResults=max_number_of_results;
        }

        public int getMaxNumOfResults() {
                return maxNumOfResults;
        }

        public void setErrorMessage(String txt) {
                errorMessage.append(txt);
        }

        public String getErrorMessage() {
                return errorMessage.toString();
        }

        public String getExplanation() {
                return explanation;
        }

        public void setExplanation(String txt) {
                this.explanation=txt;
        }
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
                        say("assigned status.statusCode : "+status.getStatusCode());
                        say("assigned status.explanation : "+status.getExplanation());
                        return status;
                }
                int failed_req           = 0;
                int canceled_req         = 0;
                int pending_req          = 0;
                int running_req          = 0;
                int done_req             = 0;
                int got_exception        = 0;
                int auth_failure         = 0;
                boolean failure = false;
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
                                        failure=true;
                                }
                                else if (fileReqSC == TStatusCode.SRM_AUTHORIZATION_FAILURE) {
                                        auth_failure++;
                                        failure=true;
                                }
                                else if (RequestStatusTool.isFailedFileRequestStatus(fileReqRS)) {
                                        failed_req++;
                                        failure=true;
                                }
                                else  {
                                        done_req++;
                                }
                        }
                        catch (Exception e) {
                                esay(e);
                                got_exception++;
                                failure=true;
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
                                         esay("can not set fail state:"+ist);
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
                                 if (!State.isFinalState(getState())) { 
                                         try { 
                                                 setState(State.DONE,State.DONE.toString());
                                         }
                                         catch(IllegalStateTransition ist) {
                                                 esay("can not set fail state:"+ist);
                                         }
                                 }
                                return status;
                        }
                        else {
                                status.setStatusCode(TStatusCode.SRM_FAILURE);
                                status.setExplanation("All ls requests failed in some way or another");
                                 if (!State.isFinalState(getState())) { 
                                         try { 
                                                 setState(State.FAILED,State.FAILED.toString());
                                         }
                                         catch(IllegalStateTransition ist) {
                                                 esay("can not set fail state:"+ist);
                                         }
                                 }
                                return status;
                        }
                }
        }

        public  TSURLReturnStatus[] getArrayOfTSURLReturnStatus(String[] surls) 
                throws SRMException,java.sql.SQLException {
                return null;
        }

        public String toString() { 
                       return toString(false);
        }
        
        public String toString(boolean longformat) { 
                StringBuilder sb = new StringBuilder();
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
                                LsFileRequest lsfr = (LsFileRequest) fr;
                                sb.append("\n").append(lsfr.getSurlString());
                                sb.append("\n    status code  for file ").append(fr.getId()).append(":").append(fr.getStatusCode());
                                sb.append("\n    error message for file ").append(fr.getId()).append(fr.getErrorMessage());
                                sb.append("\n    History of State Transitions for file ").append(fr.getId()).append(": \n");
                                sb.append(fr.getHistory());
                        }
                }
                return sb.toString();
        }

}
