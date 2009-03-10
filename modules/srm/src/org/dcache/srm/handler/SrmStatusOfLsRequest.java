package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.sql.LsFileRequestStorage;
import org.dcache.srm.request.sql.LsRequestStorage;
import org.dcache.srm.SRM;

public class SrmStatusOfLsRequest {
        private final static String SFN_STRING="?SFN=";
        AbstractStorageElement storage;
        SrmStatusOfLsRequestRequest request;
        SrmStatusOfLsRequestResponse response;
        Scheduler scheduler;
        SRMUser user;
        RequestCredential credential;
        LsRequestStorage requestStorage;
        LsFileRequestStorage fileRequestStorage;
        Configuration configuration;
        private int results_num;
        private int max_results_num;
        int numOfLevels =0;

        public SrmStatusOfLsRequest(SRMUser user,
                                    RequestCredential credential,
                                    SrmStatusOfLsRequestRequest request,
                                    AbstractStorageElement storage,
                                    SRM srm,
                                    String client_host) {
                this.request = request;
                this.user = user;
                this.credential = credential;
                this.storage = storage;
                this.scheduler = srm.getGetRequestScheduler();
                this.configuration = srm.getConfiguration();
        }

        private void say(String txt) {
                if(storage!=null) {
                        storage.log("SrmStatusOfLsRequest "+txt);
                }
        }

        private void esay(String txt) {
                if(storage!=null) {
                        storage.elog("SrmStatusOfLsRequest "+txt);
                }
        }

        private void esay(Throwable t) {
                if(storage!=null) {
                        storage.elog(" SrmStatusOfLsRequest exception : ");
                        storage.elog(t);
                }
        }

        boolean longFormat =false;
        String servicePathAndSFNPart = "";
        int port;
        String host;
        public SrmStatusOfLsRequestResponse getResponse() {
                if(response != null ) return response;
                try {
                        response = srmStatusOfLsRequest();
                }
                catch(Exception e) {
                        storage.elog(e);
                        response = new SrmStatusOfLsRequestResponse();
                        TReturnStatus returnStatus = new TReturnStatus();
                        returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
                        returnStatus.setExplanation(e.toString());
                        response.setReturnStatus(returnStatus);
                }
                return response;
        }

        public static final SrmStatusOfLsRequestResponse getFailedResponse(String error) {
                return getFailedResponse(error,null);
        }

        public static final SrmStatusOfLsRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
                if(statusCode == null) {
                        statusCode =TStatusCode.SRM_FAILURE;
                }
                TReturnStatus status = new TReturnStatus();
                status.setStatusCode(statusCode);
                status.setExplanation(error);
                SrmStatusOfLsRequestResponse srmStatusOfLsRequestResponse = new SrmStatusOfLsRequestResponse();
                srmStatusOfLsRequestResponse.setReturnStatus(status);
                return srmStatusOfLsRequestResponse;
        }

        public SrmStatusOfLsRequestResponse srmStatusOfLsRequest()
                throws SRMException,org.apache.axis.types.URI.MalformedURIException,
                java.sql.SQLException {
                say("Entering srmStatusOfLsRequest.");
                String requestToken = request.getRequestToken();
                if( requestToken == null ) {
                        return getFailedResponse("request contains no request token");
                }
                Long requestId;
                try {
                        requestId = new Long( requestToken);
                }
                catch (NumberFormatException nfe){
                        return getFailedResponse(" requestToken \""+
                                                 requestToken+"\"is not valid",
                                                 TStatusCode.SRM_FAILURE);
                }
                ContainerRequest containerRequest =(ContainerRequest) ContainerRequest.getRequest(requestId);
                if(request == null) {
                        return getFailedResponse("request for requestToken \""+
                                                 requestToken+"\"is not found",
                                                 TStatusCode.SRM_FAILURE);

                }
                if ( !(containerRequest instanceof LsRequest) ){
                        return getFailedResponse("request for requestToken \""+
                                                 requestToken+"\"is not srmLs request",
                                                 TStatusCode.SRM_FAILURE);

                }
                return ((LsRequest)containerRequest).getSrmStatusOfLsRequestResponse();
        }
}
