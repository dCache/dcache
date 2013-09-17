package org.dcache.srm.handler;

import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.LsFileRequestStorage;
import org.dcache.srm.request.sql.LsRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;



public class SrmStatusOfLsRequest {
        private static Logger logger =
            LoggerFactory.getLogger(SrmStatusOfLsRequest.class);
        private final static String SFN_STRING="?SFN=";
        AbstractStorageElement storage;
        SrmStatusOfLsRequestRequest request;
        SrmStatusOfLsRequestResponse response;
        SRMUser user;
        RequestCredential credential;
        LsRequestStorage requestStorage;
        LsFileRequestStorage fileRequestStorage;
        Configuration configuration;
        private int results_num;
        private int max_results_num;
        int numOfLevels;

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
                this.configuration = srm.getConfiguration();
        }

        boolean longFormat;
        String servicePathAndSFNPart = "";
        int port;
        String host;
        public SrmStatusOfLsRequestResponse getResponse() {
                if(response != null ) {
                    return response;
                }
                try {
                        response = srmStatusOfLsRequest();
                } catch(MalformedURIException mue) {
                    logger.debug(" malformed uri : "+mue.getMessage());
                    response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                            TStatusCode.SRM_INVALID_REQUEST);
                } catch(SRMInvalidRequestException e) {
                    logger.error(e.toString());
                    response = getFailedResponse(e.getMessage(),
                            TStatusCode.SRM_INVALID_REQUEST);
                } catch(SRMException srme) {
                    logger.error(srme.toString());
                    response = getFailedResponse(srme.toString());
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
                SrmStatusOfLsRequestResponse srmStatusOfLsRequestResponse = new SrmStatusOfLsRequestResponse();
                srmStatusOfLsRequestResponse.setReturnStatus(new TReturnStatus(statusCode, error));
                return srmStatusOfLsRequestResponse;
        }

        public SrmStatusOfLsRequestResponse srmStatusOfLsRequest()
                throws SRMException,MalformedURIException {
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
                LsRequest request = Job.getJob(requestId, LsRequest.class);
                request.applyJdc();

                return request.getSrmStatusOfLsRequestResponse();
        }
}
