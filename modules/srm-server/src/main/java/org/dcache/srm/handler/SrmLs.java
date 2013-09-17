/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.apache.axis.types.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmLsRequest;
import org.dcache.srm.v2_2.SrmLsResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmLs {
        private static Logger logger =
            LoggerFactory.getLogger(SrmLs.class);
        private final static String SFN_STRING="?SFN=";
        private int maxNumOfLevels=100;
        AbstractStorageElement storage;
        Configuration configuration;
        SrmLsRequest request;
        SrmLsResponse response;
        RequestCredential credential;
        SRMUser user;
        String client_host;
        private int results_num;
        private int max_results_num=1000;
        int numOfLevels =1;

        public SrmLs(SRMUser user,
                     RequestCredential credential,
                     SrmLsRequest request,
                     AbstractStorageElement storage,
                     SRM srm,
                     String client_host) {
                this.request = request;
                this.user    = user;
                this.storage = storage;
                this.max_results_num = srm.getConfiguration().getMaxNumberOfLsEntries();
                this.maxNumOfLevels  = srm.getConfiguration().getMaxNumberOfLsLevels();
                this.credential=credential;
                this.client_host=client_host;
                this.configuration = srm.getConfiguration();
        }

        public static final SrmLsResponse getFailedResponse(String error) {
                return getFailedResponse(error,null);
        }

        public static final  SrmLsResponse getFailedResponse(String error,
                                                             TStatusCode statusCode) {
                if(statusCode == null) {
                        statusCode =TStatusCode.SRM_FAILURE;
                }
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(new TReturnStatus(statusCode, error));
                return response;
        }

        boolean longFormat;
        String servicePathAndSFNPart = "";
        int port;
        String host;
        public SrmLsResponse getResponse() {
                if(response != null ) {
                    return response;
                }
                try {
                        response = srmLs();
                }
                catch(Exception e) {
                        logger.error(e.toString());
                        response = new SrmLsResponse();
                        response.setReturnStatus(new TReturnStatus(TStatusCode.SRM_FAILURE, e.toString()));
                }
                return response;
        }
        /**
         * implementation of srm ls
         */
        public SrmLsResponse srmLs()
                throws SRMException,URI.MalformedURIException{
                // The SRM specification is not clear, but
                // probably intends that zero (0) means "no
                // recursion", one (1) means "current
                // directory plus one (1) level down, et
                // cetera.
                // Internally, we'll set this value to -1
                // to indicate "no limit".
                if (request.getAllLevelRecursive() != null &&
                        request.getAllLevelRecursive()) {
                        numOfLevels= maxNumOfLevels;
                }
                else {
                        if(request.getNumOfLevels() !=null) {
                                numOfLevels = request.getNumOfLevels();
                                // The spec doesn't say what to do in case of negative
                                // values, so filter 'em out...
                                if (numOfLevels < 0) {
                                        return getFailedResponse("numOfLevels < 0",
                                                                 TStatusCode.SRM_INVALID_REQUEST);
                                }
                        }
                        else {
                                numOfLevels = 1;
                        }
                }
                int offset =  request.getOffset() !=null ? request.getOffset() : 0;
                int count  =  request.getCount() !=null ? request.getCount() : Integer.MAX_VALUE;
                if (offset<0) {
                        return getFailedResponse(" offset value less than 0, disallowed ",
				     TStatusCode.SRM_INVALID_REQUEST);
                }
                if (count<0) {
                        return getFailedResponse(" count value less than 0, disallowed",
                                                 TStatusCode.SRM_INVALID_REQUEST);
                }
                if(request.getFullDetailedList() != null) {
                        longFormat = request.getFullDetailedList();
                }
                if(request.getArrayOfSURLs() == null ||
                   request.getArrayOfSURLs().getUrlArray() == null ||
                   request.getArrayOfSURLs().getUrlArray().length == 0) {
                        return getFailedResponse("empty list of paths",
                                                 TStatusCode.SRM_INVALID_REQUEST);
                }

                if (request.getOffset()!=null) {
                        if (request.getOffset() <0) {
                                return getFailedResponse(" offset value less than 0, diallowed ",
                                                         TStatusCode.SRM_INVALID_REQUEST);
                        }
                }
                try {
                        LsRequest r = new LsRequest(user,
                                                    credential.getId(),
                                                    request,
                                                    3600*1000,
                                                    configuration.getLsRetryTimeout(),
                                                    configuration.getLsMaxNumOfRetries(),
                                                    client_host,
                                                    count,
                                                    offset,
                                                    numOfLevels,
                                                    longFormat,
                                                    max_results_num);
                        r.applyJdc();
                        r.schedule();
                        return r.getSrmLsResponse(configuration.getLsSwitchToAsynchronousModeDelay());
                }
                catch (Exception e) {
                        logger.error(e.toString());
                        return getFailedResponse(e.toString());
                }
        }
}
