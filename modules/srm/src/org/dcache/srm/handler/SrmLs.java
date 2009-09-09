/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.FileMetaData;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.util.Permissions;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInternalErrorException;
import org.dcache.srm.SRMTooManyResultsException;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.LsFileRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.sql.LsFileRequestStorage;
import org.dcache.srm.request.sql.LsRequestStorage;
import org.apache.log4j.Logger;

/**
 *
 * @author  timur
 */
public class SrmLs {
        private static Logger logger = 
            Logger.getLogger(SrmLs.class);
        private final static String SFN_STRING="?SFN=";
        private int maxNumOfLevels=100;
        AbstractStorageElement storage;
        Configuration configuration;
        SrmLsRequest request;
        SrmLsResponse response;
        LsRequestStorage requestStorage;
        LsFileRequestStorage fileRequestStorage;
        RequestCredential credential;
        SRMUser user;
        String client_host;
        private int results_num=0;
        private int max_results_num=1000;
        int numOfLevels =1;

        public SrmLs(SRMUser user,
                     RequestCredential credential,
                     SrmLsRequest request,
                     AbstractStorageElement storage,
                     org.dcache.srm.SRM srm,
                     String client_host) {
                this.request = request;
                this.user    = user;
                this.storage = storage;
                this.max_results_num = srm.getConfiguration().getMaxNumberOfLsEntries();
                this.maxNumOfLevels  = srm.getConfiguration().getMaxNumberOfLsLevels();
                this.credential=credential;
                this.client_host=client_host;
                this.requestStorage = srm.getLsRequestStorage();
                if(this.requestStorage == null) {
                        throw new NullPointerException("ls request storage is null");
                }
                this.fileRequestStorage = srm.getLsFileRequestStorage();
                if (this.fileRequestStorage==null) {
                        throw new NullPointerException("ls file request storage is null");
                }
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
                TReturnStatus status = new TReturnStatus();
                status.setStatusCode(statusCode);
                status.setExplanation(error);
                SrmLsResponse response = new SrmLsResponse();
                response.setReturnStatus(status);
                return response;
        }

        boolean longFormat =false;
        String servicePathAndSFNPart = "";
        int port;
        String host;
        public SrmLsResponse getResponse() {
                if(response != null ) return response;
                try {
                        response = srmLs();
                }
                catch(Exception e) {
                        logger.error(e);
                        response = new SrmLsResponse();
                        TReturnStatus returnStatus = new TReturnStatus();
                        returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
                        returnStatus.setExplanation(e.toString());
                        response.setReturnStatus(returnStatus);
                }
                return response;
        }
        /**
         * implementation of srm ls
         */
        public SrmLsResponse srmLs()
                throws SRMException,org.apache.axis.types.URI.MalformedURIException{
                // The SRM specification is not clear, but
                // probably intends that zero (0) means "no
                // recursion", one (1) means "current
                // directory plus one (1) level down, et
                // cetera.
                // Internally, we'll set this value to -1
                // to indicate "no limit".
                if (request.getAllLevelRecursive() != null &&
                    request.getAllLevelRecursive().booleanValue()) {
                        numOfLevels= maxNumOfLevels;
                }
                else {
                        if(request.getNumOfLevels() !=null) {
                                numOfLevels = request.getNumOfLevels().intValue();
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
                int offset =  request.getOffset() !=null ? request.getOffset().intValue() : 0;
                int count  =  request.getCount() !=null ? request.getCount().intValue() : 0;
                if (numOfLevels>1 && (offset > 0 || count > 0)) {
                        return getFailedResponse("numOfLevels>1 together with offset and/or count is not supported",
                                                 TStatusCode.SRM_INVALID_REQUEST);
                }
                if (offset<0) {
                        return getFailedResponse(" offset value less than 0, disallowed ",
				     TStatusCode.SRM_INVALID_REQUEST);
                }
                if (count<0) {
                        return getFailedResponse(" count value less than 0, disallowed",
                                                 TStatusCode.SRM_INVALID_REQUEST);
                }
                if(request.getFullDetailedList() != null) {
                        longFormat = request.getFullDetailedList().booleanValue();
                }
                if( request.getArrayOfSURLs() == null) {
                        return getFailedResponse(" null Path array",
                                                 TStatusCode.SRM_INVALID_REQUEST);
                }
                org.apache.axis.types.URI [] surlInfos = request.getArrayOfSURLs().getUrlArray();
                if (request.getOffset()!=null) {
                        if (request.getOffset().intValue()<0) {
                                return getFailedResponse(" offset value less than 0, diallowed ",
                                                         TStatusCode.SRM_INVALID_REQUEST);
                        }
                }
                TMetaDataPathDetail[] metaDataPathDetails =
                        new TMetaDataPathDetail[surlInfos.length];
                try {
                        LsRequest r = new LsRequest(user,
                                                    credential.getId(),
                                                    requestStorage,
                                                    request,
                                                    configuration,
                                                    3600*1000,
                                                    fileRequestStorage,
                                                    configuration.getLsRetryTimeout(),
                                                    configuration.getLsMaxNumOfRetries(),
                                                    client_host);
                        r.setCount(count);
                        r.setOffset(offset);
                        r.setNumOfLevels(numOfLevels);
                        r.setLongFormat(longFormat);
                        r.setMaxNumOfResults(max_results_num);
                        if (configuration.isAsynchronousLs()) { 
                                r.schedule();
                                return r.getSrmLsResponse();
                        }
                        else { 
                                for (FileRequest fr : r.getFileRequests()) { 
                                        fr.run();
                                        if (fr.getState()==State.PENDING) { 
                                                fr.setState(State.DONE,State.DONE.toString());
                                        }
                                }
                                SrmLsResponse response = new SrmLsResponse();
                                response.setReturnStatus(r.getTReturnStatus());
                                ArrayOfTMetaDataPathDetail details = new ArrayOfTMetaDataPathDetail();
                                details.setPathDetailArray(r.getPathDetailArray());
                                response.setDetails(details);
                                return response;
                        }
                }
                catch (Exception e) {
                        e.printStackTrace();
                        return getFailedResponse(e.toString());
                }
        }
}
