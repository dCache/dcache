/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URISyntaxException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.GetFileRequestStorage;
import org.dcache.srm.request.sql.GetRequestStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 */
public class SrmStatusOfGetRequest {
    private static Logger logger =
            LoggerFactory.getLogger(SrmStatusOfGetRequest.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmStatusOfGetRequestRequest statusOfGetRequestRequest;
    SrmStatusOfGetRequestResponse response;
    Scheduler getScheduler;
    SRMUser user;
    RequestCredential credential;
    GetRequestStorage getStorage;
    GetFileRequestStorage getFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmStatusOfGetRequest(
            SRMUser user,
            RequestCredential credential,
            SrmStatusOfGetRequestRequest statusOfGetRequestRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host
            ) {
        this.statusOfGetRequestRequest = statusOfGetRequestRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.getScheduler = srm.getGetRequestScheduler();
        this.configuration = srm.getConfiguration();


    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfGetRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmGetStatus();
        } catch(URISyntaxException e) {
            logger.debug(" malformed uri : "+e.getMessage());
            response = getFailedResponse(" malformed uri : "+e.getMessage(),
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

    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmStatusOfGetRequestResponse srmPrepareToGetResponse = new SrmStatusOfGetRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToGetResponse;
    }

    private static URI[] toUris(org.apache.axis.types.URI[] uris)
        throws URISyntaxException
    {
        URI[] result = new URI[uris.length];
        for (int i = 0; i < uris.length; i++) {
            result[i] = new URI(uris[i].toString());
        }
        return result;
    }

    /**
     * implementation of srm get status
     */
    public SrmStatusOfGetRequestResponse srmGetStatus()
        throws SRMException, URISyntaxException
    {
        String requestToken = statusOfGetRequestRequest.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token");
        }
        Long requestId;
        try {
            requestId = new Long( requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_FAILURE);
        }

        GetRequest getRequest = Job.getJob(requestId, GetRequest.class);
        getRequest.applyJdc();

        if( statusOfGetRequestRequest.getArrayOfSourceSURLs() == null ){
            return getRequest.getSrmStatusOfGetRequestResponse();
        }

        URI[] surls = toUris(statusOfGetRequestRequest.getArrayOfSourceSURLs().getUrlArray());
        if(surls.length == 0) {
            return getRequest.getSrmStatusOfGetRequestResponse();
        }
        return getRequest.getSrmStatusOfGetRequestResponse(surls);
    }
}
