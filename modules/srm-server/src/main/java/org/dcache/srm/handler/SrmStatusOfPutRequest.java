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
import org.dcache.srm.request.Job;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmStatusOfPutRequest {

    private static Logger logger =
            LoggerFactory.getLogger(SrmStatusOfPutRequest.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmStatusOfPutRequestRequest statusOfPutRequestRequest;
    SrmStatusOfPutRequestResponse response;
    Scheduler putScheduler;
    SRMUser user;
    RequestCredential credential;
    PutRequestStorage putStorage;
    PutFileRequestStorage putFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmStatusOfPutRequest(
            SRMUser user,
            RequestCredential credential,
            SrmStatusOfPutRequestRequest statusOfPutRequestRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host
            ) {
        this.statusOfPutRequestRequest = statusOfPutRequestRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.putScheduler = srm.getPutRequestScheduler();
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfPutRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmPutStatus();
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

    public static final SrmStatusOfPutRequestResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmStatusOfPutRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmStatusOfPutRequestResponse srmPrepareToPutResponse = new SrmStatusOfPutRequestResponse();
        srmPrepareToPutResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToPutResponse;
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
     * implementation of srm put status
     */
    public SrmStatusOfPutRequestResponse srmPutStatus()
        throws SRMException,URISyntaxException
    {
        String requestToken = statusOfPutRequestRequest.getRequestToken();
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

        PutRequest putRequest = Job.getJob(requestId, PutRequest.class);
        putRequest.applyJdc();

        if( statusOfPutRequestRequest.getArrayOfTargetSURLs() == null ){
            return putRequest.getSrmStatusOfPutRequestResponse();
        }

        URI[] surls = toUris(statusOfPutRequestRequest.getArrayOfTargetSURLs().getUrlArray());
        if(surls.length == 0) {
            return putRequest.getSrmStatusOfPutRequestResponse();
        }
        return putRequest.getSrmStatusOfPutRequestResponse(surls);
    }
}
