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
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.BringOnlineFileRequestStorage;
import org.dcache.srm.request.sql.BringOnlineRequestStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 */
public class SrmStatusOfBringOnlineRequest {

    private static Logger logger =
            LoggerFactory.getLogger(SrmStatusOfBringOnlineRequest.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmStatusOfBringOnlineRequestRequest statusOfBringOnlineRequest;
    SrmStatusOfBringOnlineRequestResponse response;
    Scheduler srmBringOnlineScheduler;
    SRMUser user;
    RequestCredential credential;
    BringOnlineRequestStorage getStorage;
    BringOnlineFileRequestStorage getFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmStatusOfBringOnlineRequest(
            SRMUser user,
            RequestCredential credential,
            SrmStatusOfBringOnlineRequestRequest statusOfBringOnlineRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host
            ) {
        this.statusOfBringOnlineRequest = statusOfBringOnlineRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.srmBringOnlineScheduler = srm.getBringOnlineRequestScheduler();
        this.configuration = srm.getConfiguration();


    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfBringOnlineRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmStatusOfBringOnlineRequestResponse();
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

    public static final SrmStatusOfBringOnlineRequestResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmStatusOfBringOnlineRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus(statusCode, error);
        SrmStatusOfBringOnlineRequestResponse srmPrepareToGetResponse = new SrmStatusOfBringOnlineRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(status);
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
     * implementation of srm ls
     */
    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequestResponse()
        throws SRMException, URISyntaxException
    {
        String requestToken = statusOfBringOnlineRequest.getRequestToken();
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

        ContainerRequest request = Job.getJob(requestId, ContainerRequest.class);
        request.applyJdc();

        BringOnlineRequest getRequest = (BringOnlineRequest) request;
        if( statusOfBringOnlineRequest.getArrayOfSourceSURLs() == null
            || statusOfBringOnlineRequest.getArrayOfSourceSURLs().getUrlArray() == null){
            return getRequest.getSrmStatusOfBringOnlineRequestResponse();
        }

        URI[] surls = toUris(statusOfBringOnlineRequest.getArrayOfSourceSURLs().getUrlArray());
        if(surls.length == 0) {
            return getRequest.getSrmStatusOfBringOnlineRequestResponse();
        }
        return getRequest.getSrmStatusOfBringOnlineRequestResponse(surls);
    }
}
