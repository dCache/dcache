/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.sql.BringOnlineRequestStorage;
import org.dcache.srm.request.sql.BringOnlineFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.SRM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.axis.types.URI.MalformedURIException;
import java.sql.SQLException;


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
    int numOfLevels =0;
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

    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfBringOnlineRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmStatusOfBringOnlineRequestResponse();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
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
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmStatusOfBringOnlineRequestResponse srmPrepareToGetResponse = new SrmStatusOfBringOnlineRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(status);
        return srmPrepareToGetResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequestResponse()
    throws SRMException,
            MalformedURIException,
            SQLException {
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

        ContainerRequest request =(ContainerRequest) ContainerRequest.getRequest(requestId);
        if(request == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_FAILURE);

        }
        if ( !(request instanceof BringOnlineRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToGet request",
                    TStatusCode.SRM_FAILURE);

        }
        BringOnlineRequest getRequest = (BringOnlineRequest) request;
        if( statusOfBringOnlineRequest.getArrayOfSourceSURLs() == null
            || statusOfBringOnlineRequest.getArrayOfSourceSURLs().getUrlArray() == null){
            return getRequest.getSrmStatusOfBringOnlineRequestResponse();
        }

        org.apache.axis.types.URI [] surls = statusOfBringOnlineRequest.getArrayOfSourceSURLs().getUrlArray();
        if(surls.length == 0) {
            return getRequest.getSrmStatusOfBringOnlineRequestResponse();
        }

        String[] surlStrings = new String[surls.length];
        for(int i = 0; i< surls.length; ++i) {
            surlStrings[i] = surls[i].toString();
        }

        return getRequest.getSrmStatusOfBringOnlineRequestResponse(surlStrings);
    }


}
