/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.SRM;
import org.apache.log4j.Logger;
import org.apache.axis.types.URI.MalformedURIException;
import java.sql.SQLException;

/**
 *
 * @author  timur
 */
public class SrmStatusOfPutRequest {
    
    private static Logger logger = 
            Logger.getLogger(SrmStatusOfPutRequest.class);
    
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
    int numOfLevels =0;
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
    
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfPutRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmPutStatus();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle);
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMException srme) {
            logger.error(srme);
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
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmStatusOfPutRequestResponse srmPrepareToPutResponse = new SrmStatusOfPutRequestResponse();
        srmPrepareToPutResponse.setReturnStatus(status);
        return srmPrepareToPutResponse;
    }
    /**
     * implementation of srm put status
     */
    public SrmStatusOfPutRequestResponse srmPutStatus()
    throws SRMException,MalformedURIException,
            SQLException {
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
        
        ContainerRequest request =(ContainerRequest) ContainerRequest.getRequest(requestId);
        if(request == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_FAILURE);
            
        }
        if ( !(request instanceof PutRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToPut request",
                    TStatusCode.SRM_FAILURE);
            
        }
        PutRequest putRequest = (PutRequest) request;
        if( statusOfPutRequestRequest.getArrayOfTargetSURLs() == null ){
            return putRequest.getSrmStatusOfPutRequestResponse();
        }
        
        org.apache.axis.types.URI [] surls = statusOfPutRequestRequest.getArrayOfTargetSURLs().getUrlArray();
        
        if(surls.length == 0) {
            return putRequest.getSrmStatusOfPutRequestResponse();
        }
        
        String[] surlStrings = new String[surls.length];
        for(int i = 0; i< surls.length; ++i) {
            surlStrings[i] = surls[i].toString();
        }
        
        return putRequest.getSrmStatusOfPutRequestResponse(surlStrings);
    }
    
    
}
