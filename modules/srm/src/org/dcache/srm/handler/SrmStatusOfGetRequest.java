/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.sql.GetRequestStorage;
import org.dcache.srm.request.sql.GetFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.SRM;
import org.apache.log4j.Logger;
import org.apache.axis.types.URI.MalformedURIException;
import java.sql.SQLException;


/**
 *
 * @author  timur
 */
public class SrmStatusOfGetRequest {
    private static Logger logger = 
            Logger.getLogger(SrmStatusOfGetRequest.class);
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
    int numOfLevels =0;
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
    
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfGetRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmGetStatus();
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
    
    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmStatusOfGetRequestResponse srmPrepareToGetResponse = new SrmStatusOfGetRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(status);
        return srmPrepareToGetResponse;
    }
    /**
     * implementation of srm get status
     */
    public SrmStatusOfGetRequestResponse srmGetStatus()
    throws SRMException,MalformedURIException,
            SQLException {
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
        
        ContainerRequest request =(ContainerRequest) ContainerRequest.getRequest(requestId);
        if(request == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_FAILURE);
            
        }
        if ( !(request instanceof GetRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToGet request",
                    TStatusCode.SRM_FAILURE);
            
        }
        GetRequest getRequest = (GetRequest) request;
        if( statusOfGetRequestRequest.getArrayOfSourceSURLs() == null ){
            return getRequest.getSrmStatusOfGetRequestResponse();
        }
        
        org.apache.axis.types.URI [] surls = statusOfGetRequestRequest.getArrayOfSourceSURLs().getUrlArray();
        if(surls.length == 0) {
            return getRequest.getSrmStatusOfGetRequestResponse();
        }
        
        String[] surlStrings = new String[surls.length];
        for(int i = 0; i< surls.length; ++i) {
            surlStrings[i] = surls[i].toString();
        }
        
        return getRequest.getSrmStatusOfGetRequestResponse(surlStrings);
    }
    
    
}
