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
import org.dcache.srm.request.RequestUser;
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
/**
 *
 * @author  timur
 */
public class SrmStatusOfGetRequest {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmStatusOfGetRequestRequest statusOfGetRequestRequest;
    SrmStatusOfGetRequestResponse response;
    Scheduler getScheduler;
    RequestUser user;
    RequestCredential credential;
    GetRequestStorage getStorage;
    GetFileRequestStorage getFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    /** Creates a new instance of SrmLs */
    public SrmStatusOfGetRequest(
            RequestUser user,
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
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmStatusOfGetRequest "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmStatusOfGetRequest "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmStatusOfGetRequest exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmStatusOfGetRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmGetStatus();
        } catch(Exception e) {
            storage.elog(e);
            response = new SrmStatusOfGetRequestResponse();
            TReturnStatus returnStatus = new TReturnStatus();
            returnStatus.setStatusCode(TStatusCode.SRM_FAILURE);
            returnStatus.setExplanation(e.toString());
            response.setReturnStatus(returnStatus);
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
        SrmStatusOfGetRequestResponse srmPrepareToGetResponce = new SrmStatusOfGetRequestResponse();
        srmPrepareToGetResponce.setReturnStatus(status);
        return srmPrepareToGetResponce;
    }
    /**
     * implementation of srm get status
     */
    public SrmStatusOfGetRequestResponse srmGetStatus()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException,
            java.sql.SQLException {
        
        
        say("Entering srmGetStatus.");
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
