/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmPutDoneRequest;
import org.dcache.srm.v2_2.SrmPutDoneResponse;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.sql.PutRequestStorage;
import org.dcache.srm.request.sql.PutFileRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
/**
 *
 * @author  timur
 */
public class SrmPutDone {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmPutDoneRequest srmPutDoneRequest;
    SrmPutDoneResponse response;
    Scheduler putScheduler;
    RequestUser user;
    RequestCredential credential;
    PutRequestStorage putStorage;
    PutFileRequestStorage putFileRequestStorage;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    /** Creates a new instance of SrmLs */
    public SrmPutDone(RequestUser user,
            RequestCredential credential,
            SrmPutDoneRequest srmPutDoneRequest,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        this.srmPutDoneRequest = srmPutDoneRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.putScheduler = srm.getPutRequestScheduler();
        this.configuration = srm.getConfiguration();
    }
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmPutDone "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmReleaseFiles "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmReleaseFiles exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmPutDoneResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmPutDone();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse(e.toString());
        }
        
        return response;
    }
    
    public static final SrmPutDoneResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmPutDoneResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();
        srmPutDoneResponse.setReturnStatus(status);
        return srmPutDoneResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmPutDoneResponse srmPutDone()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException,
            java.sql.SQLException, IllegalStateTransition {
        
        
        say("Entering srmPutDone.");
        String requestToken = srmPutDoneRequest.getRequestToken();
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
        
        Request request = Request.getRequest(requestId);
        if(request == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_FAILURE);
            
        }
        if ( !(request instanceof PutRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToGet request",
                    TStatusCode.SRM_FAILURE);
            
        }
        PutRequest putRequest = (PutRequest) request;
        org.apache.axis.types.URI [] surls;
        if(srmPutDoneRequest.getArrayOfSURLs() ==null) {
            surls = null;
        } else {
            surls = srmPutDoneRequest.getArrayOfSURLs().getUrlArray();
        }
        String[] surl_strings = null;
        if( surls == null ){
            synchronized(putRequest) {
                State state = putRequest.getState();
                if(!State.isFinalState(state)) {
                    putRequest.setState(State.DONE,"SrmPutDone called");
                }
            }
        } else {
            if(surls.length == 0) {
                return getFailedResponse("0 lenght SiteURLs array",
                        TStatusCode.SRM_INVALID_REQUEST);
            }
            surl_strings = new String[surls.length];
            for(int i = 0; i< surls.length; ++i) {
                if(surls[i] != null ) {
                    surl_strings[i] =surls[i].toString();
                    FileRequest fileRequest = putRequest.getFileRequestBySurl(surl_strings[i]);
                    synchronized(fileRequest) {
                        State state = fileRequest.getState();
                        if(!State.isFinalState(state)) {
                            fileRequest.setState(State.DONE,"SrmPutDone called");
                        }
                    }
                } else {
                    return getFailedResponse("SiteURLs["+i+"] is null",
                            TStatusCode.SRM_INVALID_REQUEST);
                }
            }
        }
        
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        SrmPutDoneResponse srmPutDoneResponse = new SrmPutDoneResponse();
        srmPutDoneResponse.setReturnStatus(status);
        if(surls != null) {
            srmPutDoneResponse.setArrayOfFileStatuses(
                    new ArrayOfTSURLReturnStatus(
                    putRequest.getArrayOfTSURLReturnStatus(surl_strings)));
        }
        return srmPutDoneResponse;
        
    }
    
    
}
