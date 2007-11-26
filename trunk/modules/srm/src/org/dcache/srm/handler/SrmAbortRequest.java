/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.scheduler.IllegalStateTransition;

/**
 *
 * @author  timur
 */
public class SrmAbortRequest {
    
    
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmAbortRequestRequest srmAbortRequestRequest;
    SrmAbortRequestResponse response;
    RequestUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    /** Creates a new instance of SrmLs */
    public SrmAbortRequest(
            RequestUser user,
            RequestCredential credential,
            SrmAbortRequestRequest srmAbortRequestRequest,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        this.srmAbortRequestRequest = srmAbortRequestRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.configuration = srm.getConfiguration();
    }
    
    private void say(String words_of_wisdom) {
        if(storage!=null) {
            storage.log("SrmAbortRequest "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmAbortRequest "+words_of_despare);
        }
    }
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmAbortRequest exception : ");
            storage.elog(t);
        }
    }
    boolean longFormat =false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmAbortRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmAbortRequest();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse(e.toString());
        }
        
        return response;
    }
    
    public static final SrmAbortRequestResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }
    
    public static final SrmAbortRequestResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmAbortRequestResponse srmReleaseFilesResponse = new SrmAbortRequestResponse();
        srmReleaseFilesResponse.setReturnStatus(status);
        return srmReleaseFilesResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmAbortRequestResponse srmAbortRequest()
    throws SRMException,org.apache.axis.types.URI.MalformedURIException,
            java.sql.SQLException, IllegalStateTransition {
        
        
        say("Entering srmAbortRequest.");
        String requestToken = srmAbortRequestRequest.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token");
        }
        Long requestId;
        try {
            requestId = new Long( requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        Job job = Job.getJob(requestId);

        if(job == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_INVALID_REQUEST);
            
        }
        if(job instanceof ContainerRequest) {
            // we do this to make the srm update the status of the request if it changed
            ((ContainerRequest)job).getTReturnStatus();
        }
        
        synchronized(job) {
            State state = job.getState();
            if(!State.isFinalState(state)) {
                job.setState(State.CANCELED,"SrmAbortRequest called");
            }
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        SrmAbortRequestResponse srmAbortRequestResponse = new SrmAbortRequestResponse();
        srmAbortRequestResponse.setReturnStatus(status);
        return srmAbortRequestResponse;
        
    }
    
    
}
