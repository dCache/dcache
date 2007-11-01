//______________________________________________________________________________
//
// $Id: SrmStatusOfCopyRequest.java,v 1.7 2006-06-30 15:32:14 timur Exp $
// $Author: timur $
//
// created 12/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm.handler;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.request.RequestUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.sql.CopyFileRequestStorage;
import org.dcache.srm.request.sql.CopyRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.Request;

/**
 *
 * @author  litvinse
 */

public class SrmStatusOfCopyRequest {
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement         storage;
    SrmStatusOfCopyRequestRequest  request;
    SrmStatusOfCopyRequestResponse response;
    Scheduler                      scheduler;
    RequestUser                    user;
    RequestCredential              credential;
    CopyRequestStorage     copyRequestStorage;
    CopyFileRequestStorage copyFileRequestStorage;
    Configuration          configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    
    
    public SrmStatusOfCopyRequest(RequestUser user,
            RequestCredential credential,
            SrmStatusOfCopyRequestRequest request,
            AbstractStorageElement storage,
            org.dcache.srm.SRM srm,
            String client_host) {
        this.request = request;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.scheduler = srm.getCopyRequestScheduler();
        this.configuration = srm.getConfiguration();
    }
    
    private void say(String words_of_wisdom) {
        if (storage!=null) {
            storage.log("SrmStatusOfCopyRequest "+words_of_wisdom);
        }
    }
    
    private void esay(String words_of_despare) {
        if(storage!=null) {
            storage.elog("SrmStatusOfCopyRequest "+words_of_despare);
        }
    }
    
    private void esay(Throwable t) {
        if(storage!=null) {
            storage.elog(" SrmStatusOfCopyRequest exception : ");
            storage.elog(t);
        }
    }
    
    boolean longFormat = false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    
    public SrmStatusOfCopyRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmStatusOfCopyRequest();
        } catch(Exception e) {
            storage.elog(e);
            response = getFailedResponse("Exception : "+e.toString());
        }
        return response;
    }
    
    
    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }
    
    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(text);
        response.setReturnStatus(status);
        return response;
    }
    
    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest()
    throws SRMException,
            org.apache.axis.types.URI.MalformedURIException,
            java.sql.SQLException {
        say("Entering srmStatusOfCopyRequest");
        String requestToken = request.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        Long requestId;
        try {
            requestId = new Long(requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        Request r = Request.getRequest(requestId);
        if(r == null) {
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not found",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        if ( !(r instanceof CopyRequest) ){
            return getFailedResponse("request for requestToken \""+
                    requestToken+"\"is not srmPrepareToGet request",
                    TStatusCode.SRM_INVALID_REQUEST);
            
        }
        CopyRequest copyRequest = (CopyRequest) r;
        
        if (request.getArrayOfSourceSURLs() == null ||
                request.getArrayOfTargetSURLs() == null) {
            return copyRequest.getSrmStatusOfCopyRequest();
        }
        
        org.apache.axis.types.URI [] fromsurls = request.getArrayOfSourceSURLs().getUrlArray();
        org.apache.axis.types.URI [] tosurls = request.getArrayOfTargetSURLs().getUrlArray();
        if(fromsurls.length == 0 || tosurls.length != fromsurls.length) {
            return copyRequest.getSrmStatusOfCopyRequest();
        }
        
        String[] fromsurlStrings = new String[fromsurls.length];
        for(int i = 0; i< fromsurls.length; ++i) {
            fromsurlStrings[i] = fromsurls[i].toString();
        }
        String[] tosurlStrings = new String[tosurls.length];
        for(int i = 0; i< tosurls.length; ++i) {
            tosurlStrings[i] = tosurls[i].toString();
        }
        
        return copyRequest.getSrmStatusOfCopyRequest(fromsurlStrings,tosurlStrings);
    }
    
    
}
