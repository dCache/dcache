//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 12/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm.handler;
import org.dcache.srm.v2_2.*;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.sql.CopyFileRequestStorage;
import org.dcache.srm.request.sql.CopyRequestStorage;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.scheduler.Job;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.request.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.axis.types.URI.MalformedURIException;
import java.sql.SQLException;


/**
 *
 * @author  litvinse
 */

public class SrmStatusOfCopyRequest {
    private static Logger logger = 
            LoggerFactory.getLogger(SrmStatusOfCopyRequest.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement         storage;
    SrmStatusOfCopyRequestRequest  request;
    SrmStatusOfCopyRequestResponse response;
    Scheduler                      scheduler;
    SRMUser                    user;
    RequestCredential              credential;
    CopyRequestStorage     copyRequestStorage;
    CopyFileRequestStorage copyFileRequestStorage;
    Configuration          configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels =0;
    
    
    public SrmStatusOfCopyRequest(SRMUser user,
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
    
    boolean longFormat = false;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    
    public SrmStatusOfCopyRequestResponse getResponse() {
        if(response != null ) return response;
        try {
            response = srmStatusOfCopyRequest();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
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
            MalformedURIException,
            SQLException {
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
        CopyRequest copyRequest = Job.getJob(requestId, CopyRequest.class);

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
