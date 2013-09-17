//______________________________________________________________________________
//
// $Id$
// $Author$
//
// created 12/05 by Dmitry Litvintsev (litvinse@fnal.gov)
//
//______________________________________________________________________________

package org.dcache.srm.handler;

import org.apache.axis.types.URI;
import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.CopyRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.request.sql.CopyFileRequestStorage;
import org.dcache.srm.request.sql.CopyRequestStorage;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfCopyRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


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
    int numOfLevels;


    public SrmStatusOfCopyRequest(SRMUser user,
            RequestCredential credential,
            SrmStatusOfCopyRequestRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        this.request = request;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.scheduler = srm.getCopyRequestScheduler();
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;

    public SrmStatusOfCopyRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmStatusOfCopyRequest();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
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


    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text) {
        return getFailedResponse(text,null);
    }

    public static final SrmStatusOfCopyRequestResponse getFailedResponse(String text,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        SrmStatusOfCopyRequestResponse response = new SrmStatusOfCopyRequestResponse();
        response.setReturnStatus(new TReturnStatus(statusCode, text));
        return response;
    }

    public SrmStatusOfCopyRequestResponse srmStatusOfCopyRequest()
    throws SRMException,
            MalformedURIException {
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
        copyRequest.applyJdc();

        if (request.getArrayOfSourceSURLs() == null ||
                request.getArrayOfTargetSURLs() == null) {
            return copyRequest.getSrmStatusOfCopyRequest();
        }

        URI [] fromsurls = request.getArrayOfSourceSURLs().getUrlArray();
        URI [] tosurls = request.getArrayOfTargetSURLs().getUrlArray();
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
