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
import java.sql.SQLException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.FileRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.Scheduler;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.ArrayOfTSURLReturnStatus;
import org.dcache.srm.v2_2.SrmAbortFilesRequest;
import org.dcache.srm.v2_2.SrmAbortFilesResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TSURLReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;


/**
 *
 * @author  timur
 */
public class SrmAbortFiles {

    private static Logger logger =
            LoggerFactory.getLogger(SrmAbortFiles.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmAbortFilesRequest srmAbortFilesRequest;
    SrmAbortFilesResponse response;
    Scheduler getScheduler;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmAbortFiles(
            SRMUser user,
            RequestCredential credential,
            SrmAbortFilesRequest srmAbortFilesRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        this.srmAbortFilesRequest = srmAbortFilesRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.getScheduler = srm.getGetRequestScheduler();
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmAbortFilesResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmAbortFiles();
        } catch(URISyntaxException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMInvalidRequestException ire) {
            logger.debug(" invalid request : "+ire.getMessage());
            response = getFailedResponse(" invalid request : "+ire.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
        } catch(IllegalStateTransition ist) {
            logger.error("Illegal State Transition : " +ist.getMessage());
            response = getFailedResponse("Illegal State Transition : " +ist.getMessage());
        }
        return response;
    }

    public static final SrmAbortFilesResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmAbortFilesResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmAbortFilesResponse srmAbortFilesResponse = new SrmAbortFilesResponse();
        srmAbortFilesResponse.setReturnStatus(status);
        return srmAbortFilesResponse;
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
    public SrmAbortFilesResponse srmAbortFiles()
        throws SRMException,URISyntaxException,
               SQLException, IllegalStateTransition
    {
        String requestToken = srmAbortFilesRequest.getRequestToken();
        if( requestToken == null ) {
            return getFailedResponse("request contains no request token");
        }
        long requestId;
        try {
            requestId = Long.parseLong(requestToken);
        } catch (NumberFormatException nfe){
            return getFailedResponse(" requestToken \""+
                    requestToken+"\"is not valid",
                    TStatusCode.SRM_INVALID_REQUEST);
        }

        ContainerRequest<?> request = Job.getJob(requestId, ContainerRequest.class);
        request.applyJdc();

        URI[] surls;
        if(  srmAbortFilesRequest.getArrayOfSURLs() == null ){
            return getFailedResponse("request does not contain any SURLs",
                    TStatusCode.SRM_INVALID_REQUEST);

        }  else {
            surls = toUris(srmAbortFilesRequest.getArrayOfSURLs().getUrlArray());
        }
        if(surls.length == 0) {
            return getFailedResponse("0 length SiteURLs array",
                    TStatusCode.SRM_INVALID_REQUEST);
        }
        for (URI surl: surls) {
            FileRequest<?> fileRequest = request.getFileRequestBySurl(surl);
            fileRequest.setState(State.CANCELED,"SrmAbortFiles called");
        }

        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        SrmAbortFilesResponse srmAbortFilesResponse = new SrmAbortFilesResponse();
        srmAbortFilesResponse.setReturnStatus(status);
        TSURLReturnStatus[] surlReturnStatusArray =  request.getArrayOfTSURLReturnStatus(surls);
        for (TSURLReturnStatus surlReturnStatus:surlReturnStatusArray) {
            if(surlReturnStatus.getStatus().getStatusCode() == TStatusCode.SRM_ABORTED) {
                surlReturnStatus.getStatus().setStatusCode(TStatusCode.SRM_SUCCESS);
            }
        }

        srmAbortFilesResponse.setArrayOfFileStatuses(
                new ArrayOfTSURLReturnStatus(surlReturnStatusArray));
        // we do this to make the srm update the status of the request if it changed
        request.getTReturnStatus();

        return srmAbortFilesResponse;

    }


}
