/*
 * SrmLs.java
 *
 * Created on October 4, 2005, 3:40 PM
 */

package org.dcache.srm.handler;

import org.apache.axis.types.URI.MalformedURIException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.ContainerRequest;
import org.dcache.srm.request.Job;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.scheduler.IllegalStateTransition;
import org.dcache.srm.scheduler.State;
import org.dcache.srm.util.Configuration;
import org.dcache.srm.v2_2.SrmAbortRequestRequest;
import org.dcache.srm.v2_2.SrmAbortRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmAbortRequest {

    private static Logger logger =
            LoggerFactory.getLogger(SrmAbortRequest.class);

    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmAbortRequestRequest srmAbortRequestRequest;
    SrmAbortRequestResponse response;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmAbortRequest(
            SRMUser user,
            RequestCredential credential,
            SrmAbortRequestRequest srmAbortRequestRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        this.srmAbortRequestRequest = srmAbortRequestRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmAbortRequestResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmAbortRequest();
        } catch(MalformedURIException mue) {
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
    throws SRMException,MalformedURIException,
            SQLException, IllegalStateTransition {
        String requestToken = srmAbortRequestRequest.getRequestToken();
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

        Job job = Job.getJob(requestId, Job.class);
        job.applyJdc();

        if(job instanceof ContainerRequest) {
            // FIXME we do this to make the srm update the status of the request if it changed
            ((ContainerRequest)job).getTReturnStatus();
        }

        State state = job.getState();
        if(!State.isFinalState(state)) {
            job.setState(State.CANCELED,"SrmAbortRequest called");
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        SrmAbortRequestResponse srmAbortRequestResponse = new SrmAbortRequestResponse();
        srmAbortRequestResponse.setReturnStatus(status);
        return srmAbortRequestResponse;

    }


}
