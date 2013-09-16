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
import org.dcache.srm.util.Configuration;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfTRequestSummary;
import org.dcache.srm.v2_2.SrmGetRequestSummaryRequest;
import org.dcache.srm.v2_2.SrmGetRequestSummaryResponse;
import org.dcache.srm.v2_2.TRequestSummary;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

/**
 *
 * @author  timur
 */
public class SrmGetRequestSummary {

    private static Logger logger =
            LoggerFactory.getLogger(SrmGetRequestSummary.class);
    private final static String SFN_STRING="?SFN=";
    AbstractStorageElement storage;
    SrmGetRequestSummaryRequest srmGetRequestSummaryRequest;
    SrmGetRequestSummaryResponse response;
    SRMUser user;
    RequestCredential credential;
    Configuration configuration;
    private int results_num;
    private int max_results_num;
    int numOfLevels;
    /** Creates a new instance of SrmLs */
    public SrmGetRequestSummary(
            SRMUser user,
            RequestCredential credential,
            SrmGetRequestSummaryRequest srmGetRequestSummaryRequest,
            AbstractStorageElement storage,
            SRM srm,
            String client_host) {
        this.srmGetRequestSummaryRequest = srmGetRequestSummaryRequest;
        this.user = user;
        this.credential = credential;
        this.storage = storage;
        this.configuration = srm.getConfiguration();
    }

    boolean longFormat;
    String servicePathAndSFNPart = "";
    int port;
    String host;
    public SrmGetRequestSummaryResponse getResponse() {
        if(response != null ) {
            return response;
        }
        try {
            response = srmGetRequestSummary();
        } catch(MalformedURIException mue) {
            logger.debug(" malformed uri : "+mue.getMessage());
            response = getFailedResponse(" malformed uri : "+mue.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SQLException sqle) {
            logger.error(sqle.toString());
            response = getFailedResponse("sql error "+sqle.getMessage(),
                    TStatusCode.SRM_INTERNAL_ERROR);
        } catch(SRMInvalidRequestException e) {
            logger.debug(e.getMessage());
            response = getFailedResponse(e.getMessage(),
                    TStatusCode.SRM_INVALID_REQUEST);
        } catch(SRMException srme) {
            logger.error(srme.toString());
            response = getFailedResponse(srme.toString());
        } catch(IllegalStateTransition ist) {
            logger.error("Illegal State Transition : " +ist.getMessage());
            response = getFailedResponse("Illegal State Transition : " +ist.getMessage());
        }

        return response;
    }

    public static final SrmGetRequestSummaryResponse getFailedResponse(String error) {
        return getFailedResponse(error,null);
    }

    public static final SrmGetRequestSummaryResponse getFailedResponse(String error,TStatusCode statusCode) {
        if(statusCode == null) {
            statusCode =TStatusCode.SRM_FAILURE;
        }
        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(statusCode);
        status.setExplanation(error);
        SrmGetRequestSummaryResponse srmGetRequestSummaryResponse = new SrmGetRequestSummaryResponse();
        srmGetRequestSummaryResponse.setReturnStatus(status);
        return srmGetRequestSummaryResponse;
    }
    /**
     * implementation of srm ls
     */
    public SrmGetRequestSummaryResponse srmGetRequestSummary()
    throws SRMException, MalformedURIException,
            SQLException, IllegalStateTransition {
        String[] requestTokens = srmGetRequestSummaryRequest.getArrayOfRequestTokens().getStringArray();
        if( requestTokens == null ) {
            return getFailedResponse("request contains no request tokens");
        }
        TRequestSummary[] requestSummaries = new TRequestSummary[requestTokens.length];

        for(int i = 0 ; i<requestTokens.length; ++i) {

            String requestToken = requestTokens[i];
            try {
                long requestId = Long.parseLong(requestToken);
                ContainerRequest<?> request = Job.getJob(requestId, ContainerRequest.class);

                try (JDC ignored = request.applyJdc()) {
                    // FIXME we do this to make the srm update the status of the request if it changed
                    requestSummaries[i] = request.getRequestSummary();
                }
            } catch (NumberFormatException nfe){
                requestSummaries[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_INVALID_REQUEST,
                " requestToken \""+requestToken+"\" is not valid"));
            } catch(SRMInvalidRequestException e) {
                requestSummaries[i].setStatus(new TReturnStatus(
                        TStatusCode.SRM_INVALID_REQUEST,
                        e.getMessage()));
            }
        }

        SrmGetRequestSummaryResponse response =
                new SrmGetRequestSummaryResponse();

        TReturnStatus status = new TReturnStatus();
        status.setStatusCode(TStatusCode.SRM_SUCCESS);
        response.setArrayOfRequestSummaries(new ArrayOfTRequestSummary(requestSummaries));
        response.setReturnStatus(status);
        return response;

    }


}
