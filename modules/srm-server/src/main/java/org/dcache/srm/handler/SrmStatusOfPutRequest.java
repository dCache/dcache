package org.dcache.srm.handler;

import org.apache.axis.types.URI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMException;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.PutRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfPutRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmStatusOfPutRequest
{
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SrmStatusOfPutRequest.class);

    private final SrmStatusOfPutRequestRequest statusOfPutRequestRequest;
    private SrmStatusOfPutRequestResponse response;

    public SrmStatusOfPutRequest(
            SRMUser user,
            SrmStatusOfPutRequestRequest statusOfPutRequestRequest,
            AbstractStorageElement storage,
            SRM srm,
            String clientHost)
    {
        this.statusOfPutRequestRequest = checkNotNull(statusOfPutRequestRequest);
    }

    public SrmStatusOfPutRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmPutStatus();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            } catch (SRMException e) {
                LOGGER.error(e.toString());
                response = getFailedResponse(e.toString());
            }
        }
        return response;
    }

    private SrmStatusOfPutRequestResponse srmPutStatus()
            throws SRMException
    {
        String requestToken = statusOfPutRequestRequest.getRequestToken();
        PutRequest putRequest = Request.getRequest(requestToken, PutRequest.class);
        try (JDC ignored = putRequest.applyJdc()) {
            putRequest.tryToReady();
            if (statusOfPutRequestRequest.getArrayOfTargetSURLs() == null) {
                return putRequest.getSrmStatusOfPutRequestResponse();
            }

            URI[] surls = statusOfPutRequestRequest.getArrayOfTargetSURLs().getUrlArray();
            if (surls.length == 0) {
                return putRequest.getSrmStatusOfPutRequestResponse();
            }
            return putRequest.getSrmStatusOfPutRequestResponse(surls);
        }
    }

    public static final SrmStatusOfPutRequestResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfPutRequestResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmStatusOfPutRequestResponse srmPrepareToPutResponse = new SrmStatusOfPutRequestResponse();
        srmPrepareToPutResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToPutResponse;
    }
}
