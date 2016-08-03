package org.dcache.srm.handler;

import org.apache.axis.types.URI;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.GetRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfGetRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmStatusOfGetRequest
{
    private final SrmStatusOfGetRequestRequest request;
    private SrmStatusOfGetRequestResponse response;

    public SrmStatusOfGetRequest(
            SRMUser user,
            SrmStatusOfGetRequestRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String clientHost)
    {
        this.request = checkNotNull(request);
    }

    public SrmStatusOfGetRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmGetStatus();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(),
                        TStatusCode.SRM_INVALID_REQUEST);
            }
}
        return response;
    }

    private SrmStatusOfGetRequestResponse srmGetStatus()
            throws SRMInvalidRequestException
    {
        String requestToken = request.getRequestToken();
        GetRequest getRequest = Request.getRequest(requestToken, GetRequest.class);
        try (JDC ignored = getRequest.applyJdc()) {
            getRequest.tryToReady();
            if (request.getArrayOfSourceSURLs() == null) {
                return getRequest.getSrmStatusOfGetRequestResponse();
            }

            URI[] surls = request.getArrayOfSourceSURLs().getUrlArray();
            if (surls.length == 0) {
                return getRequest.getSrmStatusOfGetRequestResponse();
            }
            return getRequest.getSrmStatusOfGetRequestResponse(surls);
        }
    }

    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfGetRequestResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmStatusOfGetRequestResponse srmPrepareToGetResponse = new SrmStatusOfGetRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmPrepareToGetResponse;
    }
}
