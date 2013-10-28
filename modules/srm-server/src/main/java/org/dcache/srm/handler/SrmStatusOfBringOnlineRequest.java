package org.dcache.srm.handler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.BringOnlineRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.ArrayOfAnyURI;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfBringOnlineRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmStatusOfBringOnlineRequest
{
    private final SrmStatusOfBringOnlineRequestRequest request;
    private SrmStatusOfBringOnlineRequestResponse response;

    public SrmStatusOfBringOnlineRequest(
            SRMUser user,
            RequestCredential credential,
            SrmStatusOfBringOnlineRequestRequest request,
            AbstractStorageElement storage,
            SRM srm,
            String clientHost)
    {
        this.request = checkNotNull(request);
    }

    public SrmStatusOfBringOnlineRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmStatusOfBringOnlineRequestResponse();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmStatusOfBringOnlineRequestResponse srmStatusOfBringOnlineRequestResponse()
            throws SRMInvalidRequestException
    {
        BringOnlineRequest bringOnlineRequest = Request.getRequest(request.getRequestToken(), BringOnlineRequest.class);
        try (JDC ignored = bringOnlineRequest.applyJdc()) {
            ArrayOfAnyURI arrayOfSourceSURLs = request.getArrayOfSourceSURLs();
            if (arrayOfSourceSURLs == null
                    || arrayOfSourceSURLs.getUrlArray() == null
                    || arrayOfSourceSURLs.getUrlArray().length == 0) {
                return bringOnlineRequest.getSrmStatusOfBringOnlineRequestResponse();
            }
            return bringOnlineRequest.getSrmStatusOfBringOnlineRequestResponse(arrayOfSourceSURLs.getUrlArray());
        }
    }

    public static final SrmStatusOfBringOnlineRequestResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfBringOnlineRequestResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        TReturnStatus status = new TReturnStatus(statusCode, error);
        SrmStatusOfBringOnlineRequestResponse srmPrepareToGetResponse = new SrmStatusOfBringOnlineRequestResponse();
        srmPrepareToGetResponse.setReturnStatus(status);
        return srmPrepareToGetResponse;
    }
}
