package org.dcache.srm.handler;

import org.dcache.srm.AbstractStorageElement;
import org.dcache.srm.SRM;
import org.dcache.srm.SRMInvalidRequestException;
import org.dcache.srm.SRMUser;
import org.dcache.srm.request.LsRequest;
import org.dcache.srm.request.Request;
import org.dcache.srm.request.RequestCredential;
import org.dcache.srm.util.JDC;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestRequest;
import org.dcache.srm.v2_2.SrmStatusOfLsRequestResponse;
import org.dcache.srm.v2_2.TReturnStatus;
import org.dcache.srm.v2_2.TStatusCode;

import static com.google.common.base.Preconditions.checkNotNull;

public class SrmStatusOfLsRequest
{
    private final SrmStatusOfLsRequestRequest request;
    private SrmStatusOfLsRequestResponse response;

    public SrmStatusOfLsRequest(SRMUser user,
                                RequestCredential credential,
                                SrmStatusOfLsRequestRequest request,
                                AbstractStorageElement storage,
                                SRM srm,
                                String clientHost)
    {
        this.request = checkNotNull(request);
    }

    public SrmStatusOfLsRequestResponse getResponse()
    {
        if (response == null) {
            try {
                response = srmStatusOfLsRequest();
            } catch (SRMInvalidRequestException e) {
                response = getFailedResponse(e.getMessage(), TStatusCode.SRM_INVALID_REQUEST);
            }
        }
        return response;
    }

    private SrmStatusOfLsRequestResponse srmStatusOfLsRequest() throws SRMInvalidRequestException
    {
        LsRequest lsRequest = Request.getRequest(request.getRequestToken(), LsRequest.class);
        try (JDC ignored = lsRequest.applyJdc()) {
            return lsRequest.getSrmStatusOfLsRequestResponse();
        }
    }

    public static final SrmStatusOfLsRequestResponse getFailedResponse(String error)
    {
        return getFailedResponse(error, TStatusCode.SRM_FAILURE);
    }

    public static final SrmStatusOfLsRequestResponse getFailedResponse(String error, TStatusCode statusCode)
    {
        SrmStatusOfLsRequestResponse srmStatusOfLsRequestResponse = new SrmStatusOfLsRequestResponse();
        srmStatusOfLsRequestResponse.setReturnStatus(new TReturnStatus(statusCode, error));
        return srmStatusOfLsRequestResponse;
    }
}
